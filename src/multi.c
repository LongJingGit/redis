/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"

/* ================================ MULTI/EXEC ============================== */

/* Client state initialization for MULTI/EXEC */
void initClientMultiState(client *c)
{
    c->mstate.commands = NULL;
    c->mstate.count = 0;
    c->mstate.cmd_flags = 0;
    c->mstate.cmd_inv_flags = 0;
}

/* Release all the resources associated with MULTI/EXEC state */
void freeClientMultiState(client *c)
{
    int j;

    for (j = 0; j < c->mstate.count; j++)
    {
        int i;
        multiCmd *mc = c->mstate.commands + j;

        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        zfree(mc->argv);
    }
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue */
/**
 * 具有 MULTI_CLIENT 状态的 client, 在接收到其他命令时，不会执行命令对应的处理函数，
 * 而是将命令加入到 client 的事务命令缓存队列中，然后等待后续的 EXEC 命令提交事务
 */
void queueMultiCommand(client *c)
{
    multiCmd *mc;
    int j;

    /* No sense to waste memory if the transaction is already aborted.
     * this is useful in case client sends these in a pipeline, or doesn't
     * bother to read previous responses and didn't notice the multi was already
     * aborted. */
    if (c->flags & CLIENT_DIRTY_EXEC)
        return;

    c->mstate.commands = zrealloc(c->mstate.commands, sizeof(multiCmd) * (c->mstate.count + 1));
    mc = c->mstate.commands + c->mstate.count;
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = zmalloc(sizeof(robj *) * c->argc);
    memcpy(mc->argv, c->argv, sizeof(robj *) * c->argc);

    for (j = 0; j < c->argc; j++)
        incrRefCount(mc->argv[j]);

    c->mstate.count++;
    c->mstate.cmd_flags |= c->cmd->flags;
    c->mstate.cmd_inv_flags |= ~c->cmd->flags;
}

void discardTransaction(client *c)
{
    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= ~(CLIENT_MULTI | CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC);
    unwatchAllKeys(c);
}

/* Flag the transaction as DIRTY_EXEC so that EXEC will fail.
 * Should be called every time there is an error while queueing a command. */
void flagTransaction(client *c)
{
    if (c->flags & CLIENT_MULTI)
        c->flags |= CLIENT_DIRTY_EXEC;
}

// client 执行 MULTI 命令, 实际上是为 client.flags 设置 CLIENT_MULTI 状态标记, 表示当前客户端开启了事务处理. 后续的命令会全部添加到事务队列中
void multiCommand(client *c)
{
    if (c->flags & CLIENT_MULTI)
    {
        addReplyError(c, "MULTI calls can not be nested");
        return;
    }
    c->flags |= CLIENT_MULTI;
    addReply(c, shared.ok);
}

void discardCommand(client *c)
{
    if (!(c->flags & CLIENT_MULTI))
    {
        addReplyError(c, "DISCARD without MULTI");
        return;
    }
    discardTransaction(c);
    addReply(c, shared.ok);
}

/* Send a MULTI command to all the slaves and AOF file. Check the execCommand
 * implementation for more information. */
void execCommandPropagateMulti(client *c)
{
    propagate(server.multiCommand, c->db->id, &shared.multi, 1,
              PROPAGATE_AOF | PROPAGATE_REPL);
}

void execCommandPropagateExec(client *c)
{
    propagate(server.execCommand, c->db->id, &shared.exec, 1,
              PROPAGATE_AOF | PROPAGATE_REPL);
}

/* Aborts a transaction, with a specific error message.
 * The transaction is always aboarted with -EXECABORT so that the client knows
 * the server exited the multi state, but the actual reason for the abort is
 * included too.
 * Note: 'error' may or may not end with \r\n. see addReplyErrorFormat. */
void execCommandAbort(client *c, sds error)
{
    discardTransaction(c);

    if (error[0] == '-')
        error++;
    addReplyErrorFormat(c, "-EXECABORT Transaction discarded because of: %s", error);

    /* Send EXEC to clients waiting data from MONITOR. We did send a MULTI
     * already, and didn't send any of the queued commands, now we'll just send
     * EXEC so it is clear that the transaction is over. */
    if (listLength(server.monitors) && !server.loading)
        replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);
}

// 执行 EXEC 命令, 提交事务, 一次性的执行事务命令缓存队列中的所有命令
void execCommand(client *c)
{
    int j;
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;
    int must_propagate = 0; /* Need to propagate MULTI/EXEC to AOF / slaves? */
    int was_master = server.masterhost == NULL;

    if (!(c->flags & CLIENT_MULTI))
    {
        addReplyError(c, "EXEC without MULTI");
        return;
    }

    /* EXEC with expired watched key is disallowed*/
    if (isWatchedKeyExpired(c))
    {
        c->flags |= (CLIENT_DIRTY_CAS);
    }

    /* Check if we need to abort the EXEC because:
     * 1) Some WATCHed key was touched.
     * 2) There was a previous error while queueing commands.
     * A failed EXEC in the first case returns a multi bulk nil object
     * (technically it is not an error but a special behavior), while
     * in the second an EXECABORT error is returned. */
    // 判断命令是否有语法错误或者 watch 的 key 被修改的情况，如果存在这两种情况，不执行事务队列中的任何一条命令，直接返回错误
    if (c->flags & (CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC))
    {
        addReply(c, c->flags & CLIENT_DIRTY_EXEC ? shared.execaborterr : shared.nullarray[c->resp]);
        discardTransaction(c);      // 清空事务执行队列，从 watch_keys 中删除正在 watch 的 key
        goto handle_monitor;
    }

    /* Exec all the queued commands */
    unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */
    orig_argv = c->argv;
    orig_argc = c->argc;
    orig_cmd = c->cmd;
    addReplyArrayLen(c, c->mstate.count);
    for (j = 0; j < c->mstate.count; j++) // 遍历命令，依次执行
    {
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->cmd = c->mstate.commands[j].cmd;

        /* Propagate a MULTI request once we encounter the first command which
         * is not readonly nor an administrative one.
         * This way we'll deliver the MULTI/..../EXEC block as a whole and
         * both the AOF and the replication link will have the same consistency
         * and atomicity guarantees. */
        if (!must_propagate &&
            !server.loading &&
            !(c->cmd->flags & (CMD_READONLY | CMD_ADMIN)))
        {
            execCommandPropagateMulti(c);
            must_propagate = 1;
        }

        int acl_keypos;
        int acl_retval = ACLCheckCommandPerm(c, &acl_keypos);
        if (acl_retval != ACL_OK)
        {
            addACLLogEntry(c, acl_retval, acl_keypos, NULL);
            addReplyErrorFormat(c,
                                "-NOPERM ACLs rules changed between the moment the "
                                "transaction was accumulated and the EXEC call. "
                                "This command is no longer allowed for the "
                                "following reason: %s",
                                (acl_retval == ACL_DENIED_CMD) ? "no permission to execute the command or subcommand" : "no permission to touch the specified keys");
        }
        else
        {
            call(c, server.loading ? CMD_CALL_NONE : CMD_CALL_FULL);
        }

        /* Commands may alter argc/argv, restore mstate. */
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    }
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;
    discardTransaction(c);

    /* Make sure the EXEC command will be propagated as well if MULTI
     * was already propagated. */
    if (must_propagate)
    {
        int is_master = server.masterhost == NULL;
        server.dirty++;
        /* If inside the MULTI/EXEC block this instance was suddenly
         * switched from master to slave (using the SLAVEOF command), the
         * initial MULTI was propagated into the replication backlog, but the
         * rest was not. We need to make sure to at least terminate the
         * backlog with the final EXEC. */
        if (server.repl_backlog && was_master && !is_master)
        {
            char *execcmd = "*1\r\n$4\r\nEXEC\r\n";
            feedReplicationBacklog(execcmd, strlen(execcmd));
        }
    }

handle_monitor:
    /* Send EXEC to clients waiting data from MONITOR. We do it here
     * since the natural order of commands execution is actually:
     * MUTLI, EXEC, ... commands inside transaction ...
     * Instead EXEC is flagged as CMD_SKIP_MONITOR in the command
     * table, and we do it here with correct ordering. */
    if (listLength(server.monitors) && !server.loading)
        replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB */
typedef struct watchedKey
{
    robj *key;
    redisDb *db;
} watchedKey;

/* Watch for the specified key */
// 将 key 添加到 client.watched_keys 和 client.db.watched_keys 中
void watchForKey(client *c, robj *key)
{
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    listRewind(c->watched_keys, &li);
    while ((ln = listNext(&li)))
    {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key, wk->key))
            return; /* Key already watched */
    }
    /* This key is not already watched in this DB. Let's add it */
    clients = dictFetchValue(c->db->watched_keys, key);
    if (!clients)
    {
        clients = listCreate();
        dictAdd(c->db->watched_keys, key, clients);
        incrRefCount(key);
    }
    listAddNodeTail(clients, c);
    /* Add the new key to the list of keys watched by this client */
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys, wk);
}

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller. */
void unwatchAllKeys(client *c)
{
    listIter li;
    listNode *ln;

    if (listLength(c->watched_keys) == 0)
        return;
    listRewind(c->watched_keys, &li);
    while ((ln = listNext(&li)))
    {
        list *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client
         * from the list */
        wk = listNodeValue(ln);
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        serverAssertWithInfo(c, NULL, clients != NULL);
        listDelNode(clients, listSearchKey(clients, c));
        /* Kill the entry at all if this was the only client */
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);
        /* Remove this watched key from the client->watched list */
        listDelNode(c->watched_keys, ln);
        decrRefCount(wk->key);
        zfree(wk);
    }
}

/* iterates over the watched_keys list and
 * look for an expired key . */
int isWatchedKeyExpired(client *c)
{
    listIter li;
    listNode *ln;
    watchedKey *wk;
    if (listLength(c->watched_keys) == 0)
        return 0;
    listRewind(c->watched_keys, &li);
    while ((ln = listNext(&li)))
    {
        wk = listNodeValue(ln);
        if (keyIsExpired(wk->db, wk->key))
            return 1;
    }

    return 0;
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail.
 * 当有其他 client 通过命令修改当前 client db 中的某个 key 时，如果该 key 刚好在 redisDb.watched_keys 中,
 * 则表明该 key 被若干个 client 的事务所监控, 对于这种情况, 就需要遍历 redisDb.watched_keys[key] 这个链表中的所有的客户端对象，
 * 为客户端设置 CLIENT_DIRTY_CAS，表明这个客户端所监控的键已经被其他客户端所修改
 */
void touchWatchedKey(redisDb *db, robj *key)
{
    list *clients;
    listIter li;
    listNode *ln;

    // 寻找该 key 是否被 watch, 如果没有被 watch, 则直接返回
    if (dictSize(db->watched_keys) == 0)
        return;

    // 该 key 已经被 watch, 找到对应的 client
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients)
        return;

    /* Mark all the clients watching this key as CLIENT_DIRTY_CAS */
    /* Check if we are already watching for this key */
    listRewind(clients, &li);
    while ((ln = listNext(&li)))
    {
        client *c = listNodeValue(ln);

        c->flags |= CLIENT_DIRTY_CAS;       // 将 client 标志设置为 CLIENT_DIRTY_CAS
    }
}

/* Set CLIENT_DIRTY_CAS to all clients of DB when DB is dirty.
 * It may happen in the following situations:
 * FLUSHDB, FLUSHALL, SWAPDB
 *
 * replaced_with: for SWAPDB, the WATCH should be invalidated if
 * the key exists in either of them, and skipped only if it
 * doesn't exist in both. */
void touchAllWatchedKeysInDb(redisDb *emptied, redisDb *replaced_with)
{
    listIter li;
    listNode *ln;
    dictEntry *de;

    if (dictSize(emptied->watched_keys) == 0)
        return;

    dictIterator *di = dictGetSafeIterator(emptied->watched_keys);
    while ((de = dictNext(di)) != NULL)
    {
        robj *key = dictGetKey(de);
        list *clients = dictGetVal(de);
        if (!clients)
            continue;
        listRewind(clients, &li);
        while ((ln = listNext(&li)))
        {
            client *c = listNodeValue(ln);
            if (dictFind(emptied->dict, key->ptr))
            {
                c->flags |= CLIENT_DIRTY_CAS;
            }
            else if (replaced_with && dictFind(replaced_with->dict, key->ptr))
            {
                c->flags |= CLIENT_DIRTY_CAS;
            }
        }
    }
    dictReleaseIterator(di);
}

// 执行 WATCH 命令: 将命令添加到 client.watched_keys 和 redisDb.watched_keys 中
// 注意: 这个命令必须在调用 multi 命令进入事务之前执行，而不能在事务的执行过程中执行。
void watchCommand(client *c)
{
    int j;

    // watch 命令不能在事务执行过程中执行
    if (c->flags & CLIENT_MULTI)
    {
        addReplyError(c, "WATCH inside MULTI is not allowed");
        return;
    }

    for (j = 1; j < c->argc; j++)
        watchForKey(c, c->argv[j]);

    addReply(c, shared.ok);
}

void unwatchCommand(client *c)
{
    unwatchAllKeys(c);
    c->flags &= (~CLIENT_DIRTY_CAS);
    addReply(c, shared.ok);
}
