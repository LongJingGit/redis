/*
 * Copyright (c) 2013, Salvatore Sanfilippo <antirez at gmail dot com>
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

/**
 * 键空间事件通知是基于订阅发布功能实现的。客户可以监听某一个 key 上发生的事件，或者某种操作发生时的回调逻辑。
 *
 * 键空间事件通知的本质是 client 订阅特定格式的频道，而 server 在事件发生时向特定格式的频道上发布消息
 *
 * Redis 将通知分为两类：
 * 1. 键空间通知: 键空间上的键发生了变化后而产生的通知。对于此类通知，redis 实际上执行了 `PUBLISH keyspace@:key event` 命令
 *
 *  其含义为，频道名为 __keyspace@<db>__:key, 表示这个频道上发布的是 db 编号数据库上的 key 这个键的消息，而 event 则是这条消息代表的 key 上的操作。
 *
 * 2. 键事件通知: 服务器执行了某种操作之后产生的通知。对于此类通知，redis 实际上执行了 `PUBLISH keyevent@:event key` 命令
 *
 *  频道名为 __keyevent@<db>__:event，表示这个频道上发布的是 db 编号数据库上的关于 event 这个事件的消息，这个事件可以是一个命令，也可以是类似过期或者淘汰这样的事件, key 则是这条消息 event 对应操作的键
 */

#include "server.h"

/* This file implements keyspace events notification via Pub/Sub and
 * described at https://redis.io/topics/notifications. */

/* Turn a string representing notification classes into an integer
 * representing notification classes flags xored.
 *
 * The function returns -1 if the input contains characters not mapping to
 * any class. */
int keyspaceEventsStringToFlags(char *classes)
{
    char *p = classes;
    int c, flags = 0;

    while ((c = *p++) != '\0')
    {
        switch (c)
        {
        case 'A':
            flags |= NOTIFY_ALL;
            break;
        case 'g':
            flags |= NOTIFY_GENERIC;
            break;
        case '$':
            flags |= NOTIFY_STRING;
            break;
        case 'l':
            flags |= NOTIFY_LIST;
            break;
        case 's':
            flags |= NOTIFY_SET;
            break;
        case 'h':
            flags |= NOTIFY_HASH;
            break;
        case 'z':
            flags |= NOTIFY_ZSET;
            break;
        case 'x':
            flags |= NOTIFY_EXPIRED;
            break;
        case 'e':
            flags |= NOTIFY_EVICTED;
            break;
        case 'K':
            flags |= NOTIFY_KEYSPACE;
            break;
        case 'E':
            flags |= NOTIFY_KEYEVENT;
            break;
        case 't':
            flags |= NOTIFY_STREAM;
            break;
        case 'm':
            flags |= NOTIFY_KEY_MISS;
            break;
        default:
            return -1;
        }
    }
    return flags;
}

/* This function does exactly the reverse of the function above: it gets
 * as input an integer with the xored flags and returns a string representing
 * the selected classes. The string returned is an sds string that needs to
 * be released with sdsfree(). */
sds keyspaceEventsFlagsToString(int flags)
{
    sds res;

    res = sdsempty();
    if ((flags & NOTIFY_ALL) == NOTIFY_ALL)
    {
        res = sdscatlen(res, "A", 1);
    }
    else
    {
        if (flags & NOTIFY_GENERIC)
            res = sdscatlen(res, "g", 1);
        if (flags & NOTIFY_STRING)
            res = sdscatlen(res, "$", 1);
        if (flags & NOTIFY_LIST)
            res = sdscatlen(res, "l", 1);
        if (flags & NOTIFY_SET)
            res = sdscatlen(res, "s", 1);
        if (flags & NOTIFY_HASH)
            res = sdscatlen(res, "h", 1);
        if (flags & NOTIFY_ZSET)
            res = sdscatlen(res, "z", 1);
        if (flags & NOTIFY_EXPIRED)
            res = sdscatlen(res, "x", 1);
        if (flags & NOTIFY_EVICTED)
            res = sdscatlen(res, "e", 1);
        if (flags & NOTIFY_STREAM)
            res = sdscatlen(res, "t", 1);
    }
    if (flags & NOTIFY_KEYSPACE)
        res = sdscatlen(res, "K", 1);
    if (flags & NOTIFY_KEYEVENT)
        res = sdscatlen(res, "E", 1);
    if (flags & NOTIFY_KEY_MISS)
        res = sdscatlen(res, "m", 1);
    return res;
}

/* The API provided to the rest of the Redis core is a simple function:
 *
 * notifyKeyspaceEvent(char *event, robj *key, int dbid);
 *
 * 'event' is a C string representing the event name.
 * 'key' is a Redis object representing the key name.
 * 'dbid' is the database ID where the key lives.
 *
 * 通过 type 与 redisServer.notify_keyspace_events 来判断这个类型的通知是否开启，如果没有开启则直接返回；否则，根据参数构建频道名，
 * 然后调用 pubsubPublishMessage, 将消息发布到对应频道上。
 */
void notifyKeyspaceEvent(int type, char *event, robj *key, int dbid)
{
    sds chan;
    robj *chanobj, *eventobj;
    int len = -1;
    char buf[24];

    /* If any modules are interested in events, notify the module system now.
     * This bypasses the notifications configuration, but the module engine
     * will only call event subscribers if the event type matches the types
     * they are interested in. */
    moduleNotifyKeyspaceEvent(type, event, key, dbid);

    /* If notifications for this class of events are off, return ASAP. */
    if (!(server.notify_keyspace_events & type))
        return;

    eventobj = createStringObject(event, strlen(event));

    /* __keyspace@<db>__:<key> <event> notifications. */
    if (server.notify_keyspace_events & NOTIFY_KEYSPACE)
    {
        chan = sdsnewlen("__keyspace@", 11);
        len = ll2string(buf, sizeof(buf), dbid);
        chan = sdscatlen(chan, buf, len);
        chan = sdscatlen(chan, "__:", 3);
        chan = sdscatsds(chan, key->ptr);
        chanobj = createObject(OBJ_STRING, chan);
        pubsubPublishMessage(chanobj, eventobj);
        decrRefCount(chanobj);
    }

    /* __keyevent@<db>__:<event> <key> notifications. */
    if (server.notify_keyspace_events & NOTIFY_KEYEVENT)
    {
        chan = sdsnewlen("__keyevent@", 11);
        if (len == -1)
            len = ll2string(buf, sizeof(buf), dbid);
        chan = sdscatlen(chan, buf, len);
        chan = sdscatlen(chan, "__:", 3);
        chan = sdscatsds(chan, eventobj->ptr);
        chanobj = createObject(OBJ_STRING, chan);
        pubsubPublishMessage(chanobj, key);
        decrRefCount(chanobj);
    }
    decrRefCount(eventobj);
}
