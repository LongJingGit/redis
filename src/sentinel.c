/* Redis Sentinel implementation
 *
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
#include "hiredis.h"
#ifdef USE_OPENSSL
#include "openssl/ssl.h"
#include "hiredis_ssl.h"
#endif
#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

extern char **environ;

#ifdef USE_OPENSSL
extern SSL_CTX *redis_tls_ctx;
#endif

#define REDIS_SENTINEL_PORT 26379

/* ======================== Sentinel global state =========================== */

/* Address object, used to describe an ip:port pair. */
typedef struct sentinelAddr
{
    char *ip;
    int port;
} sentinelAddr;

/* A Sentinel Redis Instance object is monitoring. */
#define SRI_MASTER (1 << 0)
#define SRI_SLAVE (1 << 1)
#define SRI_SENTINEL (1 << 2)
#define SRI_S_DOWN (1 << 3)               /* Subjectively down (no quorum). */
#define SRI_O_DOWN (1 << 4)               /* Objectively down (confirmed by others). */
#define SRI_MASTER_DOWN (1 << 5)          /* A Sentinel with this flag set thinks that \
                                              its master is down. */
#define SRI_FAILOVER_IN_PROGRESS (1 << 6) /* Failover is in progress for \
                                             this master. */
#define SRI_PROMOTED (1 << 7)             /* Slave selected for promotion. */
#define SRI_RECONF_SENT (1 << 8)          /* SLAVEOF <newmaster> sent. */
#define SRI_RECONF_INPROG (1 << 9)        /* Slave synchronization in progress. */
#define SRI_RECONF_DONE (1 << 10)         /* Slave synchronized with new master. */
#define SRI_FORCE_FAILOVER (1 << 11)      /* Force failover with master up. */
#define SRI_SCRIPT_KILL_SENT (1 << 12)    /* SCRIPT KILL already sent on -BUSY */

/* Note: times are in milliseconds. */
#define SENTINEL_INFO_PERIOD 10000
#define SENTINEL_PING_PERIOD 1000
#define SENTINEL_ASK_PERIOD 1000
#define SENTINEL_PUBLISH_PERIOD 2000
#define SENTINEL_DEFAULT_DOWN_AFTER 30000
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
#define SENTINEL_TILT_TRIGGER 2000
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD * 30)
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_SLAVE_RECONF_TIMEOUT 10000
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60 * 3 * 1000)
#define SENTINEL_MAX_PENDING_COMMANDS 100
#define SENTINEL_ELECTION_TIMEOUT 10000
#define SENTINEL_MAX_DESYNC 1000
#define SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG 1

/* Failover machine different states. */
#define SENTINEL_FAILOVER_STATE_NONE 0               /* No failover in progress. */
#define SENTINEL_FAILOVER_STATE_WAIT_START 1         /* Wait for failover_start_time*/
#define SENTINEL_FAILOVER_STATE_SELECT_SLAVE 2       /* Select slave to promote */
#define SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3 /* Slave -> Master */
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4     /* Wait slave to change role */
#define SENTINEL_FAILOVER_STATE_RECONF_SLAVES 5      /* SLAVEOF newmaster */
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 6      /* Monitor promoted slave. */

#define SENTINEL_MASTER_LINK_STATUS_UP 0
#define SENTINEL_MASTER_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions.
 * They use higher bits to avoid colliding with the function specific
 * flags. */
#define SENTINEL_NO_FLAGS 0
#define SENTINEL_GENERATE_EVENT (1 << 16)
#define SENTINEL_LEADER (1 << 17)
#define SENTINEL_OBSERVER (1 << 18)

/* Script execution flags and limits. */
#define SENTINEL_SCRIPT_NONE 0
#define SENTINEL_SCRIPT_RUNNING 1
#define SENTINEL_SCRIPT_MAX_QUEUE 256
#define SENTINEL_SCRIPT_MAX_RUNNING 16
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000 /* 60 seconds max exec time. */
#define SENTINEL_SCRIPT_MAX_RETRY 10
#define SENTINEL_SCRIPT_RETRY_DELAY 30000 /* 30 seconds between retries. */

/* SENTINEL SIMULATE-FAILURE command flags. */
#define SENTINEL_SIMFAILURE_NONE 0
#define SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION (1 << 0)
#define SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION (1 << 1)

/* The link to a sentinelRedisInstance. When we have the same set of Sentinels
 * monitoring many masters, we have different instances representing the
 * same Sentinels, one per master, and we need to share the hiredis connections
 * among them. Otherwise if 5 Sentinels are monitoring 100 masters we create
 * 500 outgoing connections instead of 5.
 *
 * So this structure represents a reference counted link in terms of the two
 * hiredis connections for commands and Pub/Sub, and the fields needed for
 * failure detection, since the ping/pong time are now local to the link: if
 * the link is available, the instance is available. This way we don't just
 * have 5 connections instead of 500, we also send 5 pings instead of 500.
 *
 * Links are shared only for Sentinels: master and slave instances have
 * a link with refcount = 1, always. */
typedef struct instanceLink
{
    int refcount;              /* Number of sentinelRedisInstance owners. */
    int disconnected;          /* Non-zero if we need to reconnect cc or pc. */
    int pending_commands;      /* Number of commands sent waiting for a reply. 已发送但是尚未接收到回复的命令数量*/
    redisAsyncContext *cc;     /* Hiredis context for commands. 命令连接: 用于发送命令*/
    redisAsyncContext *pc;     /* Hiredis context for Pub / Sub. 订阅连接: 执行 PUB/SUB 命令. 仅在当前节点为 master 节点时用到，
                                  用于构建 sentinel 集群，根据 master 得到监控该 master 节点的其他 sentinel 节点*/
    mstime_t cc_conn_time;     /* cc connection time. 命令连接创建的时间*/
    mstime_t pc_conn_time;     /* pc connection time. 订阅连接创建的时间*/
    mstime_t pc_last_activity; /* Last time we received any message. 最后一次从该节点接收信息的时间*/

    mstime_t last_avail_time;  /* Last time the instance replied to ping with a reply we consider valid.
                                  上一次接收到正确的 PING 命令响应的时间. 正确的 PING 命令响应包含 PONG/LOADING/MASTERDOWN */

    mstime_t act_ping_time;    /* Time at which the last pending ping (no pong
                                 received after it) was sent. This field is
                                 set to 0 when a pong is received, and set again
                                 to the current time if the value is 0 and a new ping is sent.
                                 act_ping_time 为 0 时，表示已经接收到正确的 PING 命令响应 PONG/LOADING/MASTERDOWN;
                                 否则 act_ping_time 为上次发送 PING 命令的时间 last_ping_time */

    mstime_t last_ping_time;   /* Time at which we sent the last ping. This is
                                 only used to avoid sending too many pings
                                 during failure. Idle time is computed using the act_ping_time field.
                                 最后一次发送 PING 命令的时间*/

    mstime_t last_pong_time;   /* Last time the instance replied to ping, whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected.
                                 最后一次接收到 PING 命令响应的时间, 响应包含 PONG/LOADING/MASTERDOWN/BUSY 中的某一种*/

    mstime_t last_reconn_time; /* Last reconnection attempt performed when the link was down. */
} instanceLink;

// 描述哨兵模式下, redis 的每一个节点(master/slave/sentinel)的所有状态与信息
typedef struct sentinelRedisInstance
{
    int flags;   /* See SRI_... defines. 记录当前节点的类型以及当前节点的状态*/
    char *name;  /* Master name from the point of view of this sentinel. 节点名字: master 的名字由用户在配置文件中指定,
                    slave 以及 sentinel 的名字由 sentinel 自动设置, 格式为 ip:port*/
    char *runid;  /* Run ID of this instance, or unique ID if is a Sentinel.*/
    uint64_t config_epoch;    /* Configuration epoch. 配置纪元，用于实现故障转移*/
    sentinelAddr *addr;       /* Master host. 节点的地址 ip:port*/
    instanceLink *link;       /* Link to the instance, may be shared for Sentinels. 当前被监控节点和 sentinel 节点的异步连接*/
    mstime_t last_pub_time;   /* Last time we sent hello via Pub/Sub. 当前实例为 sentinel 时，最后一次发布 hello 信息的时间*/
    mstime_t last_hello_time; /* Only used if SRI_SENTINEL is set. Last time we received a hello from this Sentinel via Pub/Sub.
                                 当前实例为 sentinel 时，最后一次接收 hello 信息的时间*/
    mstime_t last_master_down_reply_time; /* Time of last reply to SENTINEL is-master-down command.
                                             当前实例为 sentinel 时，最后一次回复 sentinel 的 is-master-down 命令的时间*/

    /* SDOWN 主观下线: 如果某个节点距离最后一次有效回复 PING 命令的时间超过 down-after-milliseconds 设定的值，那么该节点会被 sentinel 标记为 SDOWN
     * ODOWN 客观下线: 当节点被标记为 SDOWN 状态后，随后正在监控该节点的所有 sentinel 都要以每秒一次的频率确认这个节点是否真正进入了 SDOWN 状态，一旦有足够数量的 sentinel 在指定时间范围内同意这个下线判断，那么该节点就会被标记为 ODOWN */
    mstime_t s_down_since_time; /* Subjectively down since time. 节点被判定为 SDOWN 的时间*/
    mstime_t o_down_since_time; /* Objectively down since time. 节点被判定为 ODOWN 的时间*/

    // 每个哨兵会定期的向其监控的节点发送 PING 命令, 判断在线状态。如果超过该时间没有响应，则标记为 SDOWN
    mstime_t down_after_period; /* Consider it down after that period. 设置主观下线的判定时间 */
    mstime_t info_refresh;      /* Time at which we received INFO output from it. 获取 INFO 命令的时间 */
    dict *renamed_commands;     /* Commands renamed in this instance: Sentinel will use the alternative commands
                                   mapped on this table to send things like SLAVEOF, CONFING, INFO, ... */

    /* Role and the first time we observed it.
     * This is useful in order to delay replacing what the instance reports
     * with our own configuration. We need to always wait some time in order
     * to give a chance to the leader to report the new configuration before
     * we do silly things. */
    int role_reported;  // 当前节点的角色: leader 还是 follower
    mstime_t role_reported_time;        // 角色更新的时间
    mstime_t slave_conf_change_time; /* Last time slave master addr changed. slave 节点的 master 地址变更时间*/

    /* Master specific.*/
    /********************** master 特有属性 **********************/
    dict *sentinels;     /* Other sentinels monitoring the same master. 监控该 master 的其他 sentinel*/
    dict *slaves;        /* Slaves for this master instance. 该 master 节点对应的 slave 节点*/
    unsigned int quorum; /* Number of sentinels that need to agree on failure. 判定该 master 节点客观下线所需的投票数量*/
    int parallel_syncs;  /* How many slaves to reconfigure at same time. 在进行故障转移操作时，允许向 master 进行同步的 slave 的数量*/
    char *auth_pass;     /* Password to use for AUTH against master & replica. 连接 master 和 slave 所需要的密码验证信息*/
    char *auth_user;     /* Username for ACLs AUTH against master & replica. */

    /* Slave specific.*/
    /********************** slave 特有属性 **********************/
    mstime_t master_link_down_time;       /* Slave replication link down time. slave 和 master 断开连接的时间*/
    int slave_priority;                   /* Slave priority according to its INFO output. slave 的优先级*/
    mstime_t slave_reconf_sent_time;      /* Time at which we sent SLAVE OF <new>. slave 向 master 发送 replicaof 命令的时间*/
    struct sentinelRedisInstance *master; /* Master instance if it's slave. slave 对应的 master 实例*/
    char *slave_master_host;              /* Master host as reported by INFO. INFO 命令回复中记录的 master ip*/
    int slave_master_port;                /* Master port as reported by INFO. INFO 命令回复中记录的 master port*/
    int slave_master_link_status;         /* Master link status as reported by INFO. INFO 命令回复中记录的主从节点的连接状态*/
    unsigned long long slave_repl_offset; /* Slave replication offset. slave 的复制偏移量*/

    /* Failover */
    /********************** 故障转移相关属性 **********************/
    char *leader;       /* If this is a master instance, this is the runid of the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel that this Sentinel voted as leader. */
    uint64_t leader_epoch;   /* Epoch of the 'leader' field. */
    uint64_t failover_epoch; /* Epoch of the currently started failover. */
    int failover_state;      /* See SENTINEL_FAILOVER_STATE_* defines. */
    mstime_t failover_state_change_time;
    mstime_t failover_start_time;   /* Last failover attempt start time. 最后一次进行故障转移的时间*/
    mstime_t failover_timeout;      /* Max time to refresh failover state. 故障转移的超时时间*/
    mstime_t failover_delay_logged; /* For what failover_start_time value we logged the failover delay. */
    struct sentinelRedisInstance *promoted_slave; /* Promoted slave instance. 指向将被提升为 master 的 slave 节点的指针*/
    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    char *notification_script;  // 当故障发生的时候触发的脚本。比如客观下线，sentinel 会触发设置的警告脚本，向脚本发送响应的事件参数
    char *client_reconfig_script;   // 故障转移成功之后触发的脚本
    sds info; /* cached INFO output */
} sentinelRedisInstance;

/* Main state. sentinel 的主要状态*/
struct sentinelState
{
    char myid[CONFIG_RUN_ID_SIZE + 1]; /* This sentinel ID. */
    uint64_t current_epoch; /* Current epoch. 纪元, 类似 Raft 的 term 任期概念, 用来做故障转移*/
    // 用哈希表来保存 sentinel 节点监控的其他节点(master/slave/sentinel). key 为节点名称, value 为节点实例, 类型为 sentinelRedisInstance
    dict *masters; /* Dictionary of master sentinelRedisInstances. Key is the instance name, value is the
                      entinelRedisInstance structure pointer.*/

    /* TILT 模式实际上是 sentinel 特殊保护性标识. 哨兵模式的运行，实际上非常依赖与系统时间，但是当系统时间被调整，或者哨兵中的流程因为某种
     * 原因(比如负载较高,IO阻塞,进程信号被停止等)而被阻塞时,哨兵的行为就变得不可预知了。于是就有了 TILT 模式，进入 TILT 模式之后，哨兵只定期
     * 发送命令用于收集信息，而不采取实质性的动作(比如不会进行故障转移)，当恢复正常 30S 后，将自动退出 TILT 模式
     */
    int tilt;                  /* Are we in TILT mode? 是否进入 TILT 模式标识*/
    int running_scripts;       /* Number of scripts in execution right now. 正在执行的脚本数量*/
    mstime_t tilt_start_time;  /* When TITL started. 进入 TILT 的时间*/
    mstime_t previous_time;    /* Last time we ran the time handler. 最后一次执行时间事件处理的时间*/
    list *scripts_queue;       /* Queue of user scripts to execute. 需要执行的用户脚本的队列*/
    char *announce_ip;         /* IP addr that is gossiped to other sentinels if not NULL. 哨兵的 IP 地址, 如果不设置, 就是自动发现*/
    int announce_port;         /* Port that is gossiped to other sentinels if non zero. 哨兵的 PORT*/
    unsigned long simfailure_flags;    /* Failures simulation. */
    int deny_scripts_reconfig;         /* Allow SENTINEL SET ... to change script paths at runtime? 是否允许哨兵在运行时修改脚本位置*/
} sentinel;

/* A script execution job. */
typedef struct sentinelScriptJob
{
    int flags;           /* Script job flags: SENTINEL_SCRIPT_* */
    int retry_num;       /* Number of times we tried to execute it. */
    char **argv;         /* Arguments to call the script. */
    mstime_t start_time; /* Script execution time if the script is running,
                            otherwise 0 if we are allowed to retry the
                            execution at any time. If the script is not
                            running and it's not 0, it means: do not run
                            before the specified time. */
    pid_t pid;           /* Script execution pid. */
} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */
typedef struct redisAeEvents
{
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} redisAeEvents;

static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask)
{
    ((void)el);
    ((void)fd);
    ((void)mask);

    redisAeEvents *e = (redisAeEvents *)privdata;
    redisAsyncHandleRead(e->context);
}

static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask)
{
    ((void)el);
    ((void)fd);
    ((void)mask);

    redisAeEvents *e = (redisAeEvents *)privdata;
    redisAsyncHandleWrite(e->context);
}

static void redisAeAddRead(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->reading)
    {
        e->reading = 1;
        aeCreateFileEvent(loop, e->fd, AE_READABLE, redisAeReadEvent, e);
    }
}

static void redisAeDelRead(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *)privdata;
    aeEventLoop *loop = e->loop;
    if (e->reading)
    {
        e->reading = 0;
        aeDeleteFileEvent(loop, e->fd, AE_READABLE);
    }
}

static void redisAeAddWrite(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing)
    {
        e->writing = 1;
        aeCreateFileEvent(loop, e->fd, AE_WRITABLE, redisAeWriteEvent, e);
    }
}

static void redisAeDelWrite(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing)
    {
        e->writing = 0;
        aeDeleteFileEvent(loop, e->fd, AE_WRITABLE);
    }
}

static void redisAeCleanup(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac)
{
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return C_ERR;

    /* Create container for context and r/w events */
    e = (redisAeEvents *)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return C_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);
void sentinelDisconnectCallback(const redisAsyncContext *c, int status);
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);
sentinelRedisInstance *sentinelGetMasterByName(char *name);
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master);
char *sentinelGetObjectiveLeader(sentinelRedisInstance *master);
int yesnotoi(char *s);
void instanceLinkConnectionError(const redisAsyncContext *c);
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);
void sentinelAbortFailover(sentinelRedisInstance *ri);
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master);
void sentinelScheduleScriptExecution(char *path, ...);
void sentinelStartFailover(sentinelRedisInstance *master);
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);
int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port);
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch);
void sentinelFlushConfig(void);
void sentinelGenerateInitialMonitorEvents(void);
int sentinelSendPing(sentinelRedisInstance *ri);
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master);
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid);
void sentinelSimFailureCrash(void);

/* ========================= Dictionary types =============================== */

uint64_t dictSdsHash(const void *key);
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

void dictInstancesValDestructor(void *privdata, void *obj)
{
    UNUSED(privdata);
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
dictType instancesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    dictInstancesValDestructor /* val destructor */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
dictType leaderVotesDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    NULL,              /* key destructor */
    NULL               /* val destructor */
};

/* Instance renamed commands table. */
dictType renamedCommandsDictType = {
    dictSdsCaseHash,       /* hash function */
    NULL,                  /* key dup */
    NULL,                  /* val dup */
    dictSdsKeyCaseCompare, /* key compare */
    dictSdsDestructor,     /* key destructor */
    dictSdsDestructor      /* val destructor */
};

/* =========================== Initialization =============================== */

void sentinelCommand(client *c);
void sentinelInfoCommand(client *c);
void sentinelSetCommand(client *c);
void sentinelPublishCommand(client *c);
void sentinelRoleCommand(client *c);

struct redisCommand sentinelcmds[] = {
    {"ping", pingCommand, 1, "", 0, NULL, 0, 0, 0, 0, 0},
    {"sentinel", sentinelCommand, -2, "", 0, NULL, 0, 0, 0, 0, 0},
    {"subscribe", subscribeCommand, -2, "", 0, NULL, 0, 0, 0, 0, 0},
    {"unsubscribe", unsubscribeCommand, -1, "", 0, NULL, 0, 0, 0, 0, 0},
    {"psubscribe", psubscribeCommand, -2, "", 0, NULL, 0, 0, 0, 0, 0},
    {"punsubscribe", punsubscribeCommand, -1, "", 0, NULL, 0, 0, 0, 0, 0},
    {"publish", sentinelPublishCommand, 3, "", 0, NULL, 0, 0, 0, 0, 0},
    {"info", sentinelInfoCommand, -1, "", 0, NULL, 0, 0, 0, 0, 0},
    {"role", sentinelRoleCommand, 1, "ok-loading", 0, NULL, 0, 0, 0, 0, 0},
    {"client", clientCommand, -2, "read-only no-script", 0, NULL, 0, 0, 0, 0, 0},
    {"shutdown", shutdownCommand, -1, "", 0, NULL, 0, 0, 0, 0, 0},
    {"auth", authCommand, 2, "no-auth no-script ok-loading ok-stale fast", 0, NULL, 0, 0, 0, 0, 0},
    {"hello", helloCommand, -2, "no-auth no-script fast", 0, NULL, 0, 0, 0, 0, 0}
};

/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults. */
void initSentinelConfig(void)
{
    server.port = REDIS_SENTINEL_PORT;
    server.protected_mode = 0; /* Sentinel must be exposed. */
}

/* Perform the Sentinel mode initialization. */
void initSentinel(void)
{
    unsigned int j;

    /* Remove usual Redis commands from the command table, then just add the SENTINEL command. */
    dictEmpty(server.commands, NULL);       // 清空所有的命令, 因为哨兵模式并不需要这些业务命令的处理

    // 将哨兵模式的命令添加到命令哈希表中
    for (j = 0; j < sizeof(sentinelcmds) / sizeof(sentinelcmds[0]); j++)
    {
        int retval;
        struct redisCommand *cmd = sentinelcmds + j;

        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);

        /* Translate the command string flags description into an actual set of flags. */
        if (populateCommandTableParseFlags(cmd, cmd->sflags) == C_ERR)
            serverPanic("Unsupported command flag");
    }

    /* Initialize various data structures. */
    sentinel.current_epoch = 0;
    sentinel.masters = dictCreate(&instancesDictType, NULL);
    sentinel.tilt = 0;
    sentinel.tilt_start_time = 0;
    sentinel.previous_time = mstime();
    sentinel.running_scripts = 0;
    sentinel.scripts_queue = listCreate();
    sentinel.announce_ip = NULL;
    sentinel.announce_port = 0;
    sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
    sentinel.deny_scripts_reconfig = SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG;
    memset(sentinel.myid, 0, sizeof(sentinel.myid));
}

/* This function gets called when the server is in Sentinel mode, started,
 * loaded the configuration, and is ready for normal operations. */
void sentinelIsRunning(void)
{
    int j;

    if (server.configfile == NULL)
    {
        serverLog(LL_WARNING,
                  "Sentinel started without a config file. Exiting...");
        exit(1);
    }
    else if (access(server.configfile, W_OK) == -1)
    {
        serverLog(LL_WARNING,
                  "Sentinel config file %s is not writable: %s. Exiting...",
                  server.configfile, strerror(errno));
        exit(1);
    }

    /* If this Sentinel has yet no ID set in the configuration file, we
     * pick a random one and persist the config on disk. From now on this
     * will be this Sentinel ID across restarts. */
    for (j = 0; j < CONFIG_RUN_ID_SIZE; j++)
        if (sentinel.myid[j] != 0)
            break;

    if (j == CONFIG_RUN_ID_SIZE)
    {
        /* Pick ID and persist the config. */
        getRandomHexChars(sentinel.myid, CONFIG_RUN_ID_SIZE);
        sentinelFlushConfig();
    }

    /* Log its ID to make debugging of issues simpler. */
    serverLog(LL_WARNING, "Sentinel ID is %s", sentinel.myid);

    /* We want to generate a +monitor event for every configured master
     * at startup. */
    sentinelGenerateInitialMonitorEvents();
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 * On error NULL is returned and errno is set to:
 *  ENOENT: Can't resolve the hostname.
 *  EINVAL: Invalid port number.
 */
sentinelAddr *createSentinelAddr(char *hostname, int port)
{
    char ip[NET_IP_STR_LEN];
    sentinelAddr *sa;

    if (port < 0 || port > 65535)
    {
        errno = EINVAL;
        return NULL;
    }
    if (anetResolve(NULL, hostname, ip, sizeof(ip)) == ANET_ERR)
    {
        errno = ENOENT;
        return NULL;
    }
    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(ip);
    sa->port = port;
    return sa;
}

/* Return a duplicate of the source address. */
sentinelAddr *dupSentinelAddr(sentinelAddr *src)
{
    sentinelAddr *sa;

    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(src->ip);
    sa->port = src->port;
    return sa;
}

/* Free a Sentinel address. Can't fail. */
void releaseSentinelAddr(sentinelAddr *sa)
{
    sdsfree(sa->ip);
    zfree(sa);
}

/* Return non-zero if two addresses are equal. */
int sentinelAddrIsEqual(sentinelAddr *a, sentinelAddr *b)
{
    return a->port == b->port && !strcasecmp(a->ip, b->ip);
}

/* =========================== Events notification ========================== */

/* Send an event to log, pub/sub, user notification script.
 *
 * 'level' is the log level for logging. Only LL_WARNING events will trigger
 * the execution of the user notification script.
 *
 * 'type' is the message type, also used as a pub/sub channel name.
 *
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 *
 * The remaining arguments are printf-alike.
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not master, than the additional string is
 *  added to specify the originating master:
 *
 *  @ <master name> <master ip> <master port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...)
{
    va_list ap;
    char msg[LOG_MAX_LEN];
    robj *channel, *payload;

    /* Handle %@ */
    if (fmt[0] == '%' && fmt[1] == '@')
    {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? NULL : ri->master;

        if (master)
        {
            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                     sentinelRedisInstanceTypeStr(ri),
                     ri->name, ri->addr->ip, ri->addr->port,
                     master->name, master->addr->ip, master->addr->port);
        }
        else
        {
            snprintf(msg, sizeof(msg), "%s %s %s %d",
                     sentinelRedisInstanceTypeStr(ri),
                     ri->name, ri->addr->ip, ri->addr->port);
        }
        fmt += 2;
    }
    else
    {
        msg[0] = '\0';
    }

    /* Use vsprintf for the rest of the formatting if any. */
    if (fmt[0] != '\0')
    {
        va_start(ap, fmt);
        vsnprintf(msg + strlen(msg), sizeof(msg) - strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* Log the message if the log level allows it to be logged. */
    if (level >= server.verbosity)
        serverLog(level, "%s %s", type, msg);

    /* Publish the message via Pub/Sub if it's not a debugging one. */
    if (level != LL_DEBUG)
    {
        channel = createStringObject(type, strlen(type));
        payload = createStringObject(msg, strlen(msg));
        pubsubPublishMessage(channel, payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    /* Call the notification script if applicable. */
    if (level == LL_WARNING && ri != NULL)
    {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? ri : ri->master;
        if (master && master->notification_script)
        {
            sentinelScheduleScriptExecution(master->notification_script,
                                            type, msg, NULL);
        }
    }
}

/* This function is called only at startup and is used to generate a
 * +monitor event for every configured master. The same events are also
 * generated when a master to monitor is added at runtime via the
 * SENTINEL MONITOR command. */
void sentinelGenerateInitialMonitorEvents(void)
{
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);
        sentinelEvent(LL_WARNING, "+monitor", ri, "%@ quorum %d", ri->quorum);
    }
    dictReleaseIterator(di);
}

/* ============================ script execution ============================ */

/* Release a script job structure and all the associated data. */
void sentinelReleaseScriptJob(sentinelScriptJob *sj)
{
    int j = 0;

    while (sj->argv[j])
        sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

#define SENTINEL_SCRIPT_MAX_ARGS 16
void sentinelScheduleScriptExecution(char *path, ...)
{
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS + 1];
    int argc = 1;
    sentinelScriptJob *sj;

    va_start(ap, path);
    while (argc < SENTINEL_SCRIPT_MAX_ARGS)
    {
        argv[argc] = va_arg(ap, char *);
        if (!argv[argc])
            break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);

    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char *) * (argc + 1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv, argv, sizeof(char *) * (argc + 1));

    listAddNodeTail(sentinel.scripts_queue, sj);

    /* Remove the oldest non running script if we already hit the limit. */
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE)
    {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue, &li);
        while ((ln = listNext(&li)) != NULL)
        {
            sj = ln->value;

            if (sj->flags & SENTINEL_SCRIPT_RUNNING)
                continue;
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue, ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        serverAssert(listLength(sentinel.scripts_queue) <=
                     SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed). */
listNode *sentinelGetScriptListNodeByPid(pid_t pid)
{
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue, &li);
    while ((ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

/* Run pending scripts if we are not already at max number of running scripts. */
// 运行等待执行的脚本
void sentinelRunPendingScripts(void)
{
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    listRewind(sentinel.scripts_queue, &li);
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
           (ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running. */
        if (sj->flags & SENTINEL_SCRIPT_RUNNING)
            continue;

        /* Skip if it's a retry, but not enough time has elapsed. */
        if (sj->start_time && sj->start_time > now)
            continue;

        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        sj->start_time = mstime();
        sj->retry_num++;
        pid = fork();

        if (pid == -1)
        {
            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            sentinelEvent(LL_WARNING, "-script-error", NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
        }
        else if (pid == 0)
        {
            /* Child */
            execve(sj->argv[0], sj->argv, environ);
            /* If we are here an error occurred. */
            _exit(2); /* Don't retry execution. */
        }
        else
        {
            sentinel.running_scripts++;
            sj->pid = pid;
            sentinelEvent(LL_DEBUG, "+script-child", NULL, "%ld", (long)pid);
        }
    }
}

/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
mstime_t sentinelScriptRetryDelay(int retry_num)
{
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1)
        delay *= 2;
    return delay;
}

/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed.
 *
 * 清理已经执行完毕的脚本,并尝试重新执行出错的脚本
 */
void sentinelCollectTerminatedScripts(void)
{
    int statloc;
    pid_t pid;

    while ((pid = wait3(&statloc, WNOHANG, NULL)) > 0)
    {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        if (WIFSIGNALED(statloc))
            bysignal = WTERMSIG(statloc);
        sentinelEvent(LL_DEBUG, "-script-child", NULL, "%ld %d %d",
                      (long)pid, exitcode, bysignal);

        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL)
        {
            serverLog(LL_WARNING, "wait3() returned a pid (%ld) we can't find in our scripts execution queue!", (long)pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached. */
        if ((bysignal || exitcode == 1) &&
            sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY)
        {
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            sj->start_time = mstime() +
                             sentinelScriptRetryDelay(sj->retry_num);
        }
        else
        {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways. */
            if (bysignal || exitcode != 0)
            {
                sentinelEvent(LL_WARNING, "-script-error", NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }
            listDelNode(sentinel.scripts_queue, ln);
            sentinelReleaseScriptJob(sj);
        }
        sentinel.running_scripts--;
    }
}

/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function.
 * 杀死运行超时的脚本
 */
void sentinelKillTimedoutScripts(void)
{
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    listRewind(sentinel.scripts_queue, &li);
    while ((ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;

        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
            (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME)
        {
            sentinelEvent(LL_WARNING, "-script-timeout", NULL, "%s %ld",
                          sj->argv[0], (long)sj->pid);
            kill(sj->pid, SIGKILL);
        }
    }
}

/* Implements SENTINEL PENDING-SCRIPTS command. */
void sentinelPendingScriptsCommand(client *c)
{
    listNode *ln;
    listIter li;

    addReplyArrayLen(c, listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue, &li);
    while ((ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMapLen(c, 5);

        addReplyBulkCString(c, "argv");
        while (sj->argv[j])
            j++;
        addReplyArrayLen(c, j);
        j = 0;
        while (sj->argv[j])
            addReplyBulkCString(c, sj->argv[j++]);

        addReplyBulkCString(c, "flags");
        addReplyBulkCString(c,
                            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c, "pid");
        addReplyBulkLongLong(c, sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING)
        {
            addReplyBulkCString(c, "run-time");
            addReplyBulkLongLong(c, mstime() - sj->start_time);
        }
        else
        {
            mstime_t delay = sj->start_time ? (sj->start_time - mstime()) : 0;
            if (delay < 0)
                delay = 0;
            addReplyBulkCString(c, "run-delay");
            addReplyBulkLongLong(c, delay);
        }

        addReplyBulkCString(c, "retry-num");
        addReplyBulkLongLong(c, sj->retry_num);
    }
}

/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover is performed.
 *
 * <state> is currently always "failover".
 * <role> is either "leader" or "observer".
 *
 * from/to fields are respectively master -> promoted slave addresses for
 * "start" and "end". */
void sentinelCallClientReconfScript(sentinelRedisInstance *master, int role, char *state, sentinelAddr *from, sentinelAddr *to)
{
    char fromport[32], toport[32];

    if (master->client_reconfig_script == NULL)
        return;
    ll2string(fromport, sizeof(fromport), from->port);
    ll2string(toport, sizeof(toport), to->port);
    sentinelScheduleScriptExecution(master->client_reconfig_script,
                                    master->name,
                                    (role == SENTINEL_LEADER) ? "leader" : "observer",
                                    state, from->ip, fromport, to->ip, toport, NULL);
}

/* =============================== instanceLink ============================= */

/* Create a not yet connected link object. */
instanceLink *createInstanceLink(void)
{
    instanceLink *link = zmalloc(sizeof(*link));

    link->refcount = 1;
    link->disconnected = 1;
    link->pending_commands = 0;
    link->cc = NULL;
    link->pc = NULL;
    link->cc_conn_time = 0;
    link->pc_conn_time = 0;
    link->last_reconn_time = 0;
    link->pc_last_activity = 0;
    /* We set the act_ping_time to "now" even if we actually don't have yet
     * a connection with the node, nor we sent a ping.
     * This is useful to detect a timeout in case we'll not be able to connect
     * with the node at all. */
    link->act_ping_time = mstime();
    link->last_ping_time = 0;
    link->last_avail_time = mstime();
    link->last_pong_time = mstime();
    return link;
}

/* Disconnect a hiredis connection in the context of an instance link. */
void instanceLinkCloseConnection(instanceLink *link, redisAsyncContext *c)
{
    if (c == NULL)
        return;

    if (link->cc == c)
    {
        link->cc = NULL;
        link->pending_commands = 0;
    }
    if (link->pc == c)
        link->pc = NULL;
    c->data = NULL;
    link->disconnected = 1;
    redisAsyncFree(c);
}

/* Decrement the refcount of a link object, if it drops to zero, actually
 * free it and return NULL. Otherwise don't do anything and return the pointer
 * to the object.
 *
 * If we are not going to free the link and ri is not NULL, we rebind all the
 * pending requests in link->cc (hiredis connection for commands) to a
 * callback that will just ignore them. This is useful to avoid processing
 * replies for an instance that no longer exists. */
instanceLink *releaseInstanceLink(instanceLink *link, sentinelRedisInstance *ri)
{
    serverAssert(link->refcount > 0);
    link->refcount--;
    if (link->refcount != 0)
    {
        if (ri && ri->link->cc)
        {
            /* This instance may have pending callbacks in the hiredis async
             * context, having as 'privdata' the instance that we are going to
             * free. Let's rewrite the callback list, directly exploiting
             * hiredis internal data structures, in order to bind them with
             * a callback that will ignore the reply at all. */
            redisCallback *cb;
            redisCallbackList *callbacks = &link->cc->replies;

            cb = callbacks->head;
            while (cb)
            {
                if (cb->privdata == ri)
                {
                    cb->fn = sentinelDiscardReplyCallback;
                    cb->privdata = NULL; /* Not strictly needed. */
                }
                cb = cb->next;
            }
        }
        return link; /* Other active users. */
    }

    instanceLinkCloseConnection(link, link->cc);
    instanceLinkCloseConnection(link, link->pc);
    zfree(link);
    return NULL;
}

/* This function will attempt to share the instance link we already have
 * for the same Sentinel in the context of a different master, with the
 * instance we are passing as argument.
 *
 * This way multiple Sentinel objects that refer all to the same physical
 * Sentinel instance but in the context of different masters will use
 * a single connection, will send a single PING per second for failure
 * detection and so forth.
 *
 * Return C_OK if a matching Sentinel was found in the context of a
 * different master and sharing was performed. Otherwise C_ERR
 * is returned. */
int sentinelTryConnectionSharing(sentinelRedisInstance *ri)
{
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;

    if (ri->runid == NULL)
        return C_ERR; /* No way to identify it. */
    if (ri->link->refcount > 1)
        return C_ERR; /* Already shared. */

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        /* We want to share with the same physical Sentinel referenced
         * in other masters, so skip our master. */
        if (master == ri->master)
            continue;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL, 0, ri->runid);
        if (match == NULL)
            continue; /* No match. */
        if (match == ri)
            continue; /* Should never happen but... safer. */

        /* We identified a matching Sentinel, great! Let's free our link
         * and use the one of the matching Sentinel. */
        releaseInstanceLink(ri->link, NULL);
        ri->link = match->link;
        match->link->refcount++;
        dictReleaseIterator(di);
        return C_OK;
    }
    dictReleaseIterator(di);
    return C_ERR;
}

/* When we detect a Sentinel to switch address (reporting a different IP/port
 * pair in Hello messages), let's update all the matching Sentinels in the
 * context of other masters as well and disconnect the links, so that everybody
 * will be updated.
 *
 * Return the number of updated Sentinel addresses. */
int sentinelUpdateSentinelAddressInAllMasters(sentinelRedisInstance *ri)
{
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;
    int reconfigured = 0;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL, 0, ri->runid);
        /* If there is no match, this master does not know about this
         * Sentinel, try with the next one. */
        if (match == NULL)
            continue;

        /* Disconnect the old links if connected. */
        if (match->link->cc != NULL)
            instanceLinkCloseConnection(match->link, match->link->cc);
        if (match->link->pc != NULL)
            instanceLinkCloseConnection(match->link, match->link->pc);

        if (match == ri)
            continue; /* Address already updated for it. */

        /* Update the address of the matching Sentinel by copying the address
         * of the Sentinel object that received the address update. */
        releaseSentinelAddr(match->addr);
        match->addr = dupSentinelAddr(ri->addr);
        reconfigured++;
    }
    dictReleaseIterator(di);
    if (reconfigured)
        sentinelEvent(LL_NOTICE, "+sentinel-address-update", ri,
                      "%@ %d additional matching instances", reconfigured);
    return reconfigured;
}

/* This function is called when a hiredis connection reported an error.
 * We set it to NULL and mark the link as disconnected so that it will be
 * reconnected again.
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections. */
void instanceLinkConnectionError(const redisAsyncContext *c)
{
    instanceLink *link = c->data;
    int pubsub;

    if (!link)
        return;

    pubsub = (link->pc == c);
    if (pubsub)
        link->pc = NULL;
    else
        link->cc = NULL;
    link->disconnected = 1;
}

/* Hiredis connection established / disconnected callbacks. We need them
 * just to cleanup our link state. */
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status)
{
    if (status != C_OK)
        instanceLinkConnectionError(c);
}

void sentinelDisconnectCallback(const redisAsyncContext *c, int status)
{
    UNUSED(status);
    instanceLinkConnectionError(c);
}

/* ========================== sentinelRedisInstance ========================= */

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 * runid: set to NULL but will be populated once INFO output is received.
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *
 * If SRI_MASTER is set into initial flags the instance is added to
 * sentinel.masters table.
 *
 * if SRI_SLAVE or SRI_SENTINEL is set then 'master' must be not NULL and the
 * instance is added into master->slaves or master->sentinels table.
 *
 * If the instance is a slave or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master with the same name, a slave with the same address, or a sentinel
 * with the same ID already exists.
 *
 * 创建一个 redis 节点: master, slave, sentinel
 */
sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum, sentinelRedisInstance *master)
{
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL;
    char slavename[NET_PEER_ID_LEN], *sdsname;

    serverAssert(flags & (SRI_MASTER | SRI_SLAVE | SRI_SENTINEL));
    serverAssert((flags & SRI_MASTER) || master != NULL);

    /* Check address validity. */
    addr = createSentinelAddr(hostname, port);
    if (addr == NULL)
        return NULL;

    /* For slaves use ip:port as name. */
    if (flags & SRI_SLAVE)
    {
        anetFormatAddr(slavename, sizeof(slavename), hostname, port);
        name = slavename;
    }

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a master is used multiple times inside the configuration or
     * if we try to add multiple times a slave or sentinel with same ip/port
     * to a master. */
    if (flags & SRI_MASTER)
        table = sentinel.masters;   // master 节点添加到 sentinel.masters 表中. FIXME: sentinel 是在什么时候创建的
    else if (flags & SRI_SLAVE)
        table = master->slaves;     // slave 节点被添加到 sentinelRedisInstance.slaves 表中
    else if (flags & SRI_SENTINEL)
        table = master->sentinels;

    sdsname = sdsnew(name);
    if (dictFind(table, sdsname))
    {
        releaseSentinelAddr(addr);
        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    /* Create the instance object. */
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    ri->flags = flags;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->config_epoch = 0;
    ri->addr = addr;
    ri->link = createInstanceLink();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_master_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = master ? master->down_after_period : SENTINEL_DEFAULT_DOWN_AFTER;
    ri->master_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->auth_user = NULL;
    ri->slave_priority = SENTINEL_DEFAULT_SLAVE_PRIORITY;
    ri->slave_reconf_sent_time = 0;
    ri->slave_master_host = NULL;
    ri->slave_master_port = 0;
    ri->slave_master_link_status = SENTINEL_MASTER_LINK_STATUS_DOWN;
    ri->slave_repl_offset = 0;
    ri->sentinels = dictCreate(&instancesDictType, NULL);
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->master = master;
    ri->slaves = dictCreate(&instancesDictType, NULL);
    ri->info_refresh = 0;
    ri->renamed_commands = dictCreate(&renamedCommandsDictType, NULL);

    /* Failover state. */
    ri->leader = NULL;
    ri->leader_epoch = 0;
    ri->failover_epoch = 0;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->failover_delay_logged = 0;
    ri->promoted_slave = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;
    ri->info = NULL;

    /* Role */
    ri->role_reported = ri->flags & (SRI_MASTER | SRI_SLAVE);
    ri->role_reported_time = mstime();
    ri->slave_conf_change_time = mstime();

    /* Add into the right table. */
    dictAdd(table, ri->name, ri);
    return ri;
}

/* Release this instance and all its slaves, sentinels, hiredis connections.
 * This function does not take care of unlinking the instance from the main
 * masters table (if it is a master) or from its master sentinels/slaves table
 * if it is a slave or sentinel. */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri)
{
    /* Release all its slaves or sentinels if any. */
    dictRelease(ri->sentinels);
    dictRelease(ri->slaves);

    /* Disconnect the instance. */
    releaseInstanceLink(ri->link, ri);

    /* Free other resources. */
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->slave_master_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    sdsfree(ri->auth_user);
    sdsfree(ri->info);
    releaseSentinelAddr(ri->addr);
    dictRelease(ri->renamed_commands);

    /* Clear state into the master if needed. */
    if ((ri->flags & SRI_SLAVE) && (ri->flags & SRI_PROMOTED) && ri->master)
        ri->master->promoted_slave = NULL;

    zfree(ri);
}

/* Lookup a slave in a master Redis instance, by ip and port. */
sentinelRedisInstance *sentinelRedisInstanceLookupSlave(sentinelRedisInstance *ri, char *ip, int port)
{
    sds key;
    sentinelRedisInstance *slave;
    char buf[NET_PEER_ID_LEN];

    serverAssert(ri->flags & SRI_MASTER);
    anetFormatAddr(buf, sizeof(buf), ip, port);
    key = sdsnew(buf);
    slave = dictFetchValue(ri->slaves, key);
    sdsfree(key);
    return slave;
}

/* Return the name of the type of the instance as a string. */
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri)
{
    if (ri->flags & SRI_MASTER)
        return "master";
    else if (ri->flags & SRI_SLAVE)
        return "slave";
    else if (ri->flags & SRI_SENTINEL)
        return "sentinel";
    else
        return "unknown";
}

/* This function remove the Sentinel with the specified ID from the
 * specified master.
 *
 * If "runid" is NULL the function returns ASAP.
 *
 * This function is useful because on Sentinels address switch, we want to
 * remove our old entry and add a new one for the same ID but with the new
 * address.
 *
 * The function returns 1 if the matching Sentinel was removed, otherwise
 * 0 if there was no Sentinel with this ID. */
int removeMatchingSentinelFromMaster(sentinelRedisInstance *master, char *runid)
{
    dictIterator *di;
    dictEntry *de;
    int removed = 0;

    if (runid == NULL)
        return 0;

    di = dictGetSafeIterator(master->sentinels);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->runid && strcmp(ri->runid, runid) == 0)
        {
            dictDelete(master->sentinels, ri->name);
            removed++;
        }
    }
    dictReleaseIterator(di);
    return removed;
}

/* Search an instance with the same runid, ip and port into a dictionary
 * of instances. Return NULL if not found, otherwise return the instance
 * pointer.
 *
 * runid or ip can be NULL. In such a case the search is performed only
 * by the non-NULL field. */
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid)
{
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *instance = NULL;

    serverAssert(ip || runid); /* User must pass at least one search param. */
    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (runid && !ri->runid)
            continue;
        if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
            (ip == NULL || (strcmp(ri->addr->ip, ip) == 0 &&
                            ri->addr->port == port)))
        {
            instance = ri;
            break;
        }
    }
    dictReleaseIterator(di);
    return instance;
}

/* Master lookup by name */
sentinelRedisInstance *sentinelGetMasterByName(char *name)
{
    sentinelRedisInstance *ri;
    sds sdsname = sdsnew(name);

    ri = dictFetchValue(sentinel.masters, sdsname);
    sdsfree(sdsname);
    return ri;
}

/* Add the specified flags to all the instances in the specified dictionary. */
void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags)
{
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags |= flags;
    }
    dictReleaseIterator(di);
}

/* Remove the specified flags to all the instances in the specified
 * dictionary. */
void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags)
{
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags &= ~flags;
    }
    dictReleaseIterator(di);
}

/* Reset the state of a monitored master:
 * 1) Remove all slaves.
 * 2) Remove all sentinels.
 * 3) Remove most of the flags resulting from runtime operations.
 * 4) Reset timers to their default value. For example after a reset it will be
 *    possible to failover again the same master ASAP, without waiting the
 *    failover timeout delay.
 * 5) In the process of doing this undo the failover if in progress.
 * 6) Disconnect the connections with the master (will reconnect automatically).
 */

#define SENTINEL_RESET_NO_SENTINELS (1 << 0)
void sentinelResetMaster(sentinelRedisInstance *ri, int flags)
{
    serverAssert(ri->flags & SRI_MASTER);
    dictRelease(ri->slaves);
    ri->slaves = dictCreate(&instancesDictType, NULL);
    if (!(flags & SENTINEL_RESET_NO_SENTINELS))
    {
        dictRelease(ri->sentinels);
        ri->sentinels = dictCreate(&instancesDictType, NULL);
    }
    instanceLinkCloseConnection(ri->link, ri->link->cc);
    instanceLinkCloseConnection(ri->link, ri->link->pc);
    ri->flags &= SRI_MASTER;
    if (ri->leader)
    {
        sdsfree(ri->leader);
        ri->leader = NULL;
    }
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0; /* We can failover again ASAP. */
    ri->promoted_slave = NULL;
    sdsfree(ri->runid);
    sdsfree(ri->slave_master_host);
    ri->runid = NULL;
    ri->slave_master_host = NULL;
    ri->link->act_ping_time = mstime();
    ri->link->last_ping_time = 0;
    ri->link->last_avail_time = mstime();
    ri->link->last_pong_time = mstime();
    ri->role_reported_time = mstime();
    ri->role_reported = SRI_MASTER;
    if (flags & SENTINEL_GENERATE_EVENT)
        sentinelEvent(LL_WARNING, "+reset-master", ri, "%@");
}

/* Call sentinelResetMaster() on every master with a name matching the specified
 * pattern. */
int sentinelResetMastersByPattern(char *pattern, int flags)
{
    dictIterator *di;
    dictEntry *de;
    int reset = 0;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->name)
        {
            if (stringmatch(pattern, ri->name, 0))
            {
                sentinelResetMaster(ri, flags);
                reset++;
            }
        }
    }
    dictReleaseIterator(di);
    return reset;
}

/* Reset the specified master with sentinelResetMaster(), and also change
 * the ip:port address, but take the name of the instance unmodified.
 *
 * This is used to handle the +switch-master event.
 *
 * The function returns C_ERR if the address can't be resolved for some
 * reason. Otherwise C_OK is returned.  */
int sentinelResetMasterAndChangeAddress(sentinelRedisInstance *master, char *ip, int port)
{
    sentinelAddr *oldaddr, *newaddr;
    sentinelAddr **slaves = NULL;
    int numslaves = 0, j;
    dictIterator *di;
    dictEntry *de;

    newaddr = createSentinelAddr(ip, port);
    if (newaddr == NULL)
        return C_ERR;

    /* Make a list of slaves to add back after the reset.
     * Don't include the one having the address we are switching to. */
    di = dictGetIterator(master->slaves);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (sentinelAddrIsEqual(slave->addr, newaddr))
            continue;
        slaves = zrealloc(slaves, sizeof(sentinelAddr *) * (numslaves + 1));
        slaves[numslaves++] = createSentinelAddr(slave->addr->ip,
                                                 slave->addr->port);
    }
    dictReleaseIterator(di);

    /* If we are switching to a different address, include the old address
     * as a slave as well, so that we'll be able to sense / reconfigure
     * the old master. */
    if (!sentinelAddrIsEqual(newaddr, master->addr))
    {
        slaves = zrealloc(slaves, sizeof(sentinelAddr *) * (numslaves + 1));
        slaves[numslaves++] = createSentinelAddr(master->addr->ip,
                                                 master->addr->port);
    }

    /* Reset and switch address. */
    sentinelResetMaster(master, SENTINEL_RESET_NO_SENTINELS);
    oldaddr = master->addr;
    master->addr = newaddr;
    master->o_down_since_time = 0;
    master->s_down_since_time = 0;

    /* Add slaves back. */
    for (j = 0; j < numslaves; j++)
    {
        sentinelRedisInstance *slave;

        slave = createSentinelRedisInstance(NULL, SRI_SLAVE, slaves[j]->ip,
                                            slaves[j]->port, master->quorum, master);
        releaseSentinelAddr(slaves[j]);
        if (slave)
            sentinelEvent(LL_NOTICE, "+slave", slave, "%@");
    }
    zfree(slaves);

    /* Release the old address at the end so we are safe even if the function
     * gets the master->addr->ip and master->addr->port as arguments. */
    releaseSentinelAddr(oldaddr);
    sentinelFlushConfig();
    return C_OK;
}

/* Return non-zero if there was no SDOWN or ODOWN error associated to this
 * instance in the latest 'ms' milliseconds. */
int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms)
{
    mstime_t most_recent;

    most_recent = ri->s_down_since_time;
    if (ri->o_down_since_time > most_recent)
        most_recent = ri->o_down_since_time;
    return most_recent == 0 || (mstime() - most_recent) > ms;
}

/* Return the current master address, that is, its address or the address
 * of the promoted slave if already operational. */
sentinelAddr *sentinelGetCurrentMasterAddress(sentinelRedisInstance *master)
{
    /* If we are failing over the master, and the state is already
     * SENTINEL_FAILOVER_STATE_RECONF_SLAVES or greater, it means that we
     * already have the new configuration epoch in the master, and the
     * slave acknowledged the configuration switch. Advertise the new
     * address. */
    if ((master->flags & SRI_FAILOVER_IN_PROGRESS) &&
        master->promoted_slave &&
        master->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_SLAVES)
    {
        return master->promoted_slave->addr;
    }
    else
    {
        return master->addr;
    }
}

/* This function sets the down_after_period field value in 'master' to all
 * the slaves and sentinel instances connected to this master. */
void sentinelPropagateDownAfterPeriod(sentinelRedisInstance *master)
{
    dictIterator *di;
    dictEntry *de;
    int j;
    dict *d[] = {master->slaves, master->sentinels, NULL};

    for (j = 0; d[j]; j++)
    {
        di = dictGetIterator(d[j]);
        while ((de = dictNext(di)) != NULL)
        {
            sentinelRedisInstance *ri = dictGetVal(de);
            ri->down_after_period = master->down_after_period;
        }
        dictReleaseIterator(di);
    }
}

char *sentinelGetInstanceTypeString(sentinelRedisInstance *ri)
{
    if (ri->flags & SRI_MASTER)
        return "master";
    else if (ri->flags & SRI_SLAVE)
        return "slave";
    else if (ri->flags & SRI_SENTINEL)
        return "sentinel";
    else
        return "unknown";
}

/* This function is used in order to send commands to Redis instances: the
 * commands we send from Sentinel may be renamed, a common case is a master
 * with CONFIG and SLAVEOF commands renamed for security concerns. In that
 * case we check the ri->renamed_command table (or if the instance is a slave,
 * we check the one of the master), and map the command that we should send
 * to the set of renamed commads. However, if the command was not renamed,
 * we just return "command" itself. */
char *sentinelInstanceMapCommand(sentinelRedisInstance *ri, char *command)
{
    sds sc = sdsnew(command);
    if (ri->master)
        ri = ri->master;
    char *retval = dictFetchValue(ri->renamed_commands, sc);
    sdsfree(sc);
    return retval ? retval : command;
}

/* ============================ Config handling ============================= */
// 解析哨兵模式的配置文件
char *sentinelHandleConfiguration(char **argv, int argc)
{
    sentinelRedisInstance *ri;
   /* 解析 monitor 配置，格式为 monitor <name> <host> <port> <quorum>
    * <name> 代表 监控的主服务器节点别名
    * <host> 代表 监控的主服务器节点 IP 地址
    * <port> 代表 监控的主服务器节点 端口
    * <quorum> 代表 主服务器节点 宕机判定 需要有几个 sentine 节点同意 */
    if (!strcasecmp(argv[0], "monitor") && argc == 5)
    {
        /* monitor <name> <host> <port> <quorum> */
        int quorum = atoi(argv[4]); // <quorum> 字段不能小于 0
        if (quorum <= 0)
            return "Quorum must be 1 or greater.";

        // 创建一个 master 节点实例，并加入到 sentinel 节点的监控列表 sentinel.masters 中
        if (createSentinelRedisInstance(argv[1], SRI_MASTER, argv[2], atoi(argv[3]), quorum, NULL) == NULL)
        {
            switch (errno)
            {
            case EBUSY:
                return "Duplicated master name.";
            case ENOENT:
                return "Can't resolve master instance hostname.";
            case EINVAL:
                return "Invalid port number";
            }
        }
    }
    else if (!strcasecmp(argv[0], "down-after-milliseconds") && argc == 3)
    {
        /* down-after-milliseconds <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        ri->down_after_period = atoi(argv[2]);
        if (ri->down_after_period <= 0)
            return "negative or zero time parameter.";
        sentinelPropagateDownAfterPeriod(ri);
    }
    else if (!strcasecmp(argv[0], "failover-timeout") && argc == 3)
    {
        /* failover-timeout <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        ri->failover_timeout = atoi(argv[2]);
        if (ri->failover_timeout <= 0)
            return "negative or zero time parameter.";
    }
    else if (!strcasecmp(argv[0], "parallel-syncs") && argc == 3)
    {
        /* parallel-syncs <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        ri->parallel_syncs = atoi(argv[2]);
    }
    else if (!strcasecmp(argv[0], "notification-script") && argc == 3)
    {
        /* notification-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        if (access(argv[2], X_OK) == -1)
            return "Notification script seems non existing or non executable.";
        ri->notification_script = sdsnew(argv[2]);
    }
    else if (!strcasecmp(argv[0], "client-reconfig-script") && argc == 3)
    {
        /* client-reconfig-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        if (access(argv[2], X_OK) == -1)
            return "Client reconfiguration script seems non existing or "
                   "non executable.";
        ri->client_reconfig_script = sdsnew(argv[2]);
    }
    else if (!strcasecmp(argv[0], "auth-pass") && argc == 3)
    {
        /* auth-pass <name> <password> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        ri->auth_pass = sdsnew(argv[2]);
    }
    else if (!strcasecmp(argv[0], "auth-user") && argc == 3)
    {
        /* auth-user <name> <username> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        ri->auth_user = sdsnew(argv[2]);
    }
    else if (!strcasecmp(argv[0], "current-epoch") && argc == 2)
    {
        /* current-epoch <epoch> */
        unsigned long long current_epoch = strtoull(argv[1], NULL, 10);
        if (current_epoch > sentinel.current_epoch)
            sentinel.current_epoch = current_epoch;
    }
    else if (!strcasecmp(argv[0], "myid") && argc == 2)
    {
        if (strlen(argv[1]) != CONFIG_RUN_ID_SIZE)
            return "Malformed Sentinel id in myid option.";
        memcpy(sentinel.myid, argv[1], CONFIG_RUN_ID_SIZE);
    }
    else if (!strcasecmp(argv[0], "config-epoch") && argc == 3)
    {
        /* config-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        ri->config_epoch = strtoull(argv[2], NULL, 10);
        /* The following update of current_epoch is not really useful as
         * now the current epoch is persisted on the config file, but
         * we leave this check here for redundancy. */
        if (ri->config_epoch > sentinel.current_epoch)
            sentinel.current_epoch = ri->config_epoch;
    }
    else if (!strcasecmp(argv[0], "leader-epoch") && argc == 3)
    {
        /* leader-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        ri->leader_epoch = strtoull(argv[2], NULL, 10);
    }
    else if ((!strcasecmp(argv[0], "known-slave") ||
              !strcasecmp(argv[0], "known-replica")) &&
             argc == 4)
    {
        sentinelRedisInstance *slave;

        /* known-replica <name> <ip> <port> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        if ((slave = createSentinelRedisInstance(NULL, SRI_SLAVE, argv[2],
                                                 atoi(argv[3]), ri->quorum, ri)) == NULL)
        {
            return "Wrong hostname or port for replica.";
        }
    }
    else if (!strcasecmp(argv[0], "known-sentinel") &&
             (argc == 4 || argc == 5))
    {
        sentinelRedisInstance *si;

        if (argc == 5)
        { /* Ignore the old form without runid. */
            /* known-sentinel <name> <ip> <port> [runid] */
            ri = sentinelGetMasterByName(argv[1]);
            if (!ri)
                return "No such master with specified name.";
            if ((si = createSentinelRedisInstance(argv[4], SRI_SENTINEL, argv[2],
                                                  atoi(argv[3]), ri->quorum, ri)) == NULL)
            {
                return "Wrong hostname or port for sentinel.";
            }
            si->runid = sdsnew(argv[4]);
            sentinelTryConnectionSharing(si);
        }
    }
    else if (!strcasecmp(argv[0], "rename-command") && argc == 4)
    {
        /* rename-command <name> <command> <renamed-command> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri)
            return "No such master with specified name.";
        sds oldcmd = sdsnew(argv[2]);
        sds newcmd = sdsnew(argv[3]);
        if (dictAdd(ri->renamed_commands, oldcmd, newcmd) != DICT_OK)
        {
            sdsfree(oldcmd);
            sdsfree(newcmd);
            return "Same command renamed multiple times with rename-command.";
        }
    }
    else if (!strcasecmp(argv[0], "announce-ip") && argc == 2)
    {
        /* announce-ip <ip-address> */
        if (strlen(argv[1]))
            sentinel.announce_ip = sdsnew(argv[1]);
    }
    else if (!strcasecmp(argv[0], "announce-port") && argc == 2)
    {
        /* announce-port <port> */
        sentinel.announce_port = atoi(argv[1]);
    }
    else if (!strcasecmp(argv[0], "deny-scripts-reconfig") && argc == 2)
    {
        /* deny-scripts-reconfig <yes|no> */
        if ((sentinel.deny_scripts_reconfig = yesnotoi(argv[1])) == -1)
        {
            return "Please specify yes or no for the "
                   "deny-scripts-reconfig options.";
        }
    }
    else
    {
        return "Unrecognized sentinel configuration statement.";
    }
    return NULL;
}

/* Implements CONFIG REWRITE for "sentinel" option.
 * This is used not just to rewrite the configuration given by the user
 * (the configured masters) but also in order to retain the state of
 * Sentinel across restarts: config epoch of masters, associated slaves
 * and sentinel instances, and so forth. */
void rewriteConfigSentinelOption(struct rewriteConfigState *state)
{
    dictIterator *di, *di2;
    dictEntry *de;
    sds line;

    /* sentinel unique ID. */
    line = sdscatprintf(sdsempty(), "sentinel myid %s", sentinel.myid);
    rewriteConfigRewriteLine(state, "sentinel", line, 1);

    /* sentinel deny-scripts-reconfig. */
    line = sdscatprintf(sdsempty(), "sentinel deny-scripts-reconfig %s",
                        sentinel.deny_scripts_reconfig ? "yes" : "no");
    rewriteConfigRewriteLine(state, "sentinel", line,
                             sentinel.deny_scripts_reconfig != SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG);

    /* For every master emit a "sentinel monitor" config entry. */
    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *master, *ri;
        sentinelAddr *master_addr;

        /* sentinel monitor */
        master = dictGetVal(de);
        master_addr = sentinelGetCurrentMasterAddress(master);
        line = sdscatprintf(sdsempty(), "sentinel monitor %s %s %d %d",
                            master->name, master_addr->ip, master_addr->port,
                            master->quorum);
        rewriteConfigRewriteLine(state, "sentinel", line, 1);

        /* sentinel down-after-milliseconds */
        if (master->down_after_period != SENTINEL_DEFAULT_DOWN_AFTER)
        {
            line = sdscatprintf(sdsempty(),
                                "sentinel down-after-milliseconds %s %ld",
                                master->name, (long)master->down_after_period);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel failover-timeout */
        if (master->failover_timeout != SENTINEL_DEFAULT_FAILOVER_TIMEOUT)
        {
            line = sdscatprintf(sdsempty(),
                                "sentinel failover-timeout %s %ld",
                                master->name, (long)master->failover_timeout);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel parallel-syncs */
        if (master->parallel_syncs != SENTINEL_DEFAULT_PARALLEL_SYNCS)
        {
            line = sdscatprintf(sdsempty(),
                                "sentinel parallel-syncs %s %d",
                                master->name, master->parallel_syncs);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel notification-script */
        if (master->notification_script)
        {
            line = sdscatprintf(sdsempty(),
                                "sentinel notification-script %s %s",
                                master->name, master->notification_script);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel client-reconfig-script */
        if (master->client_reconfig_script)
        {
            line = sdscatprintf(sdsempty(),
                                "sentinel client-reconfig-script %s %s",
                                master->name, master->client_reconfig_script);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel auth-pass & auth-user */
        if (master->auth_pass)
        {
            line = sdscatprintf(sdsempty(),
                                "sentinel auth-pass %s %s",
                                master->name, master->auth_pass);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        if (master->auth_user)
        {
            line = sdscatprintf(sdsempty(),
                                "sentinel auth-user %s %s",
                                master->name, master->auth_user);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel config-epoch */
        line = sdscatprintf(sdsempty(),
                            "sentinel config-epoch %s %llu",
                            master->name, (unsigned long long)master->config_epoch);
        rewriteConfigRewriteLine(state, "sentinel", line, 1);

        /* sentinel leader-epoch */
        line = sdscatprintf(sdsempty(),
                            "sentinel leader-epoch %s %llu",
                            master->name, (unsigned long long)master->leader_epoch);
        rewriteConfigRewriteLine(state, "sentinel", line, 1);

        /* sentinel known-slave */
        di2 = dictGetIterator(master->slaves);
        while ((de = dictNext(di2)) != NULL)
        {
            sentinelAddr *slave_addr;

            ri = dictGetVal(de);
            slave_addr = ri->addr;

            /* If master_addr (obtained using sentinelGetCurrentMasterAddress()
             * so it may be the address of the promoted slave) is equal to this
             * slave's address, a failover is in progress and the slave was
             * already successfully promoted. So as the address of this slave
             * we use the old master address instead. */
            if (sentinelAddrIsEqual(slave_addr, master_addr))
                slave_addr = master->addr;
            line = sdscatprintf(sdsempty(),
                                "sentinel known-replica %s %s %d",
                                master->name, slave_addr->ip, slave_addr->port);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }
        dictReleaseIterator(di2);

        /* sentinel known-sentinel */
        di2 = dictGetIterator(master->sentinels);
        while ((de = dictNext(di2)) != NULL)
        {
            ri = dictGetVal(de);
            if (ri->runid == NULL)
                continue;
            line = sdscatprintf(sdsempty(),
                                "sentinel known-sentinel %s %s %d %s",
                                master->name, ri->addr->ip, ri->addr->port, ri->runid);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }
        dictReleaseIterator(di2);

        /* sentinel rename-command */
        di2 = dictGetIterator(master->renamed_commands);
        while ((de = dictNext(di2)) != NULL)
        {
            sds oldname = dictGetKey(de);
            sds newname = dictGetVal(de);
            line = sdscatprintf(sdsempty(),
                                "sentinel rename-command %s %s %s",
                                master->name, oldname, newname);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }
        dictReleaseIterator(di2);
    }

    /* sentinel current-epoch is a global state valid for all the masters. */
    line = sdscatprintf(sdsempty(),
                        "sentinel current-epoch %llu", (unsigned long long)sentinel.current_epoch);
    rewriteConfigRewriteLine(state, "sentinel", line, 1);

    /* sentinel announce-ip. */
    if (sentinel.announce_ip)
    {
        line = sdsnew("sentinel announce-ip ");
        line = sdscatrepr(line, sentinel.announce_ip, sdslen(sentinel.announce_ip));
        rewriteConfigRewriteLine(state, "sentinel", line, 1);
    }

    /* sentinel announce-port. */
    if (sentinel.announce_port)
    {
        line = sdscatprintf(sdsempty(), "sentinel announce-port %d",
                            sentinel.announce_port);
        rewriteConfigRewriteLine(state, "sentinel", line, 1);
    }

    dictReleaseIterator(di);
}

/* This function uses the config rewriting Redis engine in order to persist
 * the state of the Sentinel in the current configuration file.
 *
 * Before returning the function calls fsync() against the generated
 * configuration file to make sure changes are committed to disk.
 *
 * On failure the function logs a warning on the Redis log. */
void sentinelFlushConfig(void)
{
    int fd = -1;
    int saved_hz = server.hz;
    int rewrite_status;

    server.hz = CONFIG_DEFAULT_HZ;
    rewrite_status = rewriteConfig(server.configfile, 0);
    server.hz = saved_hz;

    if (rewrite_status == -1)
        goto werr;
    if ((fd = open(server.configfile, O_RDONLY)) == -1)
        goto werr;
    if (fsync(fd) == -1)
        goto werr;
    if (close(fd) == EOF)
        goto werr;
    return;

werr:
    if (fd != -1)
        close(fd);
    serverLog(LL_WARNING, "WARNING: Sentinel was not able to save the new configuration on disk!!!: %s", strerror(errno));
}

/* ====================== hiredis connection handling ======================= */

/* Send the AUTH command with the specified master password if needed.
 * Note that for slaves the password set for the master is used.
 *
 * In case this Sentinel requires a password as well, via the "requirepass"
 * configuration directive, we assume we should use the local password in
 * order to authenticate when connecting with the other Sentinels as well.
 * So basically all the Sentinels share the same password and use it to
 * authenticate reciprocally.
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails Sentinel will detect the instance down,
 * will disconnect and reconnect the link and so forth. */
void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c)
{
    char *auth_pass = NULL;
    char *auth_user = NULL;

    if (ri->flags & SRI_MASTER)
    {
        auth_pass = ri->auth_pass;
        auth_user = ri->auth_user;
    }
    else if (ri->flags & SRI_SLAVE)
    {
        auth_pass = ri->master->auth_pass;
        auth_user = ri->master->auth_user;
    }
    else if (ri->flags & SRI_SENTINEL)
    {
        auth_pass = server.requirepass;
        auth_user = NULL;
    }

    if (auth_pass && auth_user == NULL)
    {
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s",
                              sentinelInstanceMapCommand(ri, "AUTH"),
                              auth_pass) == C_OK)
            ri->link->pending_commands++;
    }
    else if (auth_pass && auth_user)
    {
        /* If we also have an username, use the ACL-style AUTH command
         * with two arguments, username and password. */
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s %s",
                              sentinelInstanceMapCommand(ri, "AUTH"),
                              auth_user, auth_pass) == C_OK)
            ri->link->pending_commands++;
    }
}

/* Use CLIENT SETNAME to name the connection in the Redis instance as
 * sentinel-<first_8_chars_of_runid>-<connection_type>
 * The connection type is "cmd" or "pubsub" as specified by 'type'.
 *
 * This makes it possible to list all the sentinel instances connected
 * to a Redis server with CLIENT LIST, grepping for a specific name format. */
void sentinelSetClientName(sentinelRedisInstance *ri, redisAsyncContext *c, char *type)
{
    char name[64];

    snprintf(name, sizeof(name), "sentinel-%.8s-%s", sentinel.myid, type);
    if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri,
                          "%s SETNAME %s",
                          sentinelInstanceMapCommand(ri, "CLIENT"),
                          name) == C_OK)
    {
        ri->link->pending_commands++;
    }
}

static int instanceLinkNegotiateTLS(redisAsyncContext *context)
{
#ifndef USE_OPENSSL
    (void)context;
#else
    if (!redis_tls_ctx)
        return C_ERR;
    SSL *ssl = SSL_new(redis_tls_ctx);
    if (!ssl)
        return C_ERR;

    if (redisInitiateSSL(&context->c, ssl) == REDIS_ERR)
        return C_ERR;
#endif
    return C_OK;
}

/* Create the async connections for the instance link if the link
 * is disconnected. Note that link->disconnected is true even if just
 * one of the two links (commands and pub/sub) is missing.
 *
 * sential 建立和节点 ri 的异步连接: 命令连接和订阅连接
 */
void sentinelReconnectInstance(sentinelRedisInstance *ri)
{
    if (ri->link->disconnected == 0)    // 如果 ri 节点没有断开连接，直接返回
        return;

    if (ri->addr->port == 0)
        return; /* port == 0 means invalid address. */

    instanceLink *link = ri->link;
    mstime_t now = mstime();

    // 如果最近一次重连时间距离当前时间小于 1s, 直接返回
    if (now - ri->link->last_reconn_time < SENTINEL_PING_PERIOD)
        return;

    ri->link->last_reconn_time = now;

    /* Commands connection. */
    if (link->cc == NULL)   // 创建用于发送命令的异步连接
    {
        link->cc = redisAsyncConnectBind(ri->addr->ip, ri->addr->port, NET_FIRST_BIND_ADDR);
        if (!link->cc->err && server.tls_replication && (instanceLinkNegotiateTLS(link->cc) == C_ERR))
        {
            sentinelEvent(LL_DEBUG, "-cmd-link-reconnection", ri, "%@ #Failed to initialize TLS");
            instanceLinkCloseConnection(link, link->cc);
        }
        else if (link->cc->err)
        {
            sentinelEvent(LL_DEBUG, "-cmd-link-reconnection", ri, "%@ #%s",
                          link->cc->errstr);
            instanceLinkCloseConnection(link, link->cc);
        }
        else
        {
            // 连接成功，设置连接属性
            link->pending_commands = 0;
            link->cc_conn_time = mstime();
            link->cc->data = link;
            redisAeAttach(server.el, link->cc); // 将当前连接关联到事件循环中
            redisAsyncSetConnectCallback(link->cc, sentinelLinkEstablishedCallback);        // 设置连接 callback
            redisAsyncSetDisconnectCallback(link->cc, sentinelDisconnectCallback);          // 设置断线 callback
            sentinelSendAuthIfNeeded(ri, link->cc);     // 发送 AUTH, 验证身份信息
            sentinelSetClientName(ri, link->cc, "cmd");

            /* Send a PING ASAP when reconnecting. */
            sentinelSendPing(ri);       // 发送 PING 命令
        }
    }

    /* Pub / Sub */
    // 针对主从节点，建立订阅连接，订阅连接负责接收发布到当前 sentinel 节点上的消息，包括 master 节点信息和其他 sentinel 节点信息
    if ((ri->flags & (SRI_MASTER | SRI_SLAVE)) && link->pc == NULL)
    {
        link->pc = redisAsyncConnectBind(ri->addr->ip, ri->addr->port, NET_FIRST_BIND_ADDR);
        if (!link->pc->err && server.tls_replication && (instanceLinkNegotiateTLS(link->pc) == C_ERR))
        {
            sentinelEvent(LL_DEBUG, "-pubsub-link-reconnection", ri, "%@ #Failed to initialize TLS");
        }
        else if (link->pc->err)
        {
            sentinelEvent(LL_DEBUG, "-pubsub-link-reconnection", ri, "%@ #%s", link->pc->errstr);
            instanceLinkCloseConnection(link, link->pc);
        }
        else
        {
            int retval;

            link->pc_conn_time = mstime();
            link->pc->data = link;
            redisAeAttach(server.el, link->pc);     // 将订阅连接关联到事件循环中
            redisAsyncSetConnectCallback(link->pc, sentinelLinkEstablishedCallback);        // 设置连接回调函数
            redisAsyncSetDisconnectCallback(link->pc, sentinelDisconnectCallback);          // 设置断开连接回调函数
            sentinelSendAuthIfNeeded(ri, link->pc);     // 发送 AUTH 命令认证
            sentinelSetClientName(ri, link->pc, "pubsub");

            /* Now we subscribe to the Sentinels "Hello" channel.
             * 向该订阅连接上发送订阅命令，告知 master/slave 订阅 __sentinel__:hello 频道，以后 sentinel 会通过该频道发布消息。
             * 同时，sentinel 自身也会订阅这个频道，用于从 master/slave 上接收其他 sentinel 节点发送给这些节点的消息。
             *
             * sentinelReceiveHelloMessages 回调函数用于处理 master/slave 节点通过 __sentinel__:hello 频道发布的消息
             *
             * 比如 sentinelA <---> master <---> sentinelB
             * sentinelA 向 master 通过 __sentinel__:hello 频道发布 hello 消息, master 接收到 hello 消息之后，
             * 会将该 hello 消息通过 __sentinel__:hello 频道再次发布出去，由于 sentinelB 同样订阅了该频道，所以 sentinelB 可以从
             * 该频道上接收到 master 发布的 hello 消息，然后 sentinelB 就会知道 sentinelA 节点的存在。反之也成立
             */
            retval = redisAsyncCommand(link->pc,
                                       sentinelReceiveHelloMessages, ri, "%s %s",
                                       sentinelInstanceMapCommand(ri, "SUBSCRIBE"),
                                       SENTINEL_HELLO_CHANNEL);
            if (retval != C_OK)
            {
                /* If we can't subscribe, the Pub/Sub connection is useless
                 * and we can simply disconnect it and try again. */
                instanceLinkCloseConnection(link, link->pc);
                return;
            }
        }
    }

    /* Clear the disconnected status only if we have both the connections
     * (or just the commands connection if this is a sentinel instance). */
    if (link->cc && (ri->flags & SRI_SENTINEL || link->pc))
        link->disconnected = 0; // 关闭 disconnected 标识
}

/* ======================== Redis instances pinging  ======================== */

/* Return true if master looks "sane", that is:
 * 1) It is actually a master in the current configuration.
 * 2) It reports itself as a master.
 * 3) It is not SDOWN or ODOWN.
 * 4) We obtained last INFO no more than two times the INFO period time ago. */
int sentinelMasterLooksSane(sentinelRedisInstance *master)
{
    return master->flags & SRI_MASTER &&
           master->role_reported == SRI_MASTER &&
           (master->flags & (SRI_S_DOWN | SRI_O_DOWN)) == 0 &&
           (mstime() - master->info_refresh) < SENTINEL_INFO_PERIOD * 2;
}

/* Process the INFO output from masters.
 * sentinel 视角: 处理 INFO 命令的响应(sentinel 接收到 master/slave 对 INFO 命令的响应)
 */
void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info)
{
    sds *lines;
    int numlines, j;
    int role = 0;

    /* cache full INFO output for instance */
    // 释放原来 info 命令缓存并创建新的 info 输出缓存
    sdsfree(ri->info);
    ri->info = sdsnew(info);

    /* The following fields must be reset to a given value in the case they
     * are not found at all in the INFO output.
     * 重置主从节点断开时间，防止在 INFO 输出中找不到该信息 */
    ri->master_link_down_time = 0;

    /* Process line by line. */
    lines = sdssplitlen(info, strlen(info), "\r\n", 2, &numlines);  // 解析 INFO 响应的行数
    for (j = 0; j < numlines; j++)  // 解析 INFO 响应
    {
        sentinelRedisInstance *slave;
        sds l = lines[j];

        /* run_id:<40 hex chars>*/
        // run_id 是长度为 40 的字符串，加上 "run_id:" 这几个字符串 总共长度是 47
        // runid 这行的格式为 "run_id:de4a10ef73028f75e4bd9d6e345544b8ed5e9e09\r\n"
        if (sdslen(l) >= 47 && !memcmp(l, "run_id:", 7))
        {
            if (ri->runid == NULL)    // 如果原 runid 为空，则更新为当前 l 内容中的 run_id 内容
            {
                ri->runid = sdsnewlen(l + 7, 40);
            }
            else
            {
                // 如果原 runid 不为空，则更新
                if (strncmp(ri->runid, l + 7, 40) != 0)
                {
                    sentinelEvent(LL_NOTICE, "+reboot", ri, "%@");      // 事件通知
                    sdsfree(ri->runid);                 // 释放原来的 runid
                    ri->runid = sdsnewlen(l + 7, 40);   // 设置新的 runid
                }
            }
        }

        /* old versions: slave0:<ip>,<port>,<state>
         * new versions: slave0:ip=127.0.0.1,port=9999,... */
        // 如果 INFO 命令的目标是 master 节点, sentinel 从 INFO 命令的返回信息中获取该 master 节点的所有 slave 节点的列表,
        // 如果从节点是新增的，则将其加入到监控列表
        if ((ri->flags & SRI_MASTER) && sdslen(l) >= 7 && !memcmp(l, "slave", 5) && isdigit(l[5]))
        {
            char *ip, *port, *end;

            if (strstr(l, "ip=") == NULL)
            {
                /* Old format. */
                ip = strchr(l, ':');
                if (!ip)
                    continue;
                ip++; /* Now ip points to start of ip address. */
                port = strchr(ip, ',');
                if (!port)
                    continue;
                *port = '\0'; /* nul term for easy access. */
                port++;       /* Now port points to start of port number. */
                end = strchr(port, ',');
                if (!end)
                    continue;
                *end = '\0'; /* nul term for easy access. */
            }
            else
            {
                /* New format. */
                ip = strstr(l, "ip=");
                if (!ip)
                    continue;
                ip += 3; /* Now ip points to start of ip address. */
                port = strstr(l, "port=");
                if (!port)
                    continue;
                port += 5; /* Now port points to start of port number. */
                /* Nul term both fields for easy access. */
                end = strchr(ip, ',');
                if (end)
                    *end = '\0';
                end = strchr(port, ',');
                if (end)
                    *end = '\0';
            }

            /* Check if we already have this slave into our table, otherwise add it.
             * 如果 ri.salves 中没有 ip:port 这个节点，说明该节点是新增的从节点，为 ip:port 创建新的实例，然后添加到监控列表 ri.slaves 中. */
            if (sentinelRedisInstanceLookupSlave(ri, ip, atoi(port)) == NULL)
            {
                if ((slave = createSentinelRedisInstance(NULL, SRI_SLAVE, ip,
                                                         atoi(port), ri->quorum, ri)) != NULL)
                {
                    sentinelEvent(LL_NOTICE, "+slave", slave, "%@");        // 发送事件通知
                    sentinelFlushConfig();  // 刷新 config
                }
            }
        }

        /* master_link_down_since_seconds:<seconds> */
        // 解析主从节点断线时长 master_link_down_since_seconds
        if (sdslen(l) >= 32 && !memcmp(l, "master_link_down_since_seconds", 30))
        {
            ri->master_link_down_time = strtoll(l + 31, NULL, 10) * 1000;
        }

        /* role:<role> 解析节点角色*/
        if (sdslen(l) >= 11 && !memcmp(l, "role:master", 11))
            role = SRI_MASTER;
        else if (sdslen(l) >= 10 && !memcmp(l, "role:slave", 10))
            role = SRI_SLAVE;

        // 如果 INFO 命令的目标是 slave 节点，sentinel 从 INFO 命令的返回信息中获取最新的主节点的 ip/port, 如果与历史记录不一致，则执行更新，
        // 然后获取 slave 的优先级，复制偏移量以及和主节点的连接状态并更新
        if (role == SRI_SLAVE)
        {
            /* master_host:<host> */
            if (sdslen(l) >= 12 && !memcmp(l, "master_host:", 12))
            {
                if (ri->slave_master_host == NULL ||
                    strcasecmp(l + 12, ri->slave_master_host))
                {
                    sdsfree(ri->slave_master_host);
                    ri->slave_master_host = sdsnew(l + 12);
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_port:<port> */
            if (sdslen(l) >= 12 && !memcmp(l, "master_port:", 12))
            {
                int slave_master_port = atoi(l + 12);

                if (ri->slave_master_port != slave_master_port)
                {
                    ri->slave_master_port = slave_master_port;
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_link_status:<status> */
            if (sdslen(l) >= 19 && !memcmp(l, "master_link_status:", 19))
            {
                ri->slave_master_link_status =
                    (strcasecmp(l + 19, "up") == 0) ? SENTINEL_MASTER_LINK_STATUS_UP : SENTINEL_MASTER_LINK_STATUS_DOWN;
            }

            /* slave_priority:<priority> 解析并更新 slave 节点的优先级*/
            if (sdslen(l) >= 15 && !memcmp(l, "slave_priority:", 15))
                ri->slave_priority = atoi(l + 15);

            /* slave_repl_offset:<offset> 解析并更新 slave 节点的复制偏移量*/
            if (sdslen(l) >= 18 && !memcmp(l, "slave_repl_offset:", 18))
                ri->slave_repl_offset = strtoull(l + 18, NULL, 10);
        }
    }

    ri->info_refresh = mstime();        // 更新获取 INFO 命令的时间
    sdsfreesplitres(lines, numlines);

    /* ---------------------------- Acting half -----------------------------
     * Some things will not happen if sentinel.tilt is true, but some will
     * still be processed. */

    // 如果节点的角色发生了变化，sentinel 会记录节点新的角色以及上报时间，若哨兵运行在 TILT 模式，则什么都不做，否则会执行主从切换的逻辑

    /* Remember when the role changed. */
    // 如果节点的角色发生了变化，记录节点新的角色和角色转换时间
    if (role != ri->role_reported)
    {
        ri->role_reported_time = mstime();
        ri->role_reported = role;
        if (role == SRI_SLAVE)
            ri->slave_conf_change_time = mstime();
        /* Log the event with +role-change if the new role is coherent or
         * with -role-change if there is a mismatch with the current config. */
        sentinelEvent(LL_VERBOSE,
                      ((ri->flags & (SRI_MASTER | SRI_SLAVE)) == role) ? "+role-change" : "-role-change",
                      ri, "%@ new reported role is %s",
                      role == SRI_MASTER ? "master" : "slave",
                      ri->flags & SRI_MASTER ? "master" : "slave");
    }

    /* None of the following conditions are processed when in tilt mode, so
     * return asap. */
    if (sentinel.tilt)      // 如果哨兵处于 TILT 模式，则什么也不做
        return;

    /***************************** 接下来处理状态转移 *****************************/

    /* Handle master -> slave role switch. */
    // 如果 ri 节点是 master 节点但是 INFO 回复的却是 slave 节点，原因一般是 sentinel 判断不可达，发生了故障转移所致，这里不进行处理
    if ((ri->flags & SRI_MASTER) && role == SRI_SLAVE)
    {
        /* Nothing to do, but masters claiming to be slaves are
         * considered to be unreachable by Sentinel, so eventually
         * a failover will be triggered. */
    }

    /* Handle slave -> master role switch. */
    // 如果 ri 是 slave 节点但是 INFO 回复的却是 master 节点，那就是因为故障转移将 slave 提升成了 master
    if ((ri->flags & SRI_SLAVE) && role == SRI_MASTER)
    {
        /* If this is a promoted slave we can change state to the
         * failover state machine.
         * slave 节点提升成了 master, 并且旧的 master 节点在等待 slave 提升为 master 节点 */
        if ((ri->flags & SRI_PROMOTED) &&
            (ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
            (ri->master->failover_state == SENTINEL_FAILOVER_STATE_WAIT_PROMOTION))
        {
            /* Now that we are sure the slave was reconfigured as a master
             * set the master configuration epoch to the epoch we won the
             * election to perform this failover. This will force the other
             * Sentinels to update their config (assuming there is not
             * a newer one already available). */
            ri->master->config_epoch = ri->master->failover_epoch;
            // 更新旧的 master 节点的状态，该状态会通知旧的 master 节点的所有 slave 节点向新的 master 节点进行同步操作
            ri->master->failover_state = SENTINEL_FAILOVER_STATE_RECONF_SLAVES;
            ri->master->failover_state_change_time = mstime();      // 更新故障转移状态改变的时间
            sentinelFlushConfig();                                  // 刷新配置
            sentinelEvent(LL_WARNING, "+promoted-slave", ri, "%@"); // 事件通知
            // 如果开启了故障模拟标识，而且是提升之后发生故障，直接退出
            if (sentinel.simfailure_flags & SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION)
                sentinelSimFailureCrash();

            sentinelEvent(LL_WARNING, "+failover-state-reconf-slaves", ri->master, "%@");
            sentinelCallClientReconfScript(ri->master, SENTINEL_LEADER, "start", ri->master->addr, ri->addr);   // 重新配置脚本属性
            sentinelForceHelloUpdateForMaster(ri->master);  // 强行发送 HELLO 消息给所有的节点和 sentinel 节点去关联新的 master 节点
        }
        else
        {
            /* A slave turned into a master. We want to force our view and
             * reconfigure as slave. Wait some time after the change before
             * going forward, to receive new configs if any.
             *
             * slave 节点晋升为了新的 master 节点，但是旧的 master 节点在发生故障转移的超时时间以后又重新上线了，
             * 因此要将旧的 master 节点重新设置为 slave 节点
             */
            // slave 晋升为了 master, 我们要强制其重新配置成从节点，等待一些时间，然后无论如何都要让其接收新的配置，默认计算等待的时间为 8s
            mstime_t wait_time = SENTINEL_PUBLISH_PERIOD * 4;

            /**
             * 当前实例已经成为主节点，且在最近的 wait_time 毫秒内，如果当前实例没有出现下线状态，并且实例的主节点实例看起来很健壮，
             * 并且距离角色更新的时间已经超过缓冲的 wait_time
             */
            if (!(ri->flags & SRI_PROMOTED) &&
                sentinelMasterLooksSane(ri->master) &&
                sentinelRedisInstanceNoDownFor(ri, wait_time) &&
                mstime() - ri->role_reported_time > wait_time)
            {
                int retval = sentinelSendSlaveOf(ri,
                                                 ri->master->addr->ip,
                                                 ri->master->addr->port);       // 发送 slaveof 命令，使其成为主节点
                if (retval == C_OK)
                    sentinelEvent(LL_NOTICE, "+convert-to-slave", ri, "%@");
            }
        }
    }

    /* Handle slaves replicating to a different master address.
     * 如果当前实例为从节点，而且 INFO 的回复确实也是 从节点，但是 主节点的 ip/port 发生了改变，
     * 那么需要 从节点重新指向并复制修改过 ip/port 信息的主节点
     */
    if ((ri->flags & SRI_SLAVE) &&
        role == SRI_SLAVE &&
        (ri->slave_master_port != ri->master->addr->port ||
         strcasecmp(ri->slave_master_host, ri->master->addr->ip)))
    {
        mstime_t wait_time = ri->master->failover_timeout;      // 故障转移超时时间

        /* Make sure the master is sane before reconfiguring this instance into a slave. */
        // wait_time 时间内当前实例没有出现下线状态，而且主节点变换 ip/port 信息已经超过了 wait_time
        if (sentinelMasterLooksSane(ri->master) &&
            sentinelRedisInstanceNoDownFor(ri, wait_time) &&
            mstime() - ri->slave_conf_change_time > wait_time)
        {
            // 改变 slaveof 的目标地址
            int retval = sentinelSendSlaveOf(ri,
                                             ri->master->addr->ip,
                                             ri->master->addr->port);
            if (retval == C_OK)
                sentinelEvent(LL_NOTICE, "+fix-slave-config", ri, "%@");
        }
    }

    /* Detect if the slave that is in the process of being reconfigured
     * changed state. */
    // 如果当前实例为从节点，并且 INFO 传递过来的信息确实也是从节点，并且 sentinel 向当前实例发送了 slaveof 命令 或者 当前实例已经在同步新的主节点了
    if ((ri->flags & SRI_SLAVE) && role == SRI_SLAVE &&
        (ri->flags & (SRI_RECONF_SENT | SRI_RECONF_INPROG)))
    {
        /**
         * 将 SRI_RECONF_SENT 状态改为 SRI_RECONF_INPROG，因为 sentinel 向 实例发送了 slaveof 命令，但是当前实例所属主节点的地址已经
         * 和新主节点地址相同，所以将状态改为 SRI_RECONF_INPROG，表示正在同步复制操作
         */
        /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
        if ((ri->flags & SRI_RECONF_SENT) &&
            ri->slave_master_host &&
            strcmp(ri->slave_master_host,
                   ri->master->promoted_slave->addr->ip) == 0 &&
            ri->slave_master_port == ri->master->promoted_slave->addr->port)
        {
            ri->flags &= ~SRI_RECONF_SENT;
            ri->flags |= SRI_RECONF_INPROG;
            sentinelEvent(LL_NOTICE, "+slave-reconf-inprog", ri, "%@");
        }

        /* SRI_RECONF_INPROG -> SRI_RECONF_DONE */
        // 将状态由 SRI_RECONF_INPROG 改为 SRI_RECONF_DONE，表示同步完成
        if ((ri->flags & SRI_RECONF_INPROG) &&
            ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP)
        {
            ri->flags &= ~SRI_RECONF_INPROG;
            ri->flags |= SRI_RECONF_DONE;
            sentinelEvent(LL_NOTICE, "+slave-reconf-done", ri, "%@");
        }
    }
}

// 处理 INFO 命令的响应
void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata)
{
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link)
        return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STRING)
        sentinelRefreshInstanceInfo(ri, r->str);
}

/* Just discard the reply. We use this when we are not monitoring the return
 * value of the command but its effects directly. */
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata)
{
    instanceLink *link = c->data;
    UNUSED(reply);
    UNUSED(privdata);

    if (link)
        link->pending_commands--;
}

// sentinel 接收到 master/slave/sentinel 节点的对于 PING 命令的响应
void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata)
{
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link)
        return;

    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STATUS ||
        r->type == REDIS_REPLY_ERROR)
    {
        /* Update the "instance available" field only if this is an
         * acceptable reply. */
        if (strncmp(r->str, "PONG", 4) == 0 ||
            strncmp(r->str, "LOADING", 7) == 0 ||
            strncmp(r->str, "MASTERDOWN", 10) == 0)
        {
            link->last_avail_time = mstime();
            link->act_ping_time = 0; /* Flag the pong as received. */
        }
        else
        {
            /* Send a SCRIPT KILL command if the instance appears to be
             * down because of a busy script. */
            // 如果回复是 BUSY，那么发送一个 SCRIPT KILL 命令来恢复因为执行脚本而看起来是主观下线状态的节点
            if (strncmp(r->str, "BUSY", 4) == 0 &&
                (ri->flags & SRI_S_DOWN) &&
                !(ri->flags & SRI_SCRIPT_KILL_SENT))
            {
                if (redisAsyncCommand(ri->link->cc,
                                      sentinelDiscardReplyCallback, ri,
                                      "%s KILL",
                                      sentinelInstanceMapCommand(ri, "SCRIPT")) == C_OK)
                {
                    ri->link->pending_commands++;
                }
                ri->flags |= SRI_SCRIPT_KILL_SENT;
            }
        }
    }
    link->last_pong_time = mstime();
}

/* This is called when we get the reply about the PUBLISH command we send
 * to the master to advertise this sentinel.
 *
 * sentinel 视角: sentinel 接收到 PUBLISH 命令的响应的回调函数, sentinel 解析 PUBLISH 命令的响应, 主要是更新一些统计信息
 */
void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata)
{
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link)
        return;
    link->pending_commands--;
    r = reply;

    /* Only update pub_time if we actually published our message. Otherwise
     * we'll retry again in 100 milliseconds. */
    if (r->type != REDIS_REPLY_ERROR)
        ri->last_pub_time = mstime();
}

/* Process a hello message received via Pub/Sub in master or slave instance,
 * or sent directly to this sentinel via the (fake) PUBLISH command of Sentinel.
 *
 * If the master name specified in the message is not known, the message is
 * discarded.
 */
void sentinelProcessHelloMessage(char *hello, int hello_len)
{
    /* Format is composed of 8 tokens:
     * 0=ip,1=port,2=runid,3=current_epoch,4=master_name,
     * 5=master_ip,6=master_port,7=master_config_epoch. */
    int numtokens, port, removed, master_port;
    uint64_t current_epoch, master_config_epoch;
    char **token = sdssplitlen(hello, hello_len, ",", 1, &numtokens);
    sentinelRedisInstance *si, *master;

    if (numtokens == 8)
    {
        /* Obtain a reference to the master this hello message is about */
        master = sentinelGetMasterByName(token[4]);
        if (!master)
            goto cleanup; /* Unknown master, skip the message. */

        /* First, try to see if we already have this sentinel. */
        port = atoi(token[1]);
        master_port = atoi(token[6]);
        si = getSentinelRedisInstanceByAddrAndRunID(
            master->sentinels, token[0], port, token[2]);
        current_epoch = strtoull(token[3], NULL, 10);
        master_config_epoch = strtoull(token[7], NULL, 10);

        if (!si)
        {
            /* If not, remove all the sentinels that have the same runid
             * because there was an address change, and add the same Sentinel
             * with the new address back. */
            removed = removeMatchingSentinelFromMaster(master, token[2]);
            if (removed)
            {
                sentinelEvent(LL_NOTICE, "+sentinel-address-switch", master,
                              "%@ ip %s port %d for %s", token[0], port, token[2]);
            }
            else
            {
                /* Check if there is another Sentinel with the same address this
                 * new one is reporting. What we do if this happens is to set its
                 * port to 0, to signal the address is invalid. We'll update it
                 * later if we get an HELLO message. */
                sentinelRedisInstance *other =
                    getSentinelRedisInstanceByAddrAndRunID(
                        master->sentinels, token[0], port, NULL);
                if (other)
                {
                    sentinelEvent(LL_NOTICE, "+sentinel-invalid-addr", other, "%@");
                    other->addr->port = 0; /* It means: invalid address. */
                    sentinelUpdateSentinelAddressInAllMasters(other);
                }
            }

            /* Add the new sentinel. */
            si = createSentinelRedisInstance(token[2], SRI_SENTINEL,
                                             token[0], port, master->quorum, master);

            if (si)
            {
                if (!removed)
                    sentinelEvent(LL_NOTICE, "+sentinel", si, "%@");
                /* The runid is NULL after a new instance creation and
                 * for Sentinels we don't have a later chance to fill it,
                 * so do it now. */
                si->runid = sdsnew(token[2]);
                sentinelTryConnectionSharing(si);
                if (removed)
                    sentinelUpdateSentinelAddressInAllMasters(si);
                sentinelFlushConfig();
            }
        }

        /* Update local current_epoch if received current_epoch is greater.*/
        if (current_epoch > sentinel.current_epoch)
        {
            sentinel.current_epoch = current_epoch;
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING, "+new-epoch", master, "%llu",
                          (unsigned long long)sentinel.current_epoch);
        }

        /* Update master info if received configuration is newer. */
        if (si && master->config_epoch < master_config_epoch)
        {
            master->config_epoch = master_config_epoch;
            if (master_port != master->addr->port ||
                strcmp(master->addr->ip, token[5]))
            {
                sentinelAddr *old_addr;

                sentinelEvent(LL_WARNING, "+config-update-from", si, "%@");
                sentinelEvent(LL_WARNING, "+switch-master",
                              master, "%s %s %d %s %d",
                              master->name,
                              master->addr->ip, master->addr->port,
                              token[5], master_port);

                old_addr = dupSentinelAddr(master->addr);
                sentinelResetMasterAndChangeAddress(master, token[5], master_port);
                sentinelCallClientReconfScript(master,
                                               SENTINEL_OBSERVER, "start",
                                               old_addr, master->addr);
                releaseSentinelAddr(old_addr);
            }
        }

        /* Update the state of the Sentinel. */
        if (si)
            si->last_hello_time = mstime();
    }

cleanup:
    sdsfreesplitres(token, numtokens);
}

/* This is our Pub/Sub callback for the Hello channel. It's useful in order
 * to discover other sentinels attached at the same master.
 *
 * sentinel 处理 sentinel 消息的回调函数
 */
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata)
{
    sentinelRedisInstance *ri = privdata;
    redisReply *r;
    UNUSED(c);

    if (!reply || !ri)
        return;
    r = reply;

    /* Update the last activity in the pubsub channel. Note that since we
     * receive our messages as well this timestamp can be used to detect
     * if the link is probably disconnected even if it seems otherwise. */
    ri->link->pc_last_activity = mstime();

    /* Sanity check in the reply we expect, so that the code that follows
     * can avoid to check for details. */
    if (r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str, "message") != 0)
        return;

    /* We are not interested in meeting ourselves */
    if (strstr(r->element[2]->str, sentinel.myid) != NULL)
        return;

    sentinelProcessHelloMessage(r->element[2]->str, r->element[2]->len);
}

/* Send a "Hello" message via Pub/Sub to the specified 'ri' Redis
 * instance in order to broadcast the current configuration for this
 * master, and to advertise the existence of this Sentinel at the same time.
 *
 * The message has the following format:
 *
 * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
 * master_name,master_ip,master_port,master_config_epoch.
 *
 * Returns C_OK if the PUBLISH was queued correctly, otherwise
 * C_ERR is returned.
 *
 * sentinel 视角: sentinel 向频道 __sentinel__:hello 发布信息，包含 sentinel 节点和 master 节点信息
 */
int sentinelSendHello(sentinelRedisInstance *ri)
{
    char ip[NET_IP_STR_LEN];
    char payload[NET_IP_STR_LEN + 1024];
    int retval;
    char *announce_ip;
    int announce_port;
    sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? ri : ri->master;     // 获取主节点实例
    sentinelAddr *master_addr = sentinelGetCurrentMasterAddress(master);        // 获取主节点的地址

    if (ri->link->disconnected)
        return C_ERR;

    /* Use the specified announce address if specified, otherwise try to
     * obtain our own IP address. */
    if (sentinel.announce_ip)
    {
        announce_ip = sentinel.announce_ip;
    }
    else
    {
        if (anetSockName(ri->link->cc->c.fd, ip, sizeof(ip), NULL) == -1)
            return C_ERR;
        announce_ip = ip;
    }

    if (sentinel.announce_port)
        announce_port = sentinel.announce_port;
    else if (server.tls_replication && server.tls_port)
        announce_port = server.tls_port;
    else
        announce_port = server.port;

    /* Format and send the Hello message. */
    // 组装 hello 信息，包括 sentinel 地址端口、runid 和 纪元版本信息，master 别名、地址端口 和 纪元版本信息
    snprintf(payload, sizeof(payload),
             "%s,%d,%s,%llu," /* Info about this sentinel. */
             "%s,%s,%d,%llu", /* Info about current master. */
             announce_ip, announce_port, sentinel.myid,
             (unsigned long long)sentinel.current_epoch,
             /* --- */
             master->name, master_addr->ip, master_addr->port,
             (unsigned long long)master->config_epoch);

    // 发送 PUBLISH 命令, sentinelPublishReplyCallback 为 PUBLISH 命令的响应的回调函数
    retval = redisAsyncCommand(ri->link->cc,
                               sentinelPublishReplyCallback, ri, "%s %s %s",
                               sentinelInstanceMapCommand(ri, "PUBLISH"),
                               SENTINEL_HELLO_CHANNEL, payload);
    if (retval != C_OK)
        return C_ERR;
    ri->link->pending_commands++;
    return C_OK;
}

/* Reset last_pub_time in all the instances in the specified dictionary
 * in order to force the delivery of a Hello update ASAP. */
void sentinelForceHelloUpdateDictOfRedisInstances(dict *instances)
{
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(instances);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->last_pub_time >= (SENTINEL_PUBLISH_PERIOD + 1))
            ri->last_pub_time -= (SENTINEL_PUBLISH_PERIOD + 1);
    }
    dictReleaseIterator(di);
}

/* This function forces the delivery of a "Hello" message (see
 * sentinelSendHello() top comment for further information) to all the Redis
 * and Sentinel instances related to the specified 'master'.
 *
 * It is technically not needed since we send an update to every instance
 * with a period of SENTINEL_PUBLISH_PERIOD milliseconds, however when a
 * Sentinel upgrades a configuration it is a good idea to deliver an update
 * to the other Sentinels ASAP. */
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master)
{
    if (!(master->flags & SRI_MASTER))
        return C_ERR;
    if (master->last_pub_time >= (SENTINEL_PUBLISH_PERIOD + 1))
        master->last_pub_time -= (SENTINEL_PUBLISH_PERIOD + 1);
    sentinelForceHelloUpdateDictOfRedisInstances(master->sentinels);
    sentinelForceHelloUpdateDictOfRedisInstances(master->slaves);
    return C_OK;
}

/* Send a PING to the specified instance and refresh the act_ping_time
 * if it is zero (that is, if we received a pong for the previous ping).
 *
 * On error zero is returned, and we can't consider the PING command
 * queued in the connection. */
int sentinelSendPing(sentinelRedisInstance *ri)
{
    int retval = redisAsyncCommand(ri->link->cc,
                                   sentinelPingReplyCallback, ri, "%s",
                                   sentinelInstanceMapCommand(ri, "PING"));
    if (retval == C_OK)
    {
        ri->link->pending_commands++;
        ri->link->last_ping_time = mstime();
        /* We update the active ping time only if we received the pong for
         * the previous ping, otherwise we are technically waiting since the
         * first ping that did not receive a reply. */
        if (ri->link->act_ping_time == 0)
            ri->link->act_ping_time = ri->link->last_ping_time;
        return 1;
    }
    else
    {
        return 0;
    }
}

/* Send periodic PING, INFO, and PUBLISH to the Hello channel to
 * the specified master or slave instance.
 *
 * sentinel 视角: sentinel 向节点 ri 发送 PING/INFO/PUBLISH 命令
 *    PING: sentinel 节点向其他节点(master/slave/sentinel)发送 PING 命令，检测其在线状态
 *    INFO: sentinel 节点向 master 发送 INFO 命令，获取 master 节点上的其他 slave 节点的信息
 *    PUBLISH: sentinel 节点通过 __sentinel__:hello 频道向 master/slave 节点发送 sentinel 节点和 master 节点的信息
 */
void sentinelSendPeriodicCommands(sentinelRedisInstance *ri)
{
    mstime_t now = mstime();
    mstime_t info_period, ping_period;
    int retval;

    /* Return ASAP if we have already a PING or INFO already pending, or
     * in the case the instance is not properly connected. */
    if (ri->link->disconnected)
        return;

    /* For INFO, PING, PUBLISH that are not critical commands to send we
     * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
     * want to use a lot of memory just because a link is not working
     * properly (note that anyway there is a redundant protection about this,
     * that is, the link will be disconnected and reconnected if a long
     * timeout condition is detected. */
    if (ri->link->pending_commands >= SENTINEL_MAX_PENDING_COMMANDS * ri->link->refcount)
        return;

    /* If this is a slave of a master in O_DOWN condition we start sending
     * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
     * period. In this state we want to closely monitor slaves in case they
     * are turned into masters by another Sentinel, or by the sysadmin.
     *
     * Similarly we monitor the INFO output more often if the slave reports
     * to be disconnected from the master, so that we can have a fresh
     * disconnection time figure.
     * 对于 slave 节点来说, 如果它的 master 节点正处于 ODOWN 状态或者正在执行故障转移, 为了更快的捕获 slave 节点的变动,
     * sentinel 节点会将发送 INFO 命令的频率调整为每秒一次，需要更快的发现新的 master 节点从而进行故障转移
     */
    if ((ri->flags & SRI_SLAVE) &&
        ((ri->master->flags & (SRI_O_DOWN | SRI_FAILOVER_IN_PROGRESS)) ||
         (ri->master_link_down_time != 0)))
    {
        info_period = 1000;
    }
    else
    {
        info_period = SENTINEL_INFO_PERIOD;
    }

    /* We ping instances every time the last received pong is older than
     * the configured 'down-after-milliseconds' time, but every second
     * anyway if 'down-after-milliseconds' is greater than 1 second. */
    ping_period = ri->down_after_period;
    if (ping_period > SENTINEL_PING_PERIOD) // 更新 PING 命令的时间间隔
        ping_period = SENTINEL_PING_PERIOD;

    /* Send INFO to masters and slaves, not sentinels. */
    /**
     * 向节点 ri 发送 INFO 命令, 以获取 ri 节点上连接的主从节点
     * 1. ri 节点不能是 sentinel 节点
     * 2. ri 节点没有收到过 INFO 命令 或者 距离接收到上一次 INFO 命令已经超过了 info_period 时间
     *
     * INFO 命令是 sentinel 发送给 master/slave 的, 不能发送给 sentinel 节点
     */
    if ((ri->flags & SRI_SENTINEL) == 0 &&
        (ri->info_refresh == 0 || (now - ri->info_refresh) > info_period))
    {
        // sentinelInfoReplyCallback: 接收到 INFO 命令响应的回调接口
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelInfoReplyCallback, ri, "%s",
                                   sentinelInstanceMapCommand(ri, "INFO"));
        if (retval == C_OK)
            ri->link->pending_commands++;
    }

    /* Send PING to all the three kinds of instances. */
    // 如果发送和回复 PING 命令超时，立即发送 PING 命令
    if ((now - ri->link->last_pong_time) > ping_period &&
        (now - ri->link->last_ping_time) > ping_period / 2)
    {
        sentinelSendPing(ri);   // 发送 PING 命令
    }

    /* PUBLISH hello messages to all the three kinds of instances. */
    // 如果发送 PUBLISH 命令超时，立即发送 PUBLISH 命令
    if ((now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD)
    {
        sentinelSendHello(ri);      // 发送 PUBLISH 命令, 实际上就是向 __sentinel__:hello 频道发布消息
    }
}

/* =========================== SENTINEL command ============================= */

const char *sentinelFailoverStateStr(int state)
{
    switch (state)
    {
    case SENTINEL_FAILOVER_STATE_NONE:
        return "none";
    case SENTINEL_FAILOVER_STATE_WAIT_START:
        return "wait_start";
    case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
        return "select_slave";
    case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
        return "send_slaveof_noone";
    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
        return "wait_promotion";
    case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
        return "reconf_slaves";
    case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG:
        return "update_config";
    default:
        return "unknown";
    }
}

/* Redis instance to Redis protocol representation. */
void addReplySentinelRedisInstance(client *c, sentinelRedisInstance *ri)
{
    char *flags = sdsempty();
    void *mbl;
    int fields = 0;

    mbl = addReplyDeferredLen(c);

    addReplyBulkCString(c, "name");
    addReplyBulkCString(c, ri->name);
    fields++;

    addReplyBulkCString(c, "ip");
    addReplyBulkCString(c, ri->addr->ip);
    fields++;

    addReplyBulkCString(c, "port");
    addReplyBulkLongLong(c, ri->addr->port);
    fields++;

    addReplyBulkCString(c, "runid");
    addReplyBulkCString(c, ri->runid ? ri->runid : "");
    fields++;

    addReplyBulkCString(c, "flags");
    if (ri->flags & SRI_S_DOWN)
        flags = sdscat(flags, "s_down,");
    if (ri->flags & SRI_O_DOWN)
        flags = sdscat(flags, "o_down,");
    if (ri->flags & SRI_MASTER)
        flags = sdscat(flags, "master,");
    if (ri->flags & SRI_SLAVE)
        flags = sdscat(flags, "slave,");
    if (ri->flags & SRI_SENTINEL)
        flags = sdscat(flags, "sentinel,");
    if (ri->link->disconnected)
        flags = sdscat(flags, "disconnected,");
    if (ri->flags & SRI_MASTER_DOWN)
        flags = sdscat(flags, "master_down,");
    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        flags = sdscat(flags, "failover_in_progress,");
    if (ri->flags & SRI_PROMOTED)
        flags = sdscat(flags, "promoted,");
    if (ri->flags & SRI_RECONF_SENT)
        flags = sdscat(flags, "reconf_sent,");
    if (ri->flags & SRI_RECONF_INPROG)
        flags = sdscat(flags, "reconf_inprog,");
    if (ri->flags & SRI_RECONF_DONE)
        flags = sdscat(flags, "reconf_done,");

    if (sdslen(flags) != 0)
        sdsrange(flags, 0, -2); /* remove last "," */
    addReplyBulkCString(c, flags);
    sdsfree(flags);
    fields++;

    addReplyBulkCString(c, "link-pending-commands");
    addReplyBulkLongLong(c, ri->link->pending_commands);
    fields++;

    addReplyBulkCString(c, "link-refcount");
    addReplyBulkLongLong(c, ri->link->refcount);
    fields++;

    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
    {
        addReplyBulkCString(c, "failover-state");
        addReplyBulkCString(c, (char *)sentinelFailoverStateStr(ri->failover_state));
        fields++;
    }

    addReplyBulkCString(c, "last-ping-sent");
    addReplyBulkLongLong(c,
                         ri->link->act_ping_time ? (mstime() - ri->link->act_ping_time) : 0);
    fields++;

    addReplyBulkCString(c, "last-ok-ping-reply");
    addReplyBulkLongLong(c, mstime() - ri->link->last_avail_time);
    fields++;

    addReplyBulkCString(c, "last-ping-reply");
    addReplyBulkLongLong(c, mstime() - ri->link->last_pong_time);
    fields++;

    if (ri->flags & SRI_S_DOWN)
    {
        addReplyBulkCString(c, "s-down-time");
        addReplyBulkLongLong(c, mstime() - ri->s_down_since_time);
        fields++;
    }

    if (ri->flags & SRI_O_DOWN)
    {
        addReplyBulkCString(c, "o-down-time");
        addReplyBulkLongLong(c, mstime() - ri->o_down_since_time);
        fields++;
    }

    addReplyBulkCString(c, "down-after-milliseconds");
    addReplyBulkLongLong(c, ri->down_after_period);
    fields++;

    /* Masters and Slaves */
    if (ri->flags & (SRI_MASTER | SRI_SLAVE))
    {
        addReplyBulkCString(c, "info-refresh");
        addReplyBulkLongLong(c, mstime() - ri->info_refresh);
        fields++;

        addReplyBulkCString(c, "role-reported");
        addReplyBulkCString(c, (ri->role_reported == SRI_MASTER) ? "master" : "slave");
        fields++;

        addReplyBulkCString(c, "role-reported-time");
        addReplyBulkLongLong(c, mstime() - ri->role_reported_time);
        fields++;
    }

    /* Only masters */
    if (ri->flags & SRI_MASTER)
    {
        addReplyBulkCString(c, "config-epoch");
        addReplyBulkLongLong(c, ri->config_epoch);
        fields++;

        addReplyBulkCString(c, "num-slaves");
        addReplyBulkLongLong(c, dictSize(ri->slaves));
        fields++;

        addReplyBulkCString(c, "num-other-sentinels");
        addReplyBulkLongLong(c, dictSize(ri->sentinels));
        fields++;

        addReplyBulkCString(c, "quorum");
        addReplyBulkLongLong(c, ri->quorum);
        fields++;

        addReplyBulkCString(c, "failover-timeout");
        addReplyBulkLongLong(c, ri->failover_timeout);
        fields++;

        addReplyBulkCString(c, "parallel-syncs");
        addReplyBulkLongLong(c, ri->parallel_syncs);
        fields++;

        if (ri->notification_script)
        {
            addReplyBulkCString(c, "notification-script");
            addReplyBulkCString(c, ri->notification_script);
            fields++;
        }

        if (ri->client_reconfig_script)
        {
            addReplyBulkCString(c, "client-reconfig-script");
            addReplyBulkCString(c, ri->client_reconfig_script);
            fields++;
        }
    }

    /* Only slaves */
    if (ri->flags & SRI_SLAVE)
    {
        addReplyBulkCString(c, "master-link-down-time");
        addReplyBulkLongLong(c, ri->master_link_down_time);
        fields++;

        addReplyBulkCString(c, "master-link-status");
        addReplyBulkCString(c,
                            (ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) ? "ok" : "err");
        fields++;

        addReplyBulkCString(c, "master-host");
        addReplyBulkCString(c,
                            ri->slave_master_host ? ri->slave_master_host : "?");
        fields++;

        addReplyBulkCString(c, "master-port");
        addReplyBulkLongLong(c, ri->slave_master_port);
        fields++;

        addReplyBulkCString(c, "slave-priority");
        addReplyBulkLongLong(c, ri->slave_priority);
        fields++;

        addReplyBulkCString(c, "slave-repl-offset");
        addReplyBulkLongLong(c, ri->slave_repl_offset);
        fields++;
    }

    /* Only sentinels */
    if (ri->flags & SRI_SENTINEL)
    {
        addReplyBulkCString(c, "last-hello-message");
        addReplyBulkLongLong(c, mstime() - ri->last_hello_time);
        fields++;

        addReplyBulkCString(c, "voted-leader");
        addReplyBulkCString(c, ri->leader ? ri->leader : "?");
        fields++;

        addReplyBulkCString(c, "voted-leader-epoch");
        addReplyBulkLongLong(c, ri->leader_epoch);
        fields++;
    }

    setDeferredMapLen(c, mbl, fields);
}

/* Output a number of instances contained inside a dictionary as
 * Redis protocol. */
void addReplyDictOfRedisInstances(client *c, dict *instances)
{
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    addReplyArrayLen(c, dictSize(instances));
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplySentinelRedisInstance(c, ri);
    }
    dictReleaseIterator(di);
}

/* Lookup the named master into sentinel.masters.
 * If the master is not found reply to the client with an error and returns
 * NULL. */
sentinelRedisInstance *sentinelGetMasterByNameOrReplyError(client *c,
                                                           robj *name)
{
    sentinelRedisInstance *ri;

    ri = dictFetchValue(sentinel.masters, name->ptr);
    if (!ri)
    {
        addReplyError(c, "No such master with that name");
        return NULL;
    }
    return ri;
}

#define SENTINEL_ISQR_OK 0
#define SENTINEL_ISQR_NOQUORUM (1 << 0)
#define SENTINEL_ISQR_NOAUTH (1 << 1)
int sentinelIsQuorumReachable(sentinelRedisInstance *master, int *usableptr)
{
    dictIterator *di;
    dictEntry *de;
    int usable = 1; /* Number of usable Sentinels. Init to 1 to count myself. */
    int result = SENTINEL_ISQR_OK;
    int voters = dictSize(master->sentinels) + 1; /* Known Sentinels + myself. */

    di = dictGetIterator(master->sentinels);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->flags & (SRI_S_DOWN | SRI_O_DOWN))
            continue;
        usable++;
    }
    dictReleaseIterator(di);

    if (usable < (int)master->quorum)
        result |= SENTINEL_ISQR_NOQUORUM;
    if (usable < voters / 2 + 1)
        result |= SENTINEL_ISQR_NOAUTH;
    if (usableptr)
        *usableptr = usable;
    return result;
}

void sentinelCommand(client *c)
{
    if (!strcasecmp(c->argv[1]->ptr, "masters"))
    {
        /* SENTINEL MASTERS */
        if (c->argc != 2)
            goto numargserr;
        addReplyDictOfRedisInstances(c, sentinel.masters);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "master"))
    {
        /* SENTINEL MASTER <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3)
            goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        addReplySentinelRedisInstance(c, ri);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "slaves") ||
             !strcasecmp(c->argv[1]->ptr, "replicas"))
    {
        /* SENTINEL REPLICAS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3)
            goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c, ri->slaves);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "sentinels"))
    {
        /* SENTINEL SENTINELS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3)
            goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c, ri->sentinels);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "is-master-down-by-addr"))
    {
        /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
         *
         * Arguments:
         *
         * ip and port are the ip and port of the master we want to be
         * checked by Sentinel. Note that the command will not check by
         * name but just by master, in theory different Sentinels may monitor
         * different masters with the same name.
         *
         * current-epoch is needed in order to understand if we are allowed
         * to vote for a failover leader or not. Each Sentinel can vote just
         * one time per epoch.
         *
         * runid is "*" if we are not seeking for a vote from the Sentinel
         * in order to elect the failover leader. Otherwise it is set to the
         * runid we want the Sentinel to vote if it did not already voted.
         */
        sentinelRedisInstance *ri;
        long long req_epoch;
        uint64_t leader_epoch = 0;
        char *leader = NULL;
        long port;
        int isdown = 0;

        if (c->argc != 6)
            goto numargserr;
        if (getLongFromObjectOrReply(c, c->argv[3], &port, NULL) != C_OK ||
            getLongLongFromObjectOrReply(c, c->argv[4], &req_epoch, NULL) != C_OK)
            return;
        ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.masters,
                                                    c->argv[2]->ptr, port, NULL);

        /* It exists? Is actually a master? Is subjectively down? It's down.
         * Note: if we are in tilt mode we always reply with "0". */
        if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
            (ri->flags & SRI_MASTER))
            isdown = 1;

        /* Vote for the master (or fetch the previous vote) if the request
         * includes a runid, otherwise the sender is not seeking for a vote. */
        if (ri && ri->flags & SRI_MASTER && strcasecmp(c->argv[5]->ptr, "*"))
        {
            leader = sentinelVoteLeader(ri, (uint64_t)req_epoch,
                                        c->argv[5]->ptr,
                                        &leader_epoch);
        }

        /* Reply with a three-elements multi-bulk reply:
         * down state, leader, vote epoch. */
        addReplyArrayLen(c, 3);
        addReply(c, isdown ? shared.cone : shared.czero);
        addReplyBulkCString(c, leader ? leader : "*");
        addReplyLongLong(c, (long long)leader_epoch);
        if (leader)
            sdsfree(leader);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "reset"))
    {
        /* SENTINEL RESET <pattern> */
        if (c->argc != 3)
            goto numargserr;
        addReplyLongLong(c, sentinelResetMastersByPattern(c->argv[2]->ptr, SENTINEL_GENERATE_EVENT));
    }
    else if (!strcasecmp(c->argv[1]->ptr, "get-master-addr-by-name"))
    {
        /* SENTINEL GET-MASTER-ADDR-BY-NAME <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3)
            goto numargserr;
        ri = sentinelGetMasterByName(c->argv[2]->ptr);
        if (ri == NULL)
        {
            addReplyNullArray(c);
        }
        else
        {
            sentinelAddr *addr = sentinelGetCurrentMasterAddress(ri);

            addReplyArrayLen(c, 2);
            addReplyBulkCString(c, addr->ip);
            addReplyBulkLongLong(c, addr->port);
        }
    }
    else if (!strcasecmp(c->argv[1]->ptr, "failover"))
    {
        /* SENTINEL FAILOVER <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3)
            goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        {
            addReplySds(c, sdsnew("-INPROG Failover already in progress\r\n"));
            return;
        }
        if (sentinelSelectSlave(ri) == NULL)
        {
            addReplySds(c, sdsnew("-NOGOODSLAVE No suitable replica to promote\r\n"));
            return;
        }
        serverLog(LL_WARNING, "Executing user requested FAILOVER of '%s'",
                  ri->name);
        sentinelStartFailover(ri);
        ri->flags |= SRI_FORCE_FAILOVER;
        addReply(c, shared.ok);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "pending-scripts"))
    {
        /* SENTINEL PENDING-SCRIPTS */

        if (c->argc != 2)
            goto numargserr;
        sentinelPendingScriptsCommand(c);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "monitor"))
    {
        /* SENTINEL MONITOR <name> <ip> <port> <quorum> */
        sentinelRedisInstance *ri;
        long quorum, port;
        char ip[NET_IP_STR_LEN];

        if (c->argc != 6)
            goto numargserr;
        if (getLongFromObjectOrReply(c, c->argv[5], &quorum, "Invalid quorum") != C_OK)
            return;
        if (getLongFromObjectOrReply(c, c->argv[4], &port, "Invalid port") != C_OK)
            return;

        if (quorum <= 0)
        {
            addReplyError(c, "Quorum must be 1 or greater.");
            return;
        }

        /* Make sure the IP field is actually a valid IP before passing it
         * to createSentinelRedisInstance(), otherwise we may trigger a
         * DNS lookup at runtime. */
        if (anetResolveIP(NULL, c->argv[3]->ptr, ip, sizeof(ip)) == ANET_ERR)
        {
            addReplyError(c, "Invalid IP address specified");
            return;
        }

        /* Parameters are valid. Try to create the master instance. */
        ri = createSentinelRedisInstance(c->argv[2]->ptr, SRI_MASTER,
                                         c->argv[3]->ptr, port, quorum, NULL);
        if (ri == NULL)
        {
            switch (errno)
            {
            case EBUSY:
                addReplyError(c, "Duplicated master name");
                break;
            case EINVAL:
                addReplyError(c, "Invalid port number");
                break;
            default:
                addReplyError(c, "Unspecified error adding the instance");
                break;
            }
        }
        else
        {
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING, "+monitor", ri, "%@ quorum %d", ri->quorum);
            addReply(c, shared.ok);
        }
    }
    else if (!strcasecmp(c->argv[1]->ptr, "flushconfig"))
    {
        if (c->argc != 2)
            goto numargserr;
        sentinelFlushConfig();
        addReply(c, shared.ok);
        return;
    }
    else if (!strcasecmp(c->argv[1]->ptr, "remove"))
    {
        /* SENTINEL REMOVE <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3)
            goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        sentinelEvent(LL_WARNING, "-monitor", ri, "%@");
        dictDelete(sentinel.masters, c->argv[2]->ptr);
        sentinelFlushConfig();
        addReply(c, shared.ok);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "ckquorum"))
    {
        /* SENTINEL CKQUORUM <name> */
        sentinelRedisInstance *ri;
        int usable;

        if (c->argc != 3)
            goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        int result = sentinelIsQuorumReachable(ri, &usable);
        if (result == SENTINEL_ISQR_OK)
        {
            addReplySds(c, sdscatfmt(sdsempty(),
                                     "+OK %i usable Sentinels. Quorum and failover authorization "
                                     "can be reached\r\n",
                                     usable));
        }
        else
        {
            sds e = sdscatfmt(sdsempty(),
                              "-NOQUORUM %i usable Sentinels. ", usable);
            if (result & SENTINEL_ISQR_NOQUORUM)
                e = sdscat(e, "Not enough available Sentinels to reach the"
                              " specified quorum for this master");
            if (result & SENTINEL_ISQR_NOAUTH)
            {
                if (result & SENTINEL_ISQR_NOQUORUM)
                    e = sdscat(e, ". ");
                e = sdscat(e, "Not enough available Sentinels to reach the"
                              " majority and authorize a failover");
            }
            e = sdscat(e, "\r\n");
            addReplySds(c, e);
        }
    }
    else if (!strcasecmp(c->argv[1]->ptr, "set"))
    {
        if (c->argc < 3)
            goto numargserr;
        sentinelSetCommand(c);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "info-cache"))
    {
        /* SENTINEL INFO-CACHE <name> */
        if (c->argc < 2)
            goto numargserr;
        mstime_t now = mstime();

        /* Create an ad-hoc dictionary type so that we can iterate
         * a dictionary composed of just the master groups the user
         * requested. */
        dictType copy_keeper = instancesDictType;
        copy_keeper.valDestructor = NULL;
        dict *masters_local = sentinel.masters;
        if (c->argc > 2)
        {
            masters_local = dictCreate(&copy_keeper, NULL);

            for (int i = 2; i < c->argc; i++)
            {
                sentinelRedisInstance *ri;
                ri = sentinelGetMasterByName(c->argv[i]->ptr);
                if (!ri)
                    continue; /* ignore non-existing names */
                dictAdd(masters_local, ri->name, ri);
            }
        }

        /* Reply format:
         *   1.) master name
         *   2.) 1.) info from master
         *       2.) info from replica
         *       ...
         *   3.) other master name
         *   ...
         */
        addReplyArrayLen(c, dictSize(masters_local) * 2);

        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(masters_local);
        while ((de = dictNext(di)) != NULL)
        {
            sentinelRedisInstance *ri = dictGetVal(de);
            addReplyBulkCBuffer(c, ri->name, strlen(ri->name));
            addReplyArrayLen(c, dictSize(ri->slaves) + 1); /* +1 for self */
            addReplyArrayLen(c, 2);
            addReplyLongLong(c, now - ri->info_refresh);
            if (ri->info)
                addReplyBulkCBuffer(c, ri->info, sdslen(ri->info));
            else
                addReplyNull(c);

            dictIterator *sdi;
            dictEntry *sde;
            sdi = dictGetIterator(ri->slaves);
            while ((sde = dictNext(sdi)) != NULL)
            {
                sentinelRedisInstance *sri = dictGetVal(sde);
                addReplyArrayLen(c, 2);
                addReplyLongLong(c, now - sri->info_refresh);
                if (sri->info)
                    addReplyBulkCBuffer(c, sri->info, sdslen(sri->info));
                else
                    addReplyNull(c);
            }
            dictReleaseIterator(sdi);
        }
        dictReleaseIterator(di);
        if (masters_local != sentinel.masters)
            dictRelease(masters_local);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "simulate-failure"))
    {
        /* SENTINEL SIMULATE-FAILURE <flag> <flag> ... <flag> */
        int j;

        sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
        for (j = 2; j < c->argc; j++)
        {
            if (!strcasecmp(c->argv[j]->ptr, "crash-after-election"))
            {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION;
                serverLog(LL_WARNING, "Failure simulation: this Sentinel "
                                      "will crash after being successfully elected as failover "
                                      "leader");
            }
            else if (!strcasecmp(c->argv[j]->ptr, "crash-after-promotion"))
            {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION;
                serverLog(LL_WARNING, "Failure simulation: this Sentinel "
                                      "will crash after promoting the selected replica to master");
            }
            else if (!strcasecmp(c->argv[j]->ptr, "help"))
            {
                addReplyArrayLen(c, 2);
                addReplyBulkCString(c, "crash-after-election");
                addReplyBulkCString(c, "crash-after-promotion");
            }
            else
            {
                addReplyError(c, "Unknown failure simulation specified");
                return;
            }
        }
        addReply(c, shared.ok);
    }
    else
    {
        addReplyErrorFormat(c, "Unknown sentinel subcommand '%s'",
                            (char *)c->argv[1]->ptr);
    }
    return;

numargserr:
    addReplyErrorFormat(c, "Wrong number of arguments for 'sentinel %s'",
                        (char *)c->argv[1]->ptr);
}

#define info_section_from_redis(section_name)                                 \
    do                                                                        \
    {                                                                         \
        if (defsections || allsections || !strcasecmp(section, section_name)) \
        {                                                                     \
            sds redissection;                                                 \
            if (sections++)                                                   \
                info = sdscat(info, "\r\n");                                  \
            redissection = genRedisInfoString(section_name);                  \
            info = sdscatlen(info, redissection, sdslen(redissection));       \
            sdsfree(redissection);                                            \
        }                                                                     \
    } while (0)

/* SENTINEL INFO [section] */
void sentinelInfoCommand(client *c)
{
    if (c->argc > 2)
    {
        addReply(c, shared.syntaxerr);
        return;
    }

    int defsections = 0, allsections = 0;
    char *section = c->argc == 2 ? c->argv[1]->ptr : NULL;
    if (section)
    {
        allsections = !strcasecmp(section, "all");
        defsections = !strcasecmp(section, "default");
    }
    else
    {
        defsections = 1;
    }

    int sections = 0;
    sds info = sdsempty();

    info_section_from_redis("server");
    info_section_from_redis("clients");
    info_section_from_redis("cpu");
    info_section_from_redis("stats");

    if (defsections || allsections || !strcasecmp(section, "sentinel"))
    {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;

        if (sections++)
            info = sdscat(info, "\r\n");
        info = sdscatprintf(info,
                            "# Sentinel\r\n"
                            "sentinel_masters:%lu\r\n"
                            "sentinel_tilt:%d\r\n"
                            "sentinel_running_scripts:%d\r\n"
                            "sentinel_scripts_queue_length:%ld\r\n"
                            "sentinel_simulate_failure_flags:%lu\r\n",
                            dictSize(sentinel.masters),
                            sentinel.tilt,
                            sentinel.running_scripts,
                            listLength(sentinel.scripts_queue),
                            sentinel.simfailure_flags);

        di = dictGetIterator(sentinel.masters);
        while ((de = dictNext(di)) != NULL)
        {
            sentinelRedisInstance *ri = dictGetVal(de);
            char *status = "ok";

            if (ri->flags & SRI_O_DOWN)
                status = "odown";
            else if (ri->flags & SRI_S_DOWN)
                status = "sdown";
            info = sdscatprintf(info,
                                "master%d:name=%s,status=%s,address=%s:%d,"
                                "slaves=%lu,sentinels=%lu\r\n",
                                master_id++, ri->name, status,
                                ri->addr->ip, ri->addr->port,
                                dictSize(ri->slaves),
                                dictSize(ri->sentinels) + 1);
        }
        dictReleaseIterator(di);
    }

    addReplyBulkSds(c, info);
}

/* Implements Sentinel version of the ROLE command. The output is
 * "sentinel" and the list of currently monitored master names. */
void sentinelRoleCommand(client *c)
{
    dictIterator *di;
    dictEntry *de;

    addReplyArrayLen(c, 2);
    addReplyBulkCBuffer(c, "sentinel", 8);
    addReplyArrayLen(c, dictSize(sentinel.masters));

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplyBulkCString(c, ri->name);
    }
    dictReleaseIterator(di);
}

/* SENTINEL SET <mastername> [<option> <value> ...] */
void sentinelSetCommand(client *c)
{
    sentinelRedisInstance *ri;
    int j, changes = 0;
    int badarg = 0; /* Bad argument position for error reporting. */
    char *option;

    if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
        return;

    /* Process option - value pairs. */
    for (j = 3; j < c->argc; j++)
    {
        int moreargs = (c->argc - 1) - j;
        option = c->argv[j]->ptr;
        long long ll;
        int old_j = j; /* Used to know what to log as an event. */

        if (!strcasecmp(option, "down-after-milliseconds") && moreargs > 0)
        {
            /* down-after-millisecodns <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0)
            {
                badarg = j;
                goto badfmt;
            }
            ri->down_after_period = ll;
            sentinelPropagateDownAfterPeriod(ri);
            changes++;
        }
        else if (!strcasecmp(option, "failover-timeout") && moreargs > 0)
        {
            /* failover-timeout <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0)
            {
                badarg = j;
                goto badfmt;
            }
            ri->failover_timeout = ll;
            changes++;
        }
        else if (!strcasecmp(option, "parallel-syncs") && moreargs > 0)
        {
            /* parallel-syncs <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0)
            {
                badarg = j;
                goto badfmt;
            }
            ri->parallel_syncs = ll;
            changes++;
        }
        else if (!strcasecmp(option, "notification-script") && moreargs > 0)
        {
            /* notification-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig)
            {
                addReplyError(c,
                              "Reconfiguration of scripts path is denied for "
                              "security reasons. Check the deny-scripts-reconfig "
                              "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value, X_OK) == -1)
            {
                addReplyError(c,
                              "Notification script seems non existing or non executable");
                goto seterr;
            }
            sdsfree(ri->notification_script);
            ri->notification_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        }
        else if (!strcasecmp(option, "client-reconfig-script") && moreargs > 0)
        {
            /* client-reconfig-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig)
            {
                addReplyError(c,
                              "Reconfiguration of scripts path is denied for "
                              "security reasons. Check the deny-scripts-reconfig "
                              "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value, X_OK) == -1)
            {
                addReplyError(c,
                              "Client reconfiguration script seems non existing or "
                              "non executable");
                goto seterr;
            }
            sdsfree(ri->client_reconfig_script);
            ri->client_reconfig_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        }
        else if (!strcasecmp(option, "auth-pass") && moreargs > 0)
        {
            /* auth-pass <password> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_pass);
            ri->auth_pass = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        }
        else if (!strcasecmp(option, "auth-user") && moreargs > 0)
        {
            /* auth-user <username> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_user);
            ri->auth_user = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        }
        else if (!strcasecmp(option, "quorum") && moreargs > 0)
        {
            /* quorum <count> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0)
            {
                badarg = j;
                goto badfmt;
            }
            ri->quorum = ll;
            changes++;
        }
        else if (!strcasecmp(option, "rename-command") && moreargs > 1)
        {
            /* rename-command <oldname> <newname> */
            sds oldname = c->argv[++j]->ptr;
            sds newname = c->argv[++j]->ptr;

            if ((sdslen(oldname) == 0) || (sdslen(newname) == 0))
            {
                badarg = sdslen(newname) ? j - 1 : j;
                goto badfmt;
            }

            /* Remove any older renaming for this command. */
            dictDelete(ri->renamed_commands, oldname);

            /* If the target name is the same as the source name there
             * is no need to add an entry mapping to itself. */
            if (!dictSdsKeyCaseCompare(NULL, oldname, newname))
            {
                oldname = sdsdup(oldname);
                newname = sdsdup(newname);
                dictAdd(ri->renamed_commands, oldname, newname);
            }
            changes++;
        }
        else
        {
            addReplyErrorFormat(c, "Unknown option or number of arguments for "
                                   "SENTINEL SET '%s'",
                                option);
            goto seterr;
        }

        /* Log the event. */
        int numargs = j - old_j + 1;
        switch (numargs)
        {
        case 2:
            sentinelEvent(LL_WARNING, "+set", ri, "%@ %s %s", (char *)c->argv[old_j]->ptr,
                          (char *)c->argv[old_j + 1]->ptr);
            break;
        case 3:
            sentinelEvent(LL_WARNING, "+set", ri, "%@ %s %s %s", (char *)c->argv[old_j]->ptr,
                          (char *)c->argv[old_j + 1]->ptr,
                          (char *)c->argv[old_j + 2]->ptr);
            break;
        default:
            sentinelEvent(LL_WARNING, "+set", ri, "%@ %s", (char *)c->argv[old_j]->ptr);
            break;
        }
    }

    if (changes)
        sentinelFlushConfig();
    addReply(c, shared.ok);
    return;

badfmt: /* Bad format errors */
    addReplyErrorFormat(c, "Invalid argument '%s' for SENTINEL SET '%s'",
                        (char *)c->argv[badarg]->ptr, option);
seterr:
    if (changes)
        sentinelFlushConfig();
    return;
}

/* Our fake PUBLISH command: it is actually useful only to receive hello messages
 * from the other sentinel instances, and publishing to a channel other than
 * SENTINEL_HELLO_CHANNEL is forbidden.
 *
 * Because we have a Sentinel PUBLISH, the code to send hello messages is the same
 * for all the three kind of instances: masters, slaves, sentinels. */
void sentinelPublishCommand(client *c)
{
    if (strcmp(c->argv[1]->ptr, SENTINEL_HELLO_CHANNEL))
    {
        addReplyError(c, "Only HELLO messages are accepted by Sentinel instances.");
        return;
    }
    sentinelProcessHelloMessage(c->argv[2]->ptr, sdslen(c->argv[2]->ptr));
    addReplyLongLong(c, 1);
}

/* ===================== SENTINEL availability checks ======================= */

/* Is this instance down from our point of view? */
// 故障检测：检测节点是否处于主观下线的状态
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri)
{
    mstime_t elapsed = 0;

    // act_ping_time 为 0 时，表示已经接收到正确的 PING 命令响应; 否则 act_ping_time 为上次发送 PING 命令的时间
    if (ri->link->act_ping_time)
        elapsed = mstime() - ri->link->act_ping_time;       // 获取上一次发送 PING 命令的时间到当前时间
    else if (ri->link->disconnected)            // 如果连接已经断开
        elapsed = mstime() - ri->link->last_avail_time;

    /* Check if we are in need for a reconnection of one of the
     * links, because we are detecting low activity.
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have a
     *    pending ping for more than half the timeout.
     *
     * 判断命令连接是否处于低活跃度: 连接成功之后会发送 PING 命令，但是到现在为止已经超过了 1.5s 没有收到 PING 命令的响应，
     * 可以认为该连接处于低活跃度(或者阻塞状态？)，需要断开该连接然后尝试重新连接
     */
    if (ri->link->cc &&
        (mstime() - ri->link->cc_conn_time) >
            SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        ri->link->act_ping_time != 0 && /* There is a pending ping... */
        /* The pending ping is delayed, and we did not receive
         * error replies as well. */
        (mstime() - ri->link->act_ping_time) > (ri->down_after_period / 2) &&
        (mstime() - ri->link->last_pong_time) > (ri->down_after_period / 2))
    {
        instanceLinkCloseConnection(ri->link, ri->link->cc);        // 断开节点的命令连接
    }

    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     *
     * 检查订阅连接是否处于低活跃度
     */
    if (ri->link->pc &&
        (mstime() - ri->link->pc_conn_time) >
            SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->link->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD * 3))
    {
        instanceLinkCloseConnection(ri->link, ri->link->pc);        // 断开节点的订阅连接
    }

    /* Update the SDOWN flag. We believe the instance is SDOWN if:
     *
     * 1) It is not replying.
     * 2) We believe it is a master, it reports to be a slave for enough time
     *    to meet the down_after_period, plus enough time to get two times
     *    INFO report from the instance.
     *
     * 如果在规定的时间内没有收到 PING 命令的响应，或者哨兵认为它是主节点，而节点上报它正在切换为从节点，但是在指定时间范围内它没有完成角色切换
     */
    if (elapsed > ri->down_after_period ||
        (ri->flags & SRI_MASTER &&
         ri->role_reported == SRI_SLAVE &&
         mstime() - ri->role_reported_time >
             (ri->down_after_period + SENTINEL_INFO_PERIOD * 2)))
    {
        /* Is subjectively down. 设置主观下线标识 */
        if ((ri->flags & SRI_S_DOWN) == 0)
        {
            sentinelEvent(LL_WARNING, "+sdown", ri, "%@");
            ri->s_down_since_time = mstime();
            ri->flags |= SRI_S_DOWN;
        }
    }
    else
    {
        /* Is subjectively up */
        // 如果已经是主观下线，则取消该标识
        if (ri->flags & SRI_S_DOWN)
        {
            sentinelEvent(LL_WARNING, "-sdown", ri, "%@");
            ri->flags &= ~(SRI_S_DOWN | SRI_SCRIPT_KILL_SENT);
        }
    }
}

/* Is this instance down according to the configured quorum?
 *
 * Note that ODOWN is a weak quorum, it only means that enough Sentinels
 * reported in a given time range that the instance was not reachable.
 * However messages can be delayed so there are no strong guarantees about
 * N instances agreeing at the same time about the down state.
 *
 * 故障检测: 检测主节点是否处于客观下线状态
 *
 * NOTE: 这里还没有向其他 sentinel 节点发送 is-master-down-by-addr 命令确认该节点的状态
 */
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master)
{
    dictIterator *di;
    dictEntry *de;
    unsigned int quorum = 0, odown = 0;

    // 如果已经被判定为主观下线
    if (master->flags & SRI_S_DOWN)
    {
        /* Is down for enough sentinels? */
        quorum = 1; /* the current sentinel. 当前哨兵认为下线，投票 1*/

        /* Count all the other sentinels. */
        // 遍历所有监控该主节点的哨兵，检查其他哨兵对该节点下线的判定情况
        di = dictGetIterator(master->sentinels);
        while ((de = dictNext(di)) != NULL)
        {
            sentinelRedisInstance *ri = dictGetVal(de);

            if (ri->flags & SRI_MASTER_DOWN)
                quorum++;
        }

        dictReleaseIterator(di);

        if (quorum >= master->quorum)       // 如果投票数达到要求，则判定该节点客观下线
            odown = 1;
    }

    /* Set the flag accordingly to the outcome. */
    if (odown)
    {
        // 如果主节点还没有改变其状态，则将状态修改为客观下线状态
        if ((master->flags & SRI_O_DOWN) == 0)
        {
            sentinelEvent(LL_WARNING, "+odown", master, "%@ #quorum %d/%d", quorum, master->quorum);
            master->flags |= SRI_O_DOWN;
            master->o_down_since_time = mstime();
        }
    }
    else
    {
        // 如果投票数不足, 取消主观下线状态
        if (master->flags & SRI_O_DOWN)
        {
            sentinelEvent(LL_WARNING, "-odown", master, "%@");
            master->flags &= ~SRI_O_DOWN;
        }
    }
}

/* Receive the SENTINEL is-master-down-by-addr reply, see the
 * sentinelAskMasterStateToOtherSentinels() function for more information. */
void sentinelReceiveIsMasterDownReply(redisAsyncContext *c, void *reply, void *privdata)
{
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link)
        return;
    link->pending_commands--;
    r = reply;

    /* Ignore every error or unexpected reply.
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_MASTER_DOWN flag for timeout anyway. */
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 3 &&
        r->element[0]->type == REDIS_REPLY_INTEGER &&
        r->element[1]->type == REDIS_REPLY_STRING &&
        r->element[2]->type == REDIS_REPLY_INTEGER)
    {
        ri->last_master_down_reply_time = mstime();
        if (r->element[0]->integer == 1)
        {
            ri->flags |= SRI_MASTER_DOWN;
        }
        else
        {
            ri->flags &= ~SRI_MASTER_DOWN;
        }
        if (strcmp(r->element[1]->str, "*"))
        {
            /* If the runid in the reply is not "*" the Sentinel actually
             * replied with a vote. */
            sdsfree(ri->leader);
            if ((long long)ri->leader_epoch != r->element[2]->integer)
                serverLog(LL_WARNING,
                          "%s voted for %s %llu", ri->name,
                          r->element[1]->str,
                          (unsigned long long)r->element[2]->integer);
            ri->leader = sdsnew(r->element[1]->str);
            ri->leader_epoch = r->element[2]->integer;
        }
    }
}

/* If we think the master is down, we start sending
 * SENTINEL IS-MASTER-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum
 * needed to mark the master in ODOWN state and trigger a failover.
 *
 * 向其他 sentinel 节点发送 SENTINEL is-master-down-by-addr 确认节点状态
 */
#define SENTINEL_ASK_FORCED (1 << 0)
void sentinelAskMasterStateToOtherSentinels(sentinelRedisInstance *master, int flags)
{
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(master->sentinels);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);
        mstime_t elapsed = mstime() - ri->last_master_down_reply_time;
        char port[32];
        int retval;

        /* If the master state from other sentinel is too old, we clear it. */
        if (elapsed > SENTINEL_ASK_PERIOD * 5)
        {
            ri->flags &= ~SRI_MASTER_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if master is down to other sentinels if:
         *
         * 1) We believe it is down, or there is a failover in progress.
         * 2) Sentinel is connected.
         * 3) We did not receive the info within SENTINEL_ASK_PERIOD ms. */
        if ((master->flags & SRI_S_DOWN) == 0)
            continue;
        if (ri->link->disconnected)
            continue;
        if (!(flags & SENTINEL_ASK_FORCED) &&
            mstime() - ri->last_master_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        /* Ask */
        ll2string(port, sizeof(port), master->addr->port);
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelReceiveIsMasterDownReply, ri,
                                   "%s is-master-down-by-addr %s %s %llu %s",
                                   sentinelInstanceMapCommand(ri, "SENTINEL"),
                                   master->addr->ip, port,
                                   sentinel.current_epoch,
                                   (master->failover_state > SENTINEL_FAILOVER_STATE_NONE) ? sentinel.myid : "*");
        if (retval == C_OK)
            ri->link->pending_commands++;
    }

    dictReleaseIterator(di);
}

/* =============================== FAILOVER ================================= */

/* Crash because of user request via SENTINEL simulate-failure command. */
void sentinelSimFailureCrash(void)
{
    serverLog(LL_WARNING,
              "Sentinel CRASH because of SENTINEL simulate-failure");
    exit(99);
}

/* Vote for the sentinel with 'req_runid' or return the old vote if already
 * voted for the specified 'req_epoch' or one greater.
 *
 * If a vote is not available returns NULL, otherwise return the Sentinel
 * runid and populate the leader_epoch with the epoch of the vote. */
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch)
{
    if (req_epoch > sentinel.current_epoch)
    {
        sentinel.current_epoch = req_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING, "+new-epoch", master, "%llu",
                      (unsigned long long)sentinel.current_epoch);
    }

    if (master->leader_epoch < req_epoch && sentinel.current_epoch <= req_epoch)
    {
        sdsfree(master->leader);
        master->leader = sdsnew(req_runid);
        master->leader_epoch = sentinel.current_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING, "+vote-for-leader", master, "%s %llu",
                      master->leader, (unsigned long long)master->leader_epoch);
        /* If we did not voted for ourselves, set the master failover start
         * time to now, in order to force a delay before we can start a
         * failover for the same master. */
        if (strcasecmp(master->leader, sentinel.myid))
            master->failover_start_time = mstime() + rand() % SENTINEL_MAX_DESYNC;
    }

    *leader_epoch = master->leader_epoch;
    return master->leader ? sdsnew(master->leader) : NULL;
}

struct sentinelLeader
{
    char *runid;
    unsigned long votes;
};

/* Helper function for sentinelGetLeader, increment the counter
 * relative to the specified runid. */
int sentinelLeaderIncr(dict *counters, char *runid)
{
    dictEntry *existing, *de;
    uint64_t oldval;

    de = dictAddRaw(counters, runid, &existing);
    if (existing)
    {
        oldval = dictGetUnsignedIntegerVal(existing);
        dictSetUnsignedIntegerVal(existing, oldval + 1);
        return oldval + 1;
    }
    else
    {
        serverAssert(de != NULL);
        dictSetUnsignedIntegerVal(de, 1);
        return 1;
    }
}

/* Scan all the Sentinels attached to this master to check if there
 * is a leader for the specified epoch.
 *
 * To be a leader for a given epoch, we should have the majority of
 * the Sentinels we know (ever seen since the last SENTINEL RESET) that
 * reported the same instance as leader for the same epoch.
 *
 * 在所有监控该主节点的 sentinel 中选出一个 leader 来执行对下线 master 的故障转移操作。这里选举出 sentinel leader 使用了 Raft 算法
 */
char *sentinelGetLeader(sentinelRedisInstance *master, uint64_t epoch)
{
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;
    uint64_t leader_epoch;
    uint64_t max_votes = 0;

    serverAssert(master->flags & (SRI_O_DOWN | SRI_FAILOVER_IN_PROGRESS));
    counters = dictCreate(&leaderVotesDictType, NULL);

    voters = dictSize(master->sentinels) + 1; /* All the other sentinels and me.*/

    /* Count other sentinels votes */
    di = dictGetIterator(master->sentinels);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
            sentinelLeaderIncr(counters, ri->leader);
    }
    dictReleaseIterator(di);

    /* Check what's the winner. For the winner to win, it needs two conditions:
     * 1) Absolute majority between voters (50% + 1).
     * 2) And anyway at least master->quorum votes. */
    di = dictGetIterator(counters);
    while ((de = dictNext(di)) != NULL)
    {
        uint64_t votes = dictGetUnsignedIntegerVal(de);

        if (votes > max_votes)
        {
            max_votes = votes;
            winner = dictGetKey(de);
        }
    }
    dictReleaseIterator(di);

    /* Count this Sentinel vote:
     * if this Sentinel did not voted yet, either vote for the most
     * common voted sentinel, or for itself if no vote exists at all. */
    if (winner)
        myvote = sentinelVoteLeader(master, epoch, winner, &leader_epoch);
    else
        myvote = sentinelVoteLeader(master, epoch, sentinel.myid, &leader_epoch);

    if (myvote && leader_epoch == epoch)
    {
        uint64_t votes = sentinelLeaderIncr(counters, myvote);

        if (votes > max_votes)
        {
            max_votes = votes;
            winner = myvote;
        }
    }

    voters_quorum = voters / 2 + 1;
    if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
        winner = NULL;

    winner = winner ? sdsnew(winner) : NULL;
    sdsfree(myvote);
    dictRelease(counters);
    return winner;
}

/* Send SLAVEOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * If Host is NULL the function sends "SLAVEOF NO ONE".
 *
 * The command returns C_OK if the SLAVEOF command was accepted for
 * (later) delivery otherwise C_ERR. The command replies are just
 * discarded. */
int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port)
{
    char portstr[32];
    int retval;

    ll2string(portstr, sizeof(portstr), port);

    /* If host is NULL we send SLAVEOF NO ONE that will turn the instance
     * into a master. */
    if (host == NULL)
    {
        host = "NO";
        memcpy(portstr, "ONE", 4);
    }

    /* In order to send SLAVEOF in a safe way, we send a transaction performing
     * the following tasks:
     * 1) Reconfigure the instance according to the specified host/port params.
     * 2) Rewrite the configuration.
     * 3) Disconnect all clients (but this one sending the command) in order
     *    to trigger the ask-master-on-reconnection protocol for connected
     *    clients.
     *
     * Note that we don't check the replies returned by commands, since we
     * will observe instead the effects in the next INFO output. */
    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s",
                               sentinelInstanceMapCommand(ri, "MULTI"));
    if (retval == C_ERR)
        return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s %s %s",
                               sentinelInstanceMapCommand(ri, "SLAVEOF"),
                               host, portstr);
    if (retval == C_ERR)
        return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s REWRITE",
                               sentinelInstanceMapCommand(ri, "CONFIG"));
    if (retval == C_ERR)
        return retval;
    ri->link->pending_commands++;

    /* CLIENT KILL TYPE <type> is only supported starting from Redis 2.8.12,
     * however sending it to an instance not understanding this command is not
     * an issue because CLIENT is variadic command, so Redis will not
     * recognized as a syntax error, and the transaction will not fail (but
     * only the unsupported command will fail). */
    for (int type = 0; type < 2; type++)
    {
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelDiscardReplyCallback, ri, "%s KILL TYPE %s",
                                   sentinelInstanceMapCommand(ri, "CLIENT"),
                                   type == 0 ? "normal" : "pubsub");
        if (retval == C_ERR)
            return retval;
        ri->link->pending_commands++;
    }

    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s",
                               sentinelInstanceMapCommand(ri, "EXEC"));
    if (retval == C_ERR)
        return retval;
    ri->link->pending_commands++;

    return C_OK;
}

/* Setup the master state to start a failover. */
// 开始进行故障转移
void sentinelStartFailover(sentinelRedisInstance *master)
{
    serverAssert(master->flags & SRI_MASTER);

    master->failover_state = SENTINEL_FAILOVER_STATE_WAIT_START;
    master->flags |= SRI_FAILOVER_IN_PROGRESS;
    master->failover_epoch = ++sentinel.current_epoch;
    sentinelEvent(LL_WARNING, "+new-epoch", master, "%llu",
                  (unsigned long long)sentinel.current_epoch);
    sentinelEvent(LL_WARNING, "+try-failover", master, "%@");
    master->failover_start_time = mstime() + rand() % SENTINEL_MAX_DESYNC;
    master->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 1) Master must be in ODOWN condition.
 * 2) No failover already in progress.
 * 3) No failover already attempted recently.
 *
 * We still don't know if we'll win the election so it is possible that we
 * start the failover but that we'll not be able to act.
 *
 * Return non-zero if a failover was started. */
int sentinelStartFailoverIfNeeded(sentinelRedisInstance *master)
{
    /* We can't failover if the master is not in O_DOWN state. */
    if (!(master->flags & SRI_O_DOWN))
        return 0;

    /* Failover already in progress? */
    if (master->flags & SRI_FAILOVER_IN_PROGRESS)
        return 0;

    /* Last failover attempt started too little time ago? */
    if (mstime() - master->failover_start_time <
        master->failover_timeout * 2)
    {
        if (master->failover_delay_logged != master->failover_start_time)
        {
            time_t clock = (master->failover_start_time +
                            master->failover_timeout * 2) /
                           1000;
            char ctimebuf[26];

            ctime_r(&clock, ctimebuf);
            ctimebuf[24] = '\0'; /* Remove newline. */
            master->failover_delay_logged = master->failover_start_time;
            serverLog(LL_WARNING,
                      "Next failover delay: I will not start a failover before %s",
                      ctimebuf);
        }
        return 0;
    }

    sentinelStartFailover(master);
    return 1;
}

/* Select a suitable slave to promote. The current algorithm only uses
 * the following parameters:
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 * 2) Last time the slave replied to ping no more than 5 times the PING period.
 * 3) info_refresh not older than 3 times the INFO refresh period.
 * 4) master_link_down_time no more than:
 *     (now - master->s_down_since_time) + (master->down_after_period * 10).
 *    Basically since the master is down from our POV, the slave reports
 *    to be disconnected no more than 10 times the configured down-after-period.
 *    This is pretty much black magic but the idea is, the master was not
 *    available so the slave may be lagging, but not over a certain time.
 *    Anyway we'll select the best slave according to replication offset.
 * 5) Slave priority can't be zero, otherwise the slave is discarded.
 *
 * Among all the slaves matching the above conditions we select the slave
 * with, in order of sorting key:
 *
 * - lower slave_priority.
 * - bigger processed replication offset.
 * - lexicographically smaller runid.
 *
 * Basically if runid is the same, the slave that processed more commands
 * from the master is selected.
 *
 * The function returns the pointer to the selected slave, otherwise
 * NULL if no suitable slave was found.
 */

/* Helper for sentinelSelectSlave(). This is used by qsort() in order to
 * sort suitable slaves in a "better first" order, to take the first of
 * the list.
 *
 * 从节点排序接口:
 * 1. 选择优先级低的节点，如果相同，则继续
 * 2. 选择复制偏移量最大的节点，如果相同，则继续
 * 3. 选择 runid 字典序小的节点
 *
 * 如果选择失败，会在下一次事件循环中重试
 */
int compareSlavesForPromotion(const void *a, const void *b)
{
    sentinelRedisInstance **sa = (sentinelRedisInstance **)a,
                          **sb = (sentinelRedisInstance **)b;
    char *sa_runid, *sb_runid;

    // 比较优先级，选择优先级小的
    if ((*sa)->slave_priority != (*sb)->slave_priority)
        return (*sa)->slave_priority - (*sb)->slave_priority;

    /* If priority is the same, select the slave with greater replication
     * offset (processed more data from the master).
     * 比较复制偏移量，选择复制偏移量大的，说明数据比较全
     */
    if ((*sa)->slave_repl_offset > (*sb)->slave_repl_offset)
    {
        return -1; /* a < b */
    }
    else if ((*sa)->slave_repl_offset < (*sb)->slave_repl_offset)
    {
        return 1; /* a > b */
    }

    /* If the replication offset is the same select the slave with that has
     * the lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid. */
    // 对比节点的 runid
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL)
        return 0;
    else if (sa_runid == NULL)
        return 1; /* a > b */
    else if (sb_runid == NULL)
        return -1; /* a < b */

    return strcasecmp(sa_runid, sb_runid);      // 选择 runid 字典序小的
}

// 从主节点的所有从节点中，选择一个可以提升为主节点的从节点
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master)
{
    sentinelRedisInstance **instance = zmalloc(sizeof(instance[0]) * dictSize(master->slaves));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t max_master_down_time = 0;

    // 如果主节点处于主观下线状态, 则计算下线的时长
    if (master->flags & SRI_S_DOWN)
        max_master_down_time += mstime() - master->s_down_since_time;
    max_master_down_time += master->down_after_period * 10;

    di = dictGetIterator(master->slaves);
    while ((de = dictNext(di)) != NULL)     // 遍历主节点的所有从节点
    {
        sentinelRedisInstance *slave = dictGetVal(de);
        mstime_t info_validity_time;

        // 跳过主观下线的从节点
        if (slave->flags & (SRI_S_DOWN | SRI_O_DOWN))
            continue;

        // 跳过断线的从节点
        if (slave->link->disconnected)
            continue;

        // 跳过 PING 命令响应时间太久的从节点
        if (mstime() - slave->link->last_avail_time > SENTINEL_PING_PERIOD * 5)
            continue;

        // 跳过优先级为 0 的从节点
        if (slave->slave_priority == 0)
            continue;

        /* If the master is in SDOWN state we get INFO for slaves every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay. */
        if (master->flags & SRI_S_DOWN)
            info_validity_time = SENTINEL_PING_PERIOD * 5;
        else
            info_validity_time = SENTINEL_INFO_PERIOD * 3;

        // 跳过最近 5s 内没有回复过 sentinel INFO 命令的从节点
        if (mstime() - slave->info_refresh > info_validity_time)
            continue;

        // 跳过与主节点断开连接超过 10 * down_after_period 时间的从节点
        if (slave->master_link_down_time > max_master_down_time)
            continue;

        // 经过上面的筛选，剩下的都是数比较新，与 sentinel leader 通信正常的节点，可以保证故障转移后最小的数据丢失

        instance[instances++] = slave;      // 将选出来的节点保存到 isntance 中
    }

    dictReleaseIterator(di);

    if (instances)
    {
        // 将经过上面步骤过滤出来的从节点按照 compareSlavesForPromotion 进行排序，然后选择第一个节点
        qsort(instance, instances, sizeof(sentinelRedisInstance *), compareSlavesForPromotion);
        selected = instance[0];
    }

    zfree(instance);

    return selected;
}

/* ---------------- Failover state machine implementation ------------------- */
// 故障转移开始
void sentinelFailoverWaitStart(sentinelRedisInstance *ri)
{
    char *leader;
    int isleader;

    /* Check if we are the leader for the failover epoch. */
    leader = sentinelGetLeader(ri, ri->failover_epoch);           // 选举 sentinel leader
    isleader = leader && strcasecmp(leader, sentinel.myid) == 0;  // 当前 sentinel 是否是 leader
    sdsfree(leader);

    /* If I'm not the leader, and it is not a forced failover via
     * SENTINEL FAILOVER, then I can't continue with the failover.
     * 如果当前 sentinel 不是领导者，而且也不是强制故障转移，那么经过简单判断就会返回，故障转移操作由其他 sentinel 执行*/
    if (!isleader && !(ri->flags & SRI_FORCE_FAILOVER))
    {
        int election_timeout = SENTINEL_ELECTION_TIMEOUT;

        /* The election timeout is the MIN between SENTINEL_ELECTION_TIMEOUT
         * and the configured failover timeout.
         * 将选举超时时间设置为故障转移的超时时间 */
        if (election_timeout > ri->failover_timeout)
            election_timeout = ri->failover_timeout;
        /* Abort the failover if I'm not the leader after some time. */
        // 选举超时，则取消故障转移
        if (mstime() - ri->failover_start_time > election_timeout)
        {
            sentinelEvent(LL_WARNING, "-failover-abort-not-elected", ri, "%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    // 如果当前 sentinel 为 leader, 则发送赢取选举的事件通知
    sentinelEvent(LL_WARNING, "+elected-leader", ri, "%@");
    if (sentinel.simfailure_flags & SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION)       // 如果指定了故障模拟, 则退出当前程序
        sentinelSimFailureCrash();
    ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;  // 修改故障转移状态
    ri->failover_state_change_time = mstime();                  // 更新故障转移变更时间
    sentinelEvent(LL_WARNING, "+failover-state-select-slave", ri, "%@");        // 发送事件通知
}

// 在主节点的所有从节点中，选择一个可以提升成主节点的从节点
void sentinelFailoverSelectSlave(sentinelRedisInstance *ri)
{
    sentinelRedisInstance *slave = sentinelSelectSlave(ri);     // 选择一个从节点，后面步骤会将该从节点提升成主节点

    /* We don't handle the timeout in this state as the function aborts
     * the failover or go forward in the next state. */
    if (slave == NULL)
    {
        // 没有找到合适的从节点则停止故障转移
        sentinelEvent(LL_WARNING, "-failover-abort-no-good-slave", ri, "%@");
        sentinelAbortFailover(ri);
    }
    else
    {
        sentinelEvent(LL_WARNING, "+selected-slave", slave, "%@");      // 发送选择从节点成功的事件
        slave->flags |= SRI_PROMOTED;
        ri->promoted_slave = slave;
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE;
        ri->failover_state_change_time = mstime();
        sentinelEvent(LL_NOTICE, "+failover-state-send-slaveof-noone", slave, "%@");
    }
}

/**
 * 发送 SLAVEOF no one 命令，将上一步选择的从节点提升成主节点
 *
 * 从节点接收到 SLAVEOF no one 命令之后，会重置主节点信息，断开与主节点/从节点的网络连接，重置复制 ID, 并执行持久化重写操作。
 *
 * sentinel leader 向该从节点放 SLAVEOF no one 命令之后，每隔 1s 发送一次 INFO 命令(正常是 10s 一次)，并观察命令回复中的角色信息，当被升级
 * 的从节点从原来的 slave 变成 master 时，sentinel leader 就知道该从节点已经升级为主节点了。详细过程在 sentinelRefreshInstanceInfo 中
 */
void sentinelFailoverSendSlaveOfNoOne(sentinelRedisInstance *ri)
{
    int retval;

    /* We can't send the command to the promoted slave if it is now
     * disconnected. Retry again and again with this state until the timeout
     * is reached, then abort the failover.
     * 如果该准备提升为主节点的从节点处理离线状态，就不能发送命令了. 但是会在下一次事件循环中进行重试 */
    if (ri->promoted_slave->link->disconnected)
    {
        // 如果故障转移超时，则取消故障转移，然后退出
        if (mstime() - ri->failover_state_change_time > ri->failover_timeout)
        {
            sentinelEvent(LL_WARNING, "-failover-abort-slave-timeout", ri, "%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    /* Send SLAVEOF NO ONE command to turn the slave into a master.
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (master instead of slave).
     *
     * 发送 SLAVEOF no one 命令给该从节点，让其提升为主节点 */
    retval = sentinelSendSlaveOf(ri->promoted_slave, NULL, 0);
    if (retval != C_OK)
        return;
    sentinelEvent(LL_NOTICE, "+failover-state-wait-promotion", ri->promoted_slave, "%@");
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION;
    ri->failover_state_change_time = mstime();
}

/* We actually wait for promotion indirectly checking with INFO when the
 * slave turns into a master.
 * 等待选择的从节点提升成主节点，如果超时选择新的从节点 */
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri)
{
    /* Just handle the timeout. Switching to the next state is handled
     * by the function parsing the INFO command of the promoted slave. */
    if (mstime() - ri->failover_state_change_time > ri->failover_timeout)
    {
        sentinelEvent(LL_WARNING, "-failover-abort-slave-timeout", ri, "%@");
        sentinelAbortFailover(ri);
    }
}

void sentinelFailoverDetectEnd(sentinelRedisInstance *master)
{
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t elapsed = mstime() - master->failover_state_change_time;

    /* We can't consider failover finished if the promoted slave is
     * not reachable. */
    if (master->promoted_slave == NULL ||
        master->promoted_slave->flags & SRI_S_DOWN)
        return;

    /* The failover terminates once all the reachable slaves are properly
     * configured. */
    di = dictGetIterator(master->slaves);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (slave->flags & (SRI_PROMOTED | SRI_RECONF_DONE))
            continue;
        if (slave->flags & SRI_S_DOWN)
            continue;
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    /* Force end of failover on timeout. */
    if (elapsed > master->failover_timeout)
    {
        not_reconfigured = 0;
        timeout = 1;
        sentinelEvent(LL_WARNING, "+failover-end-for-timeout", master, "%@");
    }

    if (not_reconfigured == 0)
    {
        sentinelEvent(LL_WARNING, "+failover-end", master, "%@");
        master->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        master->failover_state_change_time = mstime();
    }

    /* If I'm the leader it is a good idea to send a best effort SLAVEOF
     * command to all the slaves still not reconfigured to replicate with
     * the new master. */
    if (timeout)
    {
        dictIterator *di;
        dictEntry *de;

        di = dictGetIterator(master->slaves);
        while ((de = dictNext(di)) != NULL)
        {
            sentinelRedisInstance *slave = dictGetVal(de);
            int retval;

            if (slave->flags & (SRI_PROMOTED | SRI_RECONF_DONE | SRI_RECONF_SENT))
                continue;
            if (slave->link->disconnected)
                continue;

            retval = sentinelSendSlaveOf(slave,
                                         master->promoted_slave->addr->ip,
                                         master->promoted_slave->addr->port);
            if (retval == C_OK)
            {
                sentinelEvent(LL_NOTICE, "+slave-reconf-sent-be", slave, "%@");
                slave->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send SLAVE OF <new master address> to all the remaining slaves that
 * still don't appear to have the configuration updated.
 *
 * 给所有的从节点发送 SLAVEOF 命令，让它们向新的主节点进行同步数据
 */
void sentinelFailoverReconfNextSlave(sentinelRedisInstance *master)
{
    dictIterator *di;
    dictEntry *de;
    int in_progress = 0;

    // 遍历所有的从节点, 计算已经发送同步命令或者正在执行同步的从节点数量
    di = dictGetIterator(master->slaves);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (slave->flags & (SRI_RECONF_SENT | SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    // 如果已经发送同步命令或者正在执行同步操作的从节点的个数小于设置的同步个数限制
    di = dictGetIterator(master->slaves);
    while (in_progress < master->parallel_syncs && (de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);
        int retval;

        /* Skip the promoted slave, and already configured slaves. */
        // 跳过被提升成主节点的从节点和已经完成同步的从节点
        if (slave->flags & (SRI_PROMOTED | SRI_RECONF_DONE))
            continue;

        /* If too much time elapsed without the slave moving forward to
         * the next state, consider it reconfigured even if it is not.
         * Sentinels will detect the slave as misconfigured and fix its
         * configuration later.
         * 如果已经向从节点发送了 slaveof 命令，但是故障转移在下一状态的时候超时了
         */
        if ((slave->flags & SRI_RECONF_SENT) &&
            (mstime() - slave->slave_reconf_sent_time) >
                SENTINEL_SLAVE_RECONF_TIMEOUT)
        {
            sentinelEvent(LL_NOTICE, "-slave-reconf-sent-timeout", slave, "%@");
            slave->flags &= ~SRI_RECONF_SENT;
            slave->flags |= SRI_RECONF_DONE;
        }

        /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state.
         * 跳过已经发送过同步命令和正在同步的从节点
         */
        if (slave->flags & (SRI_RECONF_SENT | SRI_RECONF_INPROG))
            continue;
        if (slave->link->disconnected)      // 如果连接断开，则跳过
            continue;

        /* Send SLAVEOF <new master>. */
        // 向所有从节点发送 SLAVEOF 命令
        retval = sentinelSendSlaveOf(slave,
                                     master->promoted_slave->addr->ip,
                                     master->promoted_slave->addr->port);
        if (retval == C_OK)
        {
            slave->flags |= SRI_RECONF_SENT;
            slave->slave_reconf_sent_time = mstime();
            sentinelEvent(LL_NOTICE, "+slave-reconf-sent", slave, "%@");
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    /* Check if all the slaves are reconfigured and handle timeout. */
    sentinelFailoverDetectEnd(master);      // 判断故障转移是否结束
}

/* This function is called when the slave is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the master table and add the promoted slave instead. */
void sentinelFailoverSwitchToPromotedSlave(sentinelRedisInstance *master)
{
    sentinelRedisInstance *ref = master->promoted_slave ? master->promoted_slave : master;

    sentinelEvent(LL_WARNING, "+switch-master", master, "%s %s %d %s %d",
                  master->name, master->addr->ip, master->addr->port,
                  ref->addr->ip, ref->addr->port);

    sentinelResetMasterAndChangeAddress(master, ref->addr->ip, ref->addr->port);
}

// 执行故障转移操作
void sentinelFailoverStateMachine(sentinelRedisInstance *ri)
{
    serverAssert(ri->flags & SRI_MASTER);       // 故障转移必须是主节点

    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS))        // 如果不是故障转移状态，则直接返回
        return;

    // 故障转移的状态是分步骤的，当前时间事件中处理执行完一个步骤之后，在下一个时间事件中执行下一个步骤

    switch (ri->failover_state)
    {
    case SENTINEL_FAILOVER_STATE_WAIT_START:
        sentinelFailoverWaitStart(ri);          // 故障转移开始: 选举 sentinel leader, 主导故障转移的过程
        break;
    case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
        sentinelFailoverSelectSlave(ri);        // 在所有的从节点中选举出一个节点，准备将该节点提升为主节点
        break;
    case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
        sentinelFailoverSendSlaveOfNoOne(ri);   // 发送 SLAVEOF no one 命令，将上一步选择的从节点提升成主节点
        break;
    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
        sentinelFailoverWaitPromotion(ri);      // 等待选择的从节点提升成主节点，如果超时选择新的从节点
        break;
    case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
        sentinelFailoverReconfNextSlave(ri);    // 给所有的从节点发送 SLAVEOF 命令，让它们向新的主节点进行同步数据
        break;
    }
}

/* Abort a failover in progress:
 *
 * This function can only be called before the promoted slave acknowledged
 * the slave -> master switch. Otherwise the failover can't be aborted and
 * will reach its end (possibly by timeout). */
void sentinelAbortFailover(sentinelRedisInstance *ri)
{
    serverAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);
    serverAssert(ri->failover_state <= SENTINEL_FAILOVER_STATE_WAIT_PROMOTION);

    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS | SRI_FORCE_FAILOVER);
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();
    if (ri->promoted_slave)
    {
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        ri->promoted_slave = NULL;
    }
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * -------------------------------------------------------------------------- */

/* Perform scheduled operations for the specified Redis instance. */
// 对节点 ri 执行定期操作
void sentinelHandleRedisInstance(sentinelRedisInstance *ri)
{
    /* ========== MONITORING HALF 监控操作 ============ */
    /* Every kind of instance */
    sentinelReconnectInstance(ri);      // 和节点 ri 创建网络连接
    sentinelSendPeriodicCommands(ri);   // 向节点 ri 发送 PING/INFO/PUBLISH 命令

    /* ============== ACTING HALF 故障检测 ============= */
    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock.
     * 如果处于 TILT 模式，不进行故障检测 */
    if (sentinel.tilt)
    {
        if (mstime() - sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) // 如果 TILT 模式的时间还没到（默认 1000*30毫秒），则直接返回
            return;
        sentinel.tilt = 0;      // 超过时间，则退出 TILT 模式
        sentinelEvent(LL_WARNING, "-tilt", NULL, "#tilt mode exited");
    }

    /* Every kind of instance */
    sentinelCheckSubjectivelyDown(ri);      // 故障检测，判断节点 ri 是否处于主观下线状态

    /* Masters and slaves */
    if (ri->flags & (SRI_MASTER | SRI_SLAVE))
    {
        /* Nothing so far. */
    }

    /* Only masters */
    if (ri->flags & SRI_MASTER)
    {
        sentinelCheckObjectivelyDown(ri);       // 检查是否客观下线
        if (sentinelStartFailoverIfNeeded(ri))  // 如果处于客观下线，则进一步进行故障转移状态判断并设置状态转移的一些状态
        {
            /* 向其他 sentinel 节点发送 SENTINEL is-master-down-by-addr 确认是否不可达，
             * 如果能从其他 sentinel 节点足够多的票数，则标记为客观下线，触发故障转移 */
            sentinelAskMasterStateToOtherSentinels(ri, SENTINEL_ASK_FORCED);
        }

        sentinelFailoverStateMachine(ri);       // 执行故障转移操作

        // 主节点没有处于客观下线的状态，那么也要尝试发送 SENTINEL is-master-down-by-addr 给所有的 sentinel 获取回复，
        // 因为主节点如果有回复延迟等等状况，可以通过该命令，更新一些主节点状态
        sentinelAskMasterStateToOtherSentinels(ri, SENTINEL_NO_FLAGS);
    }
}

/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of slaves.
 *
 * 执行定期操作:
 * 1. 尝试建立异步网络连接, 然后发送 PING/INFO/PUBLISH 命令
 *    1.1 sentinel 向 master/slave/sentinel 节点发送 PING 命令检测在线状态
 *    1.2 sentinel 向 master/slave 节点发送 INFO 命令, 获取 master/slave 下属的 slave 节点信息
 *    1.3 sentinel 通过频道 __sentinel__:hello 发布自己的信息以及 master 节点的信息, 这样订阅了该频道的节点就可以检测到其他 sentinel 节点
 * 2. 进行故障转移操作
 * 3. 接收其他节点发送过来的 PING 和 sentinel 信息
 *
 * sentinel 之间只通过 PING 命令检测对方的在线状态，那么它们是如何相互发现并交互的:
 *
 * 比如有以下节点关系
 *
 *          sentinelA <---> master <---> sentinelB
 *
 * sentinelA 在监控 master, sentinelB 也在监控 master, 同时这三个节点都订阅了 __sentinel__:hello 频道。
 *
 * sentinelA 向 master 通过 __sentinel__:hello 频道发布 hello 消息, master 接收到 hello 消息之后，
 * 会将该 hello 消息通过 __sentinel__:hello 频道再次发布出去，由于 sentinelB 同样订阅了该频道，所以 sentinelB 可以从
 * 该频道上接收到 master 发布的 hello 消息，然后 sentinelB 就会知道 sentinelA 节点的存在。反之也成立
 */
void sentinelHandleDictOfRedisInstances(dict *instances)
{
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* There are a number of things we need to perform against every master. */
    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *ri = dictGetVal(de);

        sentinelHandleRedisInstance(ri);    // 对节点 ri 执行一系列的调度操作
        if (ri->flags & SRI_MASTER) // 如果是主节点, 需要递归遍历每一个 slave 节点以及所有的 sentinel 节点
        {
            sentinelHandleDictOfRedisInstances(ri->slaves);
            sentinelHandleDictOfRedisInstances(ri->sentinels);
            // 如果已经完成了故障转移, 并且所有的 slave 已经和新的 master 完成了同步，设置主从转换标识
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG)
            {
                switch_to_promoted = ri;
            }
        }
    }

    // 将以下线的 master 从列表中删除，然后用选举出的新的 master 代替
    if (switch_to_promoted)
        sentinelFailoverSwitchToPromotedSlave(switch_to_promoted);
    dictReleaseIterator(di);
}

/* This function checks if we need to enter the TITL mode.
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 1) The Sentinel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 * 2) The system clock was altered significantly.
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * During TILT time we still collect information, we just do not act.
 *
 * 判断是否需要进入 TITL 模式
 *     TILT 模式实际上是 sentinel 特殊保护性标识. 哨兵模式的运行，实际上非常依赖与系统时间，但是当系统时间被调整，或者哨兵中的流程因为某种
 * 原因(比如负载较高,IO阻塞,进程信号被停止等)而被阻塞时,哨兵的行为就变得不可预知了。于是就有了 TILT 模式，进入 TILT 模式之后，哨兵只定期
 * 发送命令用于收集信息，而不采取实质性的动作(比如不会进行故障转移)，当恢复正常 30S 后，自动退出 TILT 模式
 */
void sentinelCheckTiltCondition(void)
{
    mstime_t now = mstime();
    mstime_t delta = now - sentinel.previous_time;  // 计算上次调度的时间和当前时间的差值

    // 差值小于 0, 说明时钟可能出现了问题; 差值大于 2, 可能是当前进程被阻塞或者系统负载太高
    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER)
    {
        sentinel.tilt = 1;
        sentinel.tilt_start_time = mstime();
        sentinelEvent(LL_WARNING, "+tilt", NULL, "#tilt mode entered");
    }

    sentinel.previous_time = mstime();
}

// 哨兵模式的主要逻辑, 在 serverCron 中每 100ms 执行一次调用
void sentinelTimer(void)
{
    sentinelCheckTiltCondition();       // 判断是否进入 TITL 模式
    sentinelHandleDictOfRedisInstances(sentinel.masters);    // 建立网络连接, 发送 PING/INFO/PUB/SUB 等命令, 进行故障转移等
    sentinelRunPendingScripts();
    sentinelCollectTerminatedScripts();
    sentinelKillTimedoutScripts();

    /* We continuously change the frequency of the Redis "timer interrupt"
     * in order to desynchronize every Sentinel from every other.
     * This non-determinism avoids that Sentinels started at the same time
     * exactly continue to stay synchronized asking to be voted at the
     * same time again and again (resulting in nobody likely winning the
     * election because of split brain voting).
     *
     * 不断改变定期任务的执行频率，以便使每个 sentinel 节点都不同步，这种不确定性可以避免 sentinel 在同一时间开始完全继续保持同步，
     * 当被要求进行投票时，一次又一次在同一时间进行投票，因为脑裂导致有可能没有胜选者
     */
    server.hz = CONFIG_DEFAULT_HZ + rand() % CONFIG_DEFAULT_HZ;
}
