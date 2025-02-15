/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __AE_H__
#define __AE_H__

#include <time.h>

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0     /* No events registered. */
#define AE_READABLE 1 /* Fire when descriptor is readable. */
#define AE_WRITABLE 2 /* Fire when descriptor is writable. */
#define AE_BARRIER 4  /* With WRITABLE, never fire the event if the      \
                         READABLE event already fired in the same event  \
                         loop iteration. Useful when you want to persist \
                         things to disk before sending replies, and want \
                         to do that in a group fashion. */

// redis 的事件驱动模型主要关注两种事件: 文件事件和时间事件
#define AE_FILE_EVENTS (1 << 0) // 文件事件
#define AE_TIME_EVENTS (1 << 1) // 时间事件
#define AE_ALL_EVENTS (AE_FILE_EVENTS | AE_TIME_EVENTS)
#define AE_DONT_WAIT (1 << 2)
#define AE_CALL_BEFORE_SLEEP (1 << 3)
#define AE_CALL_AFTER_SLEEP (1 << 4)

#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void)V)

struct aeEventLoop;

/* Types and data structures */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

// File event structure. 文件事件结构体
typedef struct aeFileEvent
{
    int mask; /* one of AE_(READABLE|WRITABLE|BARRIER) */
    aeFileProc *rfileProc;
    aeFileProc *wfileProc;
    void *clientData;
} aeFileEvent;

// Time event structure. 时间事件结构体
typedef struct aeTimeEvent
{
    long long id; /* time event identifier. */ // 时间事件 ID, 用来标识事件
    long when_sec; /* seconds */               // 时间事件执行的秒数
    long when_ms; /* milliseconds */           // 时间事件执行的毫秒数
    aeTimeProc *timeProc;                      // 时间事件到期时的回调函数
    aeEventFinalizerProc *finalizerProc;       // 事件最终被删除时的回调函数
    void *clientData;                          // timeProc 的参数

    // prev 和 next 两个指针将 timeEvent 在 eventloop 上连接成双向链表
    struct aeTimeEvent *prev;
    struct aeTimeEvent *next;

    int refcount; /* refcount to prevent timer events from being
  		   * freed in recursive time event calls. */
} aeTimeEvent;

/* A fired event */
typedef struct aeFiredEvent
{
    int fd;   // 触发事件的文件描述符
    int mask; // 触发事件的掩码
} aeFiredEvent;

/* State of an event based program */
typedef struct aeEventLoop
{
    int maxfd;   /* highest file descriptor currently registered */
    int setsize; // max number of file descriptors tracked. 事件循环中监听的文件描述符的数量.
    long long timeEventNextId;      // eventloop 上所有时间事件的全局唯一 id, 该 id 按照从小到大的顺序进行递增
    time_t lastTime;     // Used to detect system clock skew. 上次处理事件的时间戳, 用于检测时钟偏差
    aeFileEvent *events; // Registered events. 监听的事件. 数组下标是文件描述符 fd, 数组元素是内核监听的该 fd 的事件. 在 aeCreateEventLoop 中分配内存
    aeFiredEvent *fired; // Fired events.   已经触发的事件
    aeTimeEvent *timeEventHead;     // eventloop 上所有时间事件组成的链表
    int stop;
    void *apidata;  // This is used for polling API specific data. 指向的是 aeApiState 的地址, 用于在 epoll_wait 中获取已经触发的事件
    aeBeforeSleepProc *beforesleep; // 每次进入事件循环时调用函数的函数指针
    aeBeforeSleepProc *aftersleep;  // 每次退出事件循环时调用函数的函数指针
    int flags;
} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                      aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);
void aeSetDontWait(aeEventLoop *eventLoop, int noWait);

#endif
