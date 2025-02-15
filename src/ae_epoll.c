/* Linux epoll(2) based ae.c module
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

#include <sys/epoll.h>

typedef struct aeApiState
{
    int epfd;                   // 保存 epoll-fd
    struct epoll_event *events; // 在 aeApiCreate 中分配内存，用于在 epoll_wait 中获取已经触发的事件, epoll_event.data.fd 中保存了触发事件的文件描述符
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop)
{
    // 如果将 aeApiState 修改为柔性数组，则可以使用下面的内存分配方式:
    // aeApiState *state = (aeApiState *)zmalloc(sizeof(aeApiState) + sizeof(epoll_event) * eventLoop->setsize);
    // if (!state)
    //     return -1;
    // state->events =  (struct epoll_event *)(state + sizeof(aeApiState));

    aeApiState *state = zmalloc(sizeof(aeApiState));
    if (!state)
        return -1;

    // FIXME: 分配了固定大小的 events, 如果监听的文件描述符数量超过 eventLoop->setsize 大小, 则需要调用 aeApiResize 接口重新分配
    // zeroMQ 中的处理方式和这种方式不同
    state->events = zmalloc(sizeof(struct epoll_event) * eventLoop->setsize);
    if (!state->events)
    {
        zfree(state);
        return -1;
    }

    state->epfd = epoll_create(1024); /* 1024 is just a hint for the kernel */
    if (state->epfd == -1)
    {
        zfree(state->events);
        zfree(state);
        return -1;
    }

    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(aeEventLoop *eventLoop, int setsize)
{
    aeApiState *state = eventLoop->apidata;

    state->events = zrealloc(state->events, sizeof(struct epoll_event) * setsize);
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop)
{
    aeApiState *state = eventLoop->apidata;

    close(state->epfd);
    zfree(state->events);
    zfree(state);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0}; /* avoid valgrind warning */
    /* If the fd was already monitored for some event, we need a MOD
     * operation. Otherwise we need an ADD operation. */
    int op = eventLoop->events[fd].mask == AE_NONE ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;

    ee.events = 0;
    mask |= eventLoop->events[fd].mask; /* Merge old events */
    if (mask & AE_READABLE)
        ee.events |= EPOLLIN;
    if (mask & AE_WRITABLE)
        ee.events |= EPOLLOUT;
    ee.data.fd = fd;
    if (epoll_ctl(state->epfd, op, fd, &ee) == -1)
        return -1;
    return 0;
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask)
{
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0}; /* avoid valgrind warning */
    int mask = eventLoop->events[fd].mask & (~delmask);

    ee.events = 0;
    if (mask & AE_READABLE)
        ee.events |= EPOLLIN;
    if (mask & AE_WRITABLE)
        ee.events |= EPOLLOUT;
    ee.data.fd = fd;
    if (mask != AE_NONE)
    {
        epoll_ctl(state->epfd, EPOLL_CTL_MOD, fd, &ee);
    }
    else
    {
        /* Note, Kernel < 2.6.9 requires a non null event pointer even for
         * EPOLL_CTL_DEL. */
        epoll_ctl(state->epfd, EPOLL_CTL_DEL, fd, &ee);
    }
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp)
{
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    retval = epoll_wait(state->epfd, state->events, eventLoop->setsize,
                        tvp ? (tvp->tv_sec * 1000 + tvp->tv_usec / 1000) : -1);
    if (retval > 0)
    {
        int j;

        numevents = retval;
        for (j = 0; j < numevents; j++)
        {
            int mask = 0;
            struct epoll_event *e = state->events + j;

            if (e->events & EPOLLIN)
                mask |= AE_READABLE;
            if (e->events & EPOLLOUT)
                mask |= AE_WRITABLE;
            if (e->events & EPOLLERR)
                mask |= AE_WRITABLE | AE_READABLE;
            if (e->events & EPOLLHUP)
                mask |= AE_WRITABLE | AE_READABLE;

            eventLoop->fired[j].fd = e->data.fd; // 将触发的事件的描述符放到 fired 中, 由外部对这些触发事件进行处理
            eventLoop->fired[j].mask = mask;
        }
    }
    return numevents;
}

static char *aeApiName(void)
{
    return "epoll";
}
