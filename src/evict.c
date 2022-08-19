/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "bio.h"
#include "atomicvar.h"

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across freeMemoryIfNeeded() calls.
 *
 * Entries inside the eviction pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictionPoolEntry
{
    unsigned long long idle; /* Object idle time (inverse frequency for LFU) */
    sds key;                 /* Key name. */
    sds cached;              /* Cached SDS object for key name. */
    int dbid;                /* Key DB number. */
};

static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures.
 *
 * 调用 mtime() 获取 LRU 时间戳。mtime() 这个系统调用需要在内核态和用户态之间进行切换，所以如果频繁调用，
 * 会造成性能严重下降，所以 redis 在服务器的全局变量中使用了 server.lruclock 来缓存当前的 lru。
 * redis 会在系统事件循环的心跳 serverCron 调用该接口缓存 LRU
 */
unsigned int getLRUClock(void)
{
    return (mstime() / LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call.
 *
 * server.lruclock 是在系统的心跳 serverCron 中更新的.
 * 1. 正常情况下，LRU 跳数的频率 LRU_CLOCK_RESOLUTION 为 1000ms/1次，而系统心跳则是 1000ms/10次，系统心跳的频率远高于 LRU 频率, 所有可以通过读取
 * server.lruclock 来获取当前的 LRU 时间戳
 * 2. 如果系统心跳频率低于 LRU 频率时，redisServer.lruclock 缓存无法及时更新，对于这种情况，通过 getLRUClock 函数来获取时间戳跳数。
 */
unsigned int LRU_CLOCK(void)
{
    unsigned int lruclock;
    if (1000 / server.hz <= LRU_CLOCK_RESOLUTION)
    {
        lruclock = server.lruclock;
    }
    else
    {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
// 计算一个 redis 对象没有被访问的空闲时间 idle。LRU 淘汰策略需要根据该 idle 值选择 key 然后进行淘汰
// idle: 该数据从上次访问到现在的间隔时间, 单位 ms
unsigned long long estimateObjectIdleTime(robj *o)
{
    unsigned long long lruclock = LRU_CLOCK(); // 计算当前的 lru 时间戳
    if (lruclock >= o->lru)                    // 当前的 lru 时间戳大于 redis 对象的 lru 时间戳
    {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION; // 计算空闲 idle, 单位 ms
    }
    else
    {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) * LRU_CLOCK_RESOLUTION;
    }
}

/* freeMemoryIfNeeded() gets called when 'maxmemory' is set on the config
 * file to limit the max memory used by the server, before processing a
 * command.
 *
 * The goal of the function is to free enough memory to keep Redis under the
 * configured memory limit.
 *
 * The function starts calculating how many bytes should be freed to keep
 * Redis under the limit, and enters a loop selecting the best keys to
 * evict accordingly to the configured policy.
 *
 * If all the bytes needed to return back under the limit were freed the
 * function returns C_OK, otherwise C_ERR is returned, and the caller
 * should block the execution of commands that will result in more memory
 * used by the server.
 *
 * ------------------------------------------------------------------------
 *
 * LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */

/* Create a new eviction pool. */
// 创建一个淘汰池, 淘汰池实际上就是一个数组
void evictionPoolAlloc(void)
{
    struct evictionPoolEntry *ep;
    int j;

    ep = zmalloc(sizeof(*ep) * EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++)
    {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL, EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }

    EvictionPoolLRU = ep;
}

/* This is an helper function for freeMemoryIfNeeded(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time smaller than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right.
 *
 * 从主字典或者过期字典中选择 key, 然后按照 idle 从小到大的顺序将 key 插入到淘汰池 pool 中
 *
 * 当 Redis 内存占用超过阀值后，按策略从主字典 dict 或者过期字典 expires 中随机选择 N 个 key，N 默认是 5，计算每个 key 的 idle 值，
 * 按 idle 值从小到大的顺序插入 evictionPool 中，然后选择 idle 最大的那个 key，进行淘汰。
 *
 * sampledict: 主字典或者过期字典
 * keydict: 主字典
 */
void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool)
{
    int j, k, count;
    dictEntry *samples[server.maxmemory_samples];

    // 从哈希表 sampledict 中随机获取 server.maxmemory_samples 个 key, 获取的 key 存储在 samples 指针数组中
    count = dictGetSomeKeys(sampledict, samples, server.maxmemory_samples);
    for (j = 0; j < count; j++) // 将指针数组中的 key 按照 idle 从小到大的顺序插入到淘汰池 pool 中
    {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL)
        {
            if (sampledict != keydict)
                de = dictFind(keydict, key);
            o = dictGetVal(de);
        }

        /* Calculate the idle time according to the policy. This is called
         * idle just because the code initially handled LRU, but is in fact
         * just a score where an higher score means better candidate. */
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU)
        {
            idle = estimateObjectIdleTime(o); // 如果使用 LRU 的策略, 计算空闲时间 idle
        }
        else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU)
        {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255. */
            idle = 255 - LFUDecrAndReturn(o);
        }
        else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            /* In this case the sooner the expire the better. */
            idle = ULLONG_MAX - (long)dictGetVal(de);
        }
        else
        {
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        // 淘汰池 pool 中 key 的 idle 是按照从小到大的顺序，所以在这里需要找到合适的位置将 idle 插入 pool
        k = 0;
        while (k < EVPOOL_SIZE && pool[k].key && pool[k].idle < idle)
            k++;

        if (k == 0 && pool[EVPOOL_SIZE - 1].key != NULL)
        {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        }
        else if (k < EVPOOL_SIZE && pool[k].key == NULL)
        {
            /* Inserting into empty position. No setup needed before insert. */
        }
        else
        {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[EVPOOL_SIZE - 1].key == NULL)
            {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                sds cached = pool[EVPOOL_SIZE - 1].cached;
                memmove(pool + k + 1, pool + k,
                        sizeof(pool[0]) * (EVPOOL_SIZE - k - 1));
                pool[k].cached = cached;
            }
            else
            {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached)
                    sdsfree(pool[0].key);
                memmove(pool, pool + 1, sizeof(pool[0]) * k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        int klen = sdslen(key);
        if (klen > EVPOOL_CACHED_SDS_SIZE)
        {
            pool[k].key = sdsdup(key);
        }
        else
        {
            memcpy(pool[k].cached, key, klen + 1);
            sdssetlen(pool[k].cached, klen);
            pool[k].key = pool[k].cached;
        }
        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.

 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
 * when incrementing the key, so that keys starting at COUNTER_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the COUNTER_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
// 获取 16 位的当前基于Unix时间戳的分钟时间戳
unsigned long LFUGetTimeInMinutes(void)
{
    return (server.unixtime / 60) & 65535;
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
// 计算对象上次访问的时间距今的空闲时间
unsigned long LFUTimeElapsed(unsigned long ldt)
{
    unsigned long now = LFUGetTimeInMinutes();
    if (now >= ldt)
        return now - ldt;
    return 65535 - ldt + now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really implemented. Saturate it at 255. */
uint8_t LFULogIncr(uint8_t counter)
{
    if (counter == 255)
        return 255;
    double r = (double)rand() / RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0)
        baseval = 0;
    double p = 1.0 / (baseval * server.lfu_log_factor + 1);
    if (r < p)
        counter++;
    return counter;
}

/* If the object decrement time is reached decrement the LFU counter but
 * do not update LFU fields of the object, we update the access time
 * and counter in an explicit way when the object is really accessed.
 * And we will times halve the counter according to the times of
 * elapsed time than server.lfu_decay_time.
 * Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed. */
unsigned long LFUDecrAndReturn(robj *o)
{
    unsigned long ldt = o->lru >> 8;
    unsigned long counter = o->lru & 255;
    unsigned long num_periods = server.lfu_decay_time ? LFUTimeElapsed(ldt) / server.lfu_decay_time : 0;
    if (num_periods)
        counter = (num_periods > counter) ? 0 : counter - num_periods;
    return counter;
}

/* ----------------------------------------------------------------------------
 * The external API for eviction: freeMemoryIfNeeded() is called by the
 * server when there is data to add in order to make space if needed.
 * --------------------------------------------------------------------------*/

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size. This function
 * returns the sum of AOF and slaves buffer.
 *
 * 统计 AOF 以及 salve 使用的缓存情况
 */
size_t freeMemoryGetNotCountedMemory(void)
{
    size_t overhead = 0;
    int slaves = listLength(server.slaves);

    if (slaves)
    {
        listIter li;
        listNode *ln;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li)))
        {
            client *slave = listNodeValue(ln);
            overhead += getClientOutputBufferMemoryUsage(slave);
        }
    }
    if (server.aof_state != AOF_OFF)
    {
        overhead += sdsalloc(server.aof_buf) + aofRewriteBufferSize();
    }
    return overhead;
}

/* Get the memory status from the point of view of the maxmemory directive:
 * if the memory used is under the maxmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 *
 * 判断当前 Redis 服务器的内存使用状态，进而判断是否需要启动淘汰机制，如果该函数返回 C_OK 表明当前内存使用量没有达到上限，无需进行淘汰；
 * 反之如果函数返回 C_ERR 则表明当前内存使用量已经达到上限，需要启动淘汰机制。在函数返回 C_ERR 时，通过函数传入的指针参数还可以获得当前 Redis 内存使用的具体数据：
 *
 *  1. total，通过该字段可以返回当前 Redis 使用的总内存数量
 *  2. logical，这个参数会返回 Redis 使用的逻辑内存，也就是 total 的内存减去 freeMemoryGetNotCountedMemory 之后的数量
 *  3. tofree，这个参数会返回 Redis 需要被释放的内存数量
 *  4. level，这个参数会返回当前内存的使用率，即 logical/redisServer.maxmemory 的结果
 */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level)
{
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    mem_reported = zmalloc_used_memory();
    if (total)
        *total = mem_reported;

    /* We may return ASAP if there is no need to compute the level. */
    int return_ok_asap = !server.maxmemory || mem_reported <= server.maxmemory;
    if (return_ok_asap && !level)
        return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    mem_used = mem_reported;
    size_t overhead = freeMemoryGetNotCountedMemory();  // 计算 AOF/Slave 使用的缓存
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;

    /* Compute the ratio of memory usage. */
    if (level)
    {
        if (!server.maxmemory)
        {
            *level = 0;
        }
        else
        {
            *level = (float)mem_used / (float)server.maxmemory;
        }
    }

    if (return_ok_asap)
        return C_OK;

    /* Check if we are still over the memory limit. */
    if (mem_used <= server.maxmemory)
        return C_OK;

    /* Compute how much memory we need to free. */
    mem_tofree = mem_used - server.maxmemory;

    if (logical)
        *logical = mem_used;
    if (tofree)
        *tofree = mem_tofree;

    return C_ERR;
}

/* This function is periodically called to see if there is memory to free
 * according to the current "maxmemory" settings. In case we are over the
 * memory limit, the function will try to free some memory to return back
 * under the limit.
 *
 * The function returns C_OK if we are under the memory limit or if we
 * were over the limit, but the attempt to free memory was successful.
 * Otherwise if we are over the memory limit, but not enough memory
 * was freed to return back under the limit, the function returns C_ERR.
 *
 * 淘汰机制的核心函数：
 *
 * Redis 在执行命令请求时，会检查当前内存占用是否超过 maxmemory 的数值，如果超过，则按照设置的淘汰策略，进行删除淘汰 key 操作。
 */
int freeMemoryIfNeeded(void)
{
    int keys_freed = 0;
    /* By default replicas should ignore maxmemory
     * and just be masters exact copies. */
    if (server.masterhost && server.repl_slave_ignore_maxmemory)
        return C_OK;

    size_t mem_reported, mem_tofree, mem_freed;
    mstime_t latency, eviction_latency, lazyfree_latency;
    long long delta;
    int slaves = listLength(server.slaves);
    int result = C_ERR;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    if (clientsArePaused())
        return C_OK;
    if (getMaxmemoryState(&mem_reported, NULL, &mem_tofree, NULL) == C_OK)      // 获取 redis 的内存使用状态
        return C_OK;

    mem_freed = 0;

    latencyStartMonitor(latency);
    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION)
        goto cant_free; /* We need to free memory, but policy forbids. */

    while (mem_freed < mem_tofree)
    {
        int j, k, i;
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            while (bestkey == NULL)
            {
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                for (i = 0; i < server.dbnum; i++)
                {
                    db = server.db + i;
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ? db->dict : db->expires;
                    if ((keys = dictSize(dict)) != 0)
                    {
                        evictionPoolPopulate(i, dict, db->dict, pool);      // 遍历每个 db, 按照淘汰策略选择 key 放入淘汰池 pool
                        total_keys += keys;
                    }
                }

                if (!total_keys)
                    break; /* No keys to evict. */

                /* Go backward from best to worst element to evict. */
                for (k = EVPOOL_SIZE - 1; k >= 0; k--)  // pool 中存储的 key 是按照 idle 的值从小到大排列的，下标越大，idle 越大
                {
                    if (pool[k].key == NULL)
                        continue;

                    bestdbid = pool[k].dbid;   // 从淘汰池中选择 idle 最大的 key

                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS)
                    {
                        de = dictFind(server.db[pool[k].dbid].dict, pool[k].key);       // 在主字典中查找 key 对应的 dictEntry
                    }
                    else
                    {
                        de = dictFind(server.db[pool[k].dbid].expires, pool[k].key);    // 在过期字典中查找 key 的 dictEntry
                    }

                    /* Remove the entry from the pool. */
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    if (de)
                    {
                        bestkey = dictGetKey(de);       // 找到了 idle 最大的 key-value
                        break;
                    }
                    else
                    {
                        /* Ghost... Iterate again. */
                    }
                }
            }
        }

        /* volatile-random and allkeys-random policy */
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
        {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            for (i = 0; i < server.dbnum; i++)
            {
                j = (++next_db) % server.dbnum;
                db = server.db + j;
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ? db->dict : db->expires;
                if (dictSize(dict) != 0)
                {
                    de = dictGetRandomKey(dict);
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }

        /* Finally remove the selected key. */
        if (bestkey)
        {
            db = server.db + bestdbid;
            robj *keyobj = createStringObject(bestkey, sdslen(bestkey));    // 创建 idle 最大的 key-value 的 redisObject 类型的 key
            propagateExpire(db, keyobj, server.lazyfree_lazy_eviction);
            /* We compute the amount of memory freed by db*Delete() alone.
             * It is possible that actually the memory needed to propagate
             * the DEL in AOF and replication link is greater than the one
             * we are freeing removing the key, but we can't account for
             * that otherwise we would never exit the loop.
             *
             * Same for CSC invalidation messages generated by signalModifiedKey.
             *
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            delta = (long long)zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);
            if (server.lazyfree_lazy_eviction)
                dbAsyncDelete(db, keyobj);
            else
                dbSyncDelete(db, keyobj);       // 从主字典中删除 key-value

            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del", eviction_latency);
            delta -= (long long)zmalloc_used_memory();
            mem_freed += delta;
            server.stat_evictedkeys++;
            signalModifiedKey(NULL, db, keyobj);
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted", keyobj, db->id);
            decrRefCount(keyobj);
            keys_freed++;

            /* When the memory to free starts to be big enough, we may
             * start spending so much time here that is impossible to
             * deliver data to the slaves fast enough, so we force the
             * transmission here inside the loop. */
            if (slaves)
                flushSlavesOutputBuffers();

            /* Normally our stop condition is the ability to release
             * a fixed, pre-computed amount of memory. However when we
             * are deleting objects in another thread, it's better to
             * check, from time to time, if we already reached our target
             * memory, since the "mem_freed" amount is computed only
             * across the dbAsyncDelete() call, while the thread can
             * release the memory all the time. */
            if (server.lazyfree_lazy_eviction && !(keys_freed % 16))
            {
                if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_OK)
                {
                    /* Let's satisfy our stop condition. */
                    mem_freed = mem_tofree;
                }
            }
        }
        else
        {
            goto cant_free; /* nothing to free... */
        }
    }
    result = C_OK;

cant_free:
    /* We are here if we are not able to reclaim memory. There is only one
     * last thing we can try: check if the lazyfree thread has jobs in queue
     * and wait... */
    if (result != C_OK)
    {
        latencyStartMonitor(lazyfree_latency);
        while (bioPendingJobsOfType(BIO_LAZY_FREE))
        {
            if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_OK)
            {
                result = C_OK;
                break;
            }
            usleep(1000);
        }
        latencyEndMonitor(lazyfree_latency);
        latencyAddSampleIfNeeded("eviction-lazyfree", lazyfree_latency);
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle", latency);
    return result;
}

/* This is a wrapper for freeMemoryIfNeeded() that only really calls the
 * function if right now there are the conditions to do so safely:
 *
 * - There must be no script in timeout condition.
 * - Nor we are loading data right now.
 *
 */
int freeMemoryIfNeededAndSafe(void)
{
    if (server.lua_timedout || server.loading)
        return C_OK;
    return freeMemoryIfNeeded();
}
