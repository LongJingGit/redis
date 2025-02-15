/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#ifndef DICT_BENCHMARK_MAIN
#include "redisassert.h"
#else
#include <assert.h>
#endif

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio.
 *
 * redis 是以单链表的形式来解决 hash 冲突的。但是如果链表长度过长的话，会严重降低搜索的效率；同时如果键值对过少，而哈希表中的桶数过多,
 * 则会浪费内存的空间。因此需要在哈希表的装载因子过大或者过小的时候，对哈希表进行 rehash、扩容或者缩容，以达到提高搜索速度，或者优化内存存储的目的。
 *
 * 当 dict_can_resize 为 0, 不会对哈希表进行扩缩；当哈希表的装载因子大于强制 rehash 的阈值 dict_force_resize_ratio 时，仍然会进行重哈希扩容操作
 * 当 dict_can_resize 为 1, 会对哈希表进行扩缩，如果哈希表的装载因子大于 1，就会对哈希表执行 rehash 扩容操作。
 *
 * 哈希表装载因子的计算：dictSize()/dictSlots()
 */
static int dict_can_resize = 1;                  // 是否允许扩容和缩容
static unsigned int dict_force_resize_ratio = 5; // 强制 rehash 的阈值

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static long _dictKeyIndex(dict *ht, const void *key, uint64_t hash, dictEntry **existing);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_hash_function_seed[16];

void dictSetHashFunctionSeed(uint8_t *seed)
{
    memcpy(dict_hash_function_seed, seed, sizeof(dict_hash_function_seed));
}

uint8_t *dictGetHashFunctionSeed(void)
{
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictGenHashFunction(const void *key, int len)
{
    return siphash(key, len, dict_hash_function_seed);
}

uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len)
{
    return siphash_nocase(buf, len, dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/* Reset a hash table already initialized with ht_init().
 * NOTE: This function should only be called by ht_destroy(). */
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/* Create a new hash table */
dict *dictCreate(dictType *type, void *privDataPtr)
{
    dict *d = zmalloc(sizeof(*d));

    _dictInit(d, type, privDataPtr);
    return d;
}

/* Initialize the hash table */
int _dictInit(dict *d, dictType *type, void *privDataPtr)
{
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);
    d->type = type;
    d->privdata = privDataPtr;
    d->rehashidx = -1;
    d->iterators = 0;
    return DICT_OK;
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
int dictResize(dict *d)
{
    unsigned long minimal;

    if (!dict_can_resize || dictIsRehashing(d))
        return DICT_ERR;
    minimal = d->ht[0].used;
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;
    return dictExpand(d, minimal);
}

/* Expand or create the hash table */
// 申请一个新的 1 号哈希表，然后置 rehash 标志位，并不会真正的移动数据
int dictExpand(dict *d, unsigned long size)
{
    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    dictht n; /* the new hash table */
    unsigned long realsize = _dictNextPower(size);

    /* Rehashing to the same table size is not useful. */
    if (realsize == d->ht[0].size)
        return DICT_ERR;

    /* Allocate the new hash table and initialize all pointers to NULL */
    n.size = realsize;
    n.sizemask = realsize - 1;
    n.table = zcalloc(realsize * sizeof(dictEntry *));
    n.used = 0;

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    if (d->ht[0].table == NULL)
    {
        d->ht[0] = n;
        return DICT_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    d->ht[1] = n;
    d->rehashidx = 0; // 置标志位，表示正在 rehash
    return DICT_OK;
}

/* Performs N steps of incremental rehashing. Returns 1 if there are still
 * keys to move from the old to the new hash table, otherwise 0 is returned.
 *
 * Note that a rehashing step consists in moving a bucket (that may have more
 * than one key as we use chaining) from the old to the new hash table, however
 * since part of the hash table may be composed of empty spaces, it is not
 * guaranteed that this function will rehash even a single bucket, since it
 * will visit at max N*10 empty buckets in total, otherwise the amount of
 * work it does would be unbound and the function may block for a long time.
 *
 * n: 需要 rehash 的元素个数
 */
int dictRehash(dict *d, int n)
{
    int empty_visits = n * 10; /* Max number of empty buckets to visit. */
    if (!dictIsRehashing(d))
        return 0;

    while (n-- && d->ht[0].used != 0)
    {
        dictEntry *de, *nextde;

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        assert(d->ht[0].size > (unsigned long)d->rehashidx);
        while (d->ht[0].table[d->rehashidx] == NULL)
        {
            d->rehashidx++;
            if (--empty_visits == 0) // 遍历到 n*10 个空桶，终止本次 rehash 操作
                return 1;
        }
        de = d->ht[0].table[d->rehashidx];
        /* Move all the keys in this bucket from the old to the new hash HT */
        while (de) // de 是桶里面的某一个 entry 单链表
        {
            uint64_t h;

            nextde = de->next; // 保存当前 entry 节点 的 next 指针
            /* Get the index in the new hash table */
            h = dictHashKey(d, de->key) & d->ht[1].sizemask; // 计算新的数据应该落到哪个桶里
            de->next = d->ht[1].table[h];
            d->ht[1].table[h] = de;
            d->ht[0].used--;
            d->ht[1].used++;
            de = nextde;
        }
        d->ht[0].table[d->rehashidx] = NULL;
        d->rehashidx++;
    }

    /* Check if we already rehashed the whole table... */
    if (d->ht[0].used == 0) // rehash 结束，ht[0] 的元素已经全部转移到了 ht[1] 中
    {
        zfree(d->ht[0].table); // 释放第一个 hash 表
        d->ht[0] = d->ht[1];   // rehash 结束之后，会把第二个 hash 表转移给第一个 hash 表
        _dictReset(&d->ht[1]); // 重置第二个 hash 表
        d->rehashidx = -1;     // 置为 -1 表示 rehash 结束了
        return 0;
    }

    /* More to rehash... */
    return 1;
}

long long timeInMilliseconds(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}

/* Rehash in ms+"delta" milliseconds. The value of "delta" is larger
 * than 0, and is smaller than 1 in most cases. The exact upper bound
 * depends on the running time of dictRehash(d,100).
 *
 * 在1毫秒的时间片内，执行若干次 dictRehash 操作，直到所有数据都已经 rehash, 或者执行时间超过1毫秒的时间片.
 * 该接口在系统心跳中被主动调用，处理的是 redisDb.dict 主字典
 */
int dictRehashMilliseconds(dict *d, int ms)
{
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while (dictRehash(d, 100))
    {
        rehashes += 100;
        if (timeInMilliseconds() - start > ms)
            break;
    }
    return rehashes;
}

/* This function performs just a step of rehashing, and only if there are
 * no safe iterators bound to our hash table. When we have iterators in the
 * middle of a rehashing we can't mess with the two hash tables otherwise
 * some element can be missed or duplicated.
 *
 * This function is called by common lookup or update operations in the
 * dictionary so that the hash table automatically migrates from H1 to H2
 * while it is actively used.
 *
 * 本接口是一个通常在内部调用的函数，如果在没有安全迭代器的情况下，这个函数会对其中 1 个元素执行 rehash。
 * 设计这个函数的意义在于，在执行添加、删除、查找操作时被动执行一次单步 rehash，用来提高系统 rehash 的速度
 *
 * 为什么单步 rehash 需要判断迭代器的数量，而心跳中的 dictRehashMilliseconds() 不需要判断？
 *     猜测：redis 对于核心数据的操作都是单线程模式进行的，心跳中的 dictRehashMilliseconds() 在执行时会阻塞 redis 的核心主线程，
 * 此时不会对于哈希表有其他的操作，因此不需要判断关联安全迭代器的数量。而对于哈希表的迭代器，在迭代的过程中有可能基于逻辑的需求而对
 * 哈希表进行增加，查找等操作，故此需要判断关联安全迭代器的数量。
 * NOTE: 该接口主要处理的是 redisDb.dict 中的 value 对应的 散列对象, 集合对象, 有序集合对象 底层的哈希表
 */
static void _dictRehashStep(dict *d)
{
    if (d->iterators == 0)
        dictRehash(d, 1);
}

/* Add an element to the target hash table */
int dictAdd(dict *d, void *key, void *val)
{
    dictEntry *entry = dictAddRaw(d, key, NULL);

    if (!entry)
        return DICT_ERR;
    dictSetVal(d, entry, val); // 如果插入 key 成功，则设置 value
    return DICT_OK;
}

/* Low level add or find:
 * This function adds the entry but instead of setting a value returns the
 * dictEntry structure to the user, that will make sure to fill the value
 * field as he wishes.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictAddRaw(dict,mykey,NULL);
 * if (entry != NULL) dictSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned, and "*existing" is populated
 * with the existing entry if existing is not NULL.
 *
 * If key was added, the hash entry is returned to be manipulated by the caller.
 *
 * 把 key 插入到 dict 中。如果插入成功，返回插入成功的 dictEntry 的指针，否则返回 nullptr，
 * 如果 key 已经存在，则将已经存在的 key 的 dictEntry 赋值给 existing。
 *
 * 需要注意的是，这个接口在插入 key 之后，不会设置 key 对应的 value，需要调用者自己处理
 */
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing)
{
    long index;
    dictEntry *entry;
    dictht *ht;

    if (dictIsRehashing(d))
        _dictRehashStep(d);

    /* Get the index of the new element, or -1 if
     * the element already exists. */
    if ((index = _dictKeyIndex(d, key, dictHashKey(d, key), existing)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry.
     * Insert the element in top, with the assumption that in a database
     * system it is more likely that recently added entries are accessed
     * more frequently. */
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];
    entry = zmalloc(sizeof(*entry));
    entry->next = ht->table[index];
    ht->table[index] = entry;
    ht->used++;

    /* Set the hash entry fields. */
    dictSetKey(d, entry, key); // 只设置了 dictEntry 的 key，没有设置 value
    return entry;
}

/* Add or Overwrite:
 * Add an element, discarding the old value if the key already exists.
 * Return 1 if the key was added from scratch, 0 if there was already an
 * element with such key and dictReplace() just performed a value update
 * operation.
 *
 * 如果 key 不存在，则插入 key-value，并返回 1
 * 如果 key 存在，则覆盖原有 key 的 value 值，并返回 0
 */
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, *existing, auxentry;

    /* Try to add the element. If the key
     * does not exists dictAdd will succeed. */
    entry = dictAddRaw(d, key, &existing);
    if (entry)
    {
        dictSetVal(d, entry, val);
        return 1;
    }

    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */
    auxentry = *existing;
    dictSetVal(d, existing, val);
    dictFreeVal(d, &auxentry);
    return 0;
}

/* Add or Find:
 * dictAddOrFind() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information.
 *
 * 根据 key 查找对应的 dictEntry 指针，如果 key 不存在，则向其中插入一个 key，并返回 dictEntry。
 * 如果 key 已经存在，返回已经存在的 key 的 dictEntry 指针。如果查找不到，返回 nullptr
 */
dictEntry *dictAddOrFind(dict *d, void *key)
{
    dictEntry *entry, *existing;
    entry = dictAddRaw(d, key, &existing);
    return entry ? entry : existing;
}

/* Search and remove an element. This is an helper function for
 * dictDelete() and dictUnlink(), please check the top comment
 * of those functions. */
static dictEntry *dictGenericDelete(dict *d, const void *key, int nofree)
{
    uint64_t h, idx;
    dictEntry *he, *prevHe;
    int table;

    if (d->ht[0].used == 0 && d->ht[1].used == 0)
        return NULL;

    if (dictIsRehashing(d))
        _dictRehashStep(d);
    h = dictHashKey(d, key);

    for (table = 0; table <= 1; table++)
    {
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        prevHe = NULL;
        while (he)
        {
            if (key == he->key || dictCompareKeys(d, key, he->key))
            {
                /* Unlink the element from the list */
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;
                if (!nofree)
                {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                    zfree(he);
                }
                d->ht[table].used--;
                return he;
            }
            prevHe = he;
            he = he->next;
        }
        if (!dictIsRehashing(d))
            break;
    }
    return NULL; /* not found */
}

/* Remove an element, returning DICT_OK on success or DICT_ERR if the
 * element was not found. */
int dictDelete(dict *ht, const void *key)
{
    return dictGenericDelete(ht, key, 0) ? DICT_OK : DICT_ERR;
}

/* Remove an element from the table, but without actually releasing
 * the key, value and dictionary entry. The dictionary entry is returned
 * if the element was found (and unlinked from the table), and the user
 * should later call `dictFreeUnlinkedEntry()` with it in order to release it.
 * Otherwise if the key is not found, NULL is returned.
 *
 * This function is useful when we want to remove something from the hash
 * table but want to use its value before actually deleting the entry.
 * Without this function the pattern would require two lookups:
 *
 *  entry = dictFind(...);
 *  // Do something with entry
 *  dictDelete(dictionary,entry);
 *
 * Thanks to this function it is possible to avoid this, and use
 * instead:
 *
 * entry = dictUnlink(dictionary,entry);
 * // Do something with entry
 * dictFreeUnlinkedEntry(entry); // <- This does not need to lookup again.
 *
 * 将 key 对应的 entry 从哈希表 ht 中删除，但是并没有释放 entry 的内存，而是将 entry 返回给调用者，
 * 由调用者使用 dictFreeUnlinkedEntry() 接口对 entry 进行释放。
 */
dictEntry *dictUnlink(dict *ht, const void *key)
{
    return dictGenericDelete(ht, key, 1);
}

/* You need to call this function to really free the entry after a call
 * to dictUnlink(). It's safe to call this function with 'he' = NULL.
 *
 * 一般用于释放从 dictUnlink() 中删除的 entry
 */
void dictFreeUnlinkedEntry(dict *d, dictEntry *he)
{
    if (he == NULL)
        return;
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}

/* Destroy an entire dictionary */
// 逐个释放哈希表 dictht 中的每个桶中的元素
int _dictClear(dict *d, dictht *ht, void(callback)(void *))
{
    unsigned long i;

    /* Free all the elements */
    for (i = 0; i < ht->size && ht->used > 0; i++)
    {
        dictEntry *he, *nextHe;

        /**
         * 每清空 65535 个桶时，会调用 callback 函数，处理响应逻辑。
         * redis 的哈希表操作很多的是以非阻塞的方式进行的，但是释放与清空的操作却是以阻塞的方式进行的，当删除一个很大的哈希表时，
         * 缺少一种增量逐步执行某些操作的机制，所以作者引入了这个机制，可以在删除 hash 表时，以一个固定的时间间隔来执行回调函数
         */
        if (callback && (i & 65535) == 0)
            callback(d->privdata);

        if ((he = ht->table[i]) == NULL) // 空桶
            continue;

        while (he)
        {
            // 逐个释放链表的每一个节点: 包括 key value 和 节点本身
            nextHe = he->next;
            dictFreeKey(d, he);
            dictFreeVal(d, he);
            zfree(he);
            ht->used--;
            he = nextHe;
        }
    }
    /* Free the table and the allocated cache structure */
    zfree(ht->table);
    /* Re-initialize the table */
    _dictReset(ht);
    return DICT_OK; /* never fails */
}

/* Clear & Release the hash table */
// 清空并释放 dict 中的两个哈希表，并释放 dict 结构
void dictRelease(dict *d)
{
    _dictClear(d, &d->ht[0], NULL); // 清空并释放 0 号哈希表
    _dictClear(d, &d->ht[1], NULL); // 清空并释放 1 号哈希表
    zfree(d);                       // 释放 dict
}

// 根据 key 在 dict 中查找对应的 dictEntry 指针
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    uint64_t h, idx, table;

    if (dictSize(d) == 0)
        return NULL; /* dict is empty */
    if (dictIsRehashing(d))
        _dictRehashStep(d);
    h = dictHashKey(d, key);
    for (table = 0; table <= 1; table++)
    {
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        while (he)
        {
            if (key == he->key || dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }
        if (!dictIsRehashing(d))
            return NULL;
    }
    return NULL;
}

void *dictFetchValue(dict *d, const void *key)
{
    dictEntry *he;

    he = dictFind(d, key);
    return he ? dictGetVal(he) : NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictionary
 * at a given time, it's just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating. */
long long dictFingerprint(dict *d)
{
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long)d->ht[0].table;
    integers[1] = d->ht[0].size;
    integers[2] = d->ht[0].used;
    integers[3] = (long)d->ht[1].table;
    integers[4] = d->ht[1].size;
    integers[5] = d->ht[1].used;

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++)
    {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

dictIterator *dictGetSafeIterator(dict *d)
{
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

// 哈希表迭代器的迭代接口
dictEntry *dictNext(dictIterator *iter)
{
    while (1)
    {
        if (iter->entry == NULL)
        {
            dictht *ht = &iter->d->ht[iter->table];
            if (iter->index == -1 && iter->table == 0)
            {
                if (iter->safe)
                    iter->d->iterators++; // 对当前迭代器 +1。如果 iterators 大于0，那么原本在添加、删除查找操作中进行的单步 rehash 就不会进行
                else
                    iter->fingerprint = dictFingerprint(iter->d); // 计算指纹值
            }

            iter->index++;
            if (iter->index >= (long)ht->size)
            {
                if (dictIsRehashing(iter->d) && iter->table == 0)
                {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                }
                else
                {
                    break;
                }
            }
            iter->entry = ht->table[iter->index];
        }
        else
        {
            iter->entry = iter->nextEntry;
        }
        if (iter->entry)
        {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL;
}

// 释放迭代器
void dictReleaseIterator(dictIterator *iter)
{
    if (!(iter->index == -1 && iter->table == 0))
    {
        if (iter->safe)
            iter->d->iterators--; // 当 iterators 为 0 时，才可以进行单步 rehash
        else
            assert(iter->fingerprint == dictFingerprint(iter->d)); // 重新计算指纹值并校验
    }
    zfree(iter);
}

/**
 * Return a random entry from the hash table. Useful to implement randomized algorithms.
 * 从 hash 表中，随机返回一个 key-value 数据
 * 1. 从所有的桶中，随机选择一个非空的桶
 * 2. 从这个选定的桶的链表中，随机返回一个 dictEntry
 *
 * 每个元素并不是等概率的被采样出来。如果某个桶中的链表长度比较短的话，那么该桶中的元素相比较与其他链表较长的桶的元素，有更高的概率被获取
 */
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned long h;
    int listlen, listele;

    if (dictSize(d) == 0)
        return NULL;

    if (dictIsRehashing(d))
        _dictRehashStep(d);

    if (dictIsRehashing(d))
    {
        do
        {
            /* We are sure there are no elements in indexes from 0
             * to rehashidx-1 */
            h = d->rehashidx + (randomULong() % (dictSlots(d) - d->rehashidx));
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] : d->ht[0].table[h];
        } while (he == NULL);
    }
    else
    {
        do
        {
            h = randomULong() & d->ht[0].sizemask;
            he = d->ht[0].table[h];
        } while (he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    listlen = 0;
    orighe = he;
    while (he)
    {
        he = he->next;
        listlen++;
    }

    listele = random() % listlen;
    he = orighe;

    while (listele--)
        he = he->next;

    return he;
}

/**
 * This function samples the dictionary to return a few keys from random
 * locations.
 * 该接口用于从哈希表中，随机地获取给定 count 数量的元素
 *
 * It does not guarantee to return all the keys specified in 'count', nor
 * it does guarantee to return non-duplicated elements, however it will make
 * some effort to do both things.
 * 1. 不能保证一定会获取 count 个采样结果(哈希表中的元素个数小于 count)
 * 2. 不能保证返回的采样结果中一定没有重复的
 *
 * Returned pointers to hash table entries are stored into 'des' that
 * points to an array of dictEntry pointers. The array must have room for
 * at least 'count' elements, that is the argument we pass to the function
 * to tell how many random elements we need.
 * 返回值存储在 des 的指针数组里。该指针数组必须有 count 个元素的空间。
 *
 * The function returns the number of items stored into 'des', that may
 * be less than 'count' if the hash table has less than 'count' elements
 * inside, or if not enough elements were found in a reasonable amount of
 * steps.
 * 返回值为存储在 des 中的元素个数，如果哈希表中的元素个数小于 count, 或者在特定的次数内没有找到足够的元素，则 des 中元素个数可能小于 count
 *
 * Note that this function is not suitable when you need a good distribution
 * of the returned items, but only when you need to "sample" a given number
 * of continuous elements to run some kind of algorithm or to produce
 * statistics. However the function is much faster than dictGetRandomKey()
 * at producing N elements.
 *
 * 该接口在生成 n 个元素时要比 dictGetRandomKey() 接口快得多
 */
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count)
{
    unsigned long j;      /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    if (dictSize(d) < count) // 如果哈希表中元素数量小于 count，则修正 count 为 dictSize()
        count = dictSize(d);
    maxsteps = count * 10; // 采样的最大次数。如果超过该次数还没有获取到 count 数量的元素，则直接返回

    /* Try to do a rehashing work proportional to 'count'. */
    for (j = 0; j < count; j++)
    {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }

    tables = dictIsRehashing(d) ? 2 : 1; // 判断是否正在 rehash. 如果正在 rehash，则使用了两个哈希表。否则只使用了一个哈希表
    maxsizemask = d->ht[0].sizemask;
    if (tables > 1 && maxsizemask < d->ht[1].sizemask) // 选择两个哈希表中最大的 mask
        maxsizemask = d->ht[1].sizemask;

    /* Pick a random point inside the larger table. */
    unsigned long i = randomULong() & maxsizemask; // 用上面找到的最大的 mask 生成随机索引
    unsigned long emptylen = 0;                    /* Continuous empty entries so far. */

    /**
     * 循环停止的两个条件：
     * 1. 获取到 count 数量的元素
     * 2. 采样次数到达最大限值，则停止采样，防止 redis 长时间阻塞在这个调用中。这也是导致该接口无法保证返回 count 个采样结果的原因之一
     */
    while (stored < count && maxsteps--)
    {
        for (j = 0; j < tables; j++)
        {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            if (tables == 2 && j == 0 && i < (unsigned long)d->rehashidx)
            {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (i >= d->ht[1].size)
                    i = d->rehashidx;
                else
                    continue;
            }

            // 如果生成的随机索引大于当前哈希表的 size，则用该索引查看另一个哈希表
            // 前面获取到的 maxsizemask 可以保证随机索引在两个哈希表中至少有一个是有效的
            if (i >= d->ht[j].size)
                continue; /* Out of range for this table. */

            dictEntry *he = d->ht[j].table[i];

            /* Count contiguous empty buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            // 如果随机选择的桶为空，则选择下一个桶进行采样；
            if (he == NULL)
            {
                emptylen++;
                // 如果连续选择的五个桶都为空，则重新生成随机索引（注意：这里可能导致数据被重复采样）
                if (emptylen >= 5 && emptylen > count)
                {
                    i = randomULong() & maxsizemask;
                    emptylen = 0;
                }
            }
            else
            {
                // 根据随机索引找到了对应的桶，将该桶中的元素尽可能的加入到返回结果中，如果到达 count 的数量，直接调用结束
                emptylen = 0;
                while (he)
                {
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    if (stored == count)
                        return stored;
                }
            }
        }

        i = (i + 1) & maxsizemask;
    }
    return stored;
}

/* This is like dictGetRandomKey() from the POV of the API, but will do more
 * work to ensure a better distribution of the returned element.
 *
 * This function improves the distribution because the dictGetRandomKey()
 * problem is that it selects a random bucket, then it selects a random
 * element from the chain in the bucket. However elements being in different
 * chain lengths will have different probabilities of being reported. With
 * this function instead what we do is to consider a "linear" range of the table
 * that may be constituted of N buckets with chains of different lengths
 * appearing one after the other. Then we report a random element in the range.
 * In this way we smooth away the problem of different chain lengths. */
#define GETFAIR_NUM_ENTRIES 15
dictEntry *dictGetFairRandomKey(dict *d)
{
    dictEntry *entries[GETFAIR_NUM_ENTRIES];
    unsigned int count = dictGetSomeKeys(d, entries, GETFAIR_NUM_ENTRIES);
    /* Note that dictGetSomeKeys() may return zero elements in an unlucky
     * run() even if there are actually elements inside the hash table. So
     * when we get zero, we call the true dictGetRandomKey() that will always
     * yeld the element if the hash table has at least one. */
    if (count == 0)
        return dictGetRandomKey(d);
    unsigned int idx = rand() % count;
    return entries[idx];
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static unsigned long rev(unsigned long v)
{
    unsigned long s = CHAR_BIT * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0UL;
    while ((s >>= 1) > 0)
    {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

/* dictScan() is used to iterate over the elements of a dictionary.
 *
 * Iterating works the following way:
 *
 * 1) Initially you call the function using a cursor (v) value of 0.
 * 2) The function performs one step of the iteration, and returns the
 *    new cursor value you must use in the next call.
 * 3) When the returned cursor is 0, the iteration is complete.
 *
 * The function guarantees all elements present in the
 * dictionary get returned between the start and end of the iteration.
 * However it is possible some elements get returned multiple times.
 *
 * For every element returned, the callback argument 'fn' is
 * called with 'privdata' as first argument and the dictionary entry
 * 'de' as second argument.
 *
 * HOW IT WORKS.
 *
 * The iteration algorithm was designed by Pieter Noordhuis.
 * The main idea is to increment a cursor starting from the higher order
 * bits. That is, instead of incrementing the cursor normally, the bits
 * of the cursor are reversed, then the cursor is incremented, and finally
 * the bits are reversed again.
 *
 * This strategy is needed because the hash table may be resized between
 * iteration calls.
 *
 * dict.c hash tables are always power of two in size, and they
 * use chaining, so the position of an element in a given table is given
 * by computing the bitwise AND between Hash(key) and SIZE-1
 * (where SIZE-1 is always the mask that is equivalent to taking the rest
 *  of the division between the Hash of the key and SIZE).
 *
 * For example if the current hash table size is 16, the mask is
 * (in binary) 1111. The position of a key in the hash table will always be
 * the last four bits of the hash output, and so forth.
 *
 * WHAT HAPPENS IF THE TABLE CHANGES IN SIZE?
 *
 * If the hash table grows, elements can go anywhere in one multiple of
 * the old bucket: for example let's say we already iterated with
 * a 4 bit cursor 1100 (the mask is 1111 because hash table size = 16).
 *
 * If the hash table will be resized to 64 elements, then the new mask will
 * be 111111. The new buckets you obtain by substituting in ??1100
 * with either 0 or 1 can be targeted only by keys we already visited
 * when scanning the bucket 1100 in the smaller hash table.
 *
 * By iterating the higher bits first, because of the inverted counter, the
 * cursor does not need to restart if the table size gets bigger. It will
 * continue iterating using cursors without '1100' at the end, and also
 * without any other combination of the final 4 bits already explored.
 *
 * Similarly when the table size shrinks over time, for example going from
 * 16 to 8, if a combination of the lower three bits (the mask for size 8
 * is 111) were already completely explored, it would not be visited again
 * because we are sure we tried, for example, both 0111 and 1111 (all the
 * variations of the higher bit) so we don't need to test it again.
 *
 * WAIT... YOU HAVE *TWO* TABLES DURING REHASHING!
 *
 * Yes, this is true, but we always iterate the smaller table first, then
 * we test all the expansions of the current cursor into the larger
 * table. For example if the current cursor is 101 and we also have a
 * larger table of size 16, we also test (0)101 and (1)101 inside the larger
 * table. This reduces the problem back to having only one table, where
 * the larger one, if it exists, is just an expansion of the smaller one.
 *
 * LIMITATIONS
 *
 * This iterator is completely stateless, and this is a huge advantage,
 * including no additional memory used.
 *
 * The disadvantages resulting from this design are:
 *
 * 1) It is possible we return elements more than once. However this is usually
 *    easy to deal with in the application level.
 * 2) The iterator must return multiple elements per call, as it needs to always
 *    return all the keys chained in a given bucket, and all the expansions, so
 *    we are sure we don't miss keys moving during rehashing.
 * 3) The reverse cursor is somewhat hard to understand at first, but this
 *    comment is supposed to help.
 *
 * 代码解释参考：《Redis源码学习(6)-Redis中哈希表的实现（下）》
 *
 * redis 中对哈希表的遍历并不是按照 0->1->2->3->4->5->6->... 这样的顺序进行的，而是采用了高位进位加法。
 * 以拥有 8 个桶并且没有 rehash 的哈希表来说，其遍历顺序为: 0->4->2->6->1->5->3->7->0.
 * 转化为二进制就是 000->100->010->110->001->101->011->111->000
 *
 * 1. 从 0 开始，高位加 1
 * 2. 向低位进位，到 0 结束
 *
 * redis 采用这种遍历方式的原因在于，对于哈希表的扫描，不是一次性完成的，而是客户端通过调用 SCAN 命令增量的进行，而在这个过程中，有可能会发生 rehash 操作，
 * 如果采用 0->1->2->3->4->... 这样的扫描顺序，则有可能漏掉某些元素，或者对某些元素进行了重复扫描。重复扫描可以通过客户端自己维护一个数据集合，来对重复数据
 * 进行过滤，但是如果某些元素被漏掉，那么则有可能会产生一些错误。
 */
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       dictScanBucketFunction *bucketfn,
                       void *privdata)
{
    dictht *t0, *t1;
    const dictEntry *de, *next;
    unsigned long m0, m1;

    if (dictSize(d) == 0)
        return 0;

    /* Having a safe iterator means no rehashing can happen, see _dictRehashStep.
     * This is needed in case the scan callback tries to do dictFind or alike. */
    d->iterators++;

    if (!dictIsRehashing(d))
    {
        t0 = &(d->ht[0]);
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        if (bucketfn)
            bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de)
        {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Set unmasked bits so incrementing the reversed cursor
         * operates on the masked bits */
        v |= ~m0;

        /* Increment the reverse cursor */
        v = rev(v);
        v++;
        v = rev(v);
    }
    else
    {
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        if (t0->size > t1->size)
        {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        if (bucketfn)
            bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de)
        {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        do
        {
            /* Emit entries at cursor */
            if (bucketfn)
                bucketfn(privdata, &t1->table[v & m1]);
            de = t1->table[v & m1];
            while (de)
            {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment the reverse cursor not covered by the smaller mask.*/
            v |= ~m1;
            v = rev(v);
            v++;
            v = rev(v);

            /* Continue while bits covered by mask difference is non-zero */
        } while (v & (m0 ^ m1));
    }

    /* undo the ++ at the top */
    d->iterators--;

    return v;
}

/* ------------------------- private functions ------------------------------ */

/* Expand the hash table if needed */
// 给 dict 添加一个新的 key-value 时，都会调用该接口来判断是否需要扩容
static int _dictExpandIfNeeded(dict *d)
{
    /* Incremental rehashing already in progress. Return. */
    if (dictIsRehashing(d))
        return DICT_OK;

    /* If the hash table is empty expand it to the initial size. */
    if (d->ht[0].size == 0)
        return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets.
     *
     * 哈希表中存储的元素数量超过或者等于桶的数量，且设置了 dict_can_resize 或者装载因子大于 dict_force_resize_ratio 时，就会进行扩容
     */
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used / d->ht[0].size > dict_force_resize_ratio))
    {
        return dictExpand(d, d->ht[0].used * 2);
    }
    return DICT_OK;
}

/* Our hash table capability is a power of two */
// 通过 size 来计算需要扩容的大小
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX)
        return LONG_MAX + 1LU;
    while (1)
    {
        if (i >= size)
            return i;
        i *= 2;
    }
}

/* Returns the index of a free slot that can be populated with
 * a hash entry for the given 'key'.
 * If the key already exists, -1 is returned
 * and the optional output parameter may be filled.
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table.
 *
 * 根据 key 和哈希值，返回应该放置 key 的桶的索引。如果 key 已经在哈希表中存在，则返回 -1，并将已经存在的 key 赋值给 existing
 *
 * 注意：如果没有正在进行 rehash，则只会在第一个哈希表中查找，否则会在两个哈希表中进行查找
 */
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing)
{
    unsigned long idx, table;
    dictEntry *he;
    if (existing)
        *existing = NULL;

    /* Expand the hash table if needed */
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;

    for (table = 0; table <= 1; table++)
    {
        idx = hash & d->ht[table].sizemask; // 计算哈希值对应的桶的索引
        /* Search if this slot does not already contain the given key */
        he = d->ht[table].table[idx];
        while (he)
        {
            if (key == he->key || dictCompareKeys(d, key, he->key)) // key 已经存在
            {
                if (existing)
                    *existing = he;
                return -1;
            }
            he = he->next;
        }
        if (!dictIsRehashing(d)) // 没有 rehash, 只在 0 号哈希表中进行查找，否则还需要查找 1 号哈希表
            break;
    }
    return idx;
}

// 释放 dict 中的两个哈希表，但是并不释放 dict 结构
void dictEmpty(dict *d, void(callback)(void *))
{
    _dictClear(d, &d->ht[0], callback);
    _dictClear(d, &d->ht[1], callback);
    d->rehashidx = -1;
    d->iterators = 0;
}

void dictEnableResize(void)
{
    dict_can_resize = 1;
}

void dictDisableResize(void)
{
    dict_can_resize = 0;
}

uint64_t dictGetHash(dict *d, const void *key)
{
    return dictHashKey(d, key);
}

/* Finds the dictEntry reference by using pointer and pre-calculated hash.
 * oldkey is a dead pointer and should not be accessed.
 * the hash value should be provided using dictGetHash.
 * no string / key comparison is performed.
 * return value is the reference to the dictEntry if found, or NULL if not found. */
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash)
{
    dictEntry *he, **heref;
    unsigned long idx, table;

    if (dictSize(d) == 0)
        return NULL; /* dict is empty */
    for (table = 0; table <= 1; table++)
    {
        idx = hash & d->ht[table].sizemask;
        heref = &d->ht[table].table[idx];
        he = *heref;
        while (he)
        {
            if (oldptr == he->key)
                return heref;
            heref = &he->next;
            he = *heref;
        }
        if (!dictIsRehashing(d))
            return NULL;
    }
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50
size_t _dictGetStatsHt(char *buf, size_t bufsize, dictht *ht, int tableid)
{
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];
    size_t l = 0;

    if (ht->used == 0)
    {
        return snprintf(buf, bufsize,
                        "No stats available for empty dictionaries\n");
    }

    /* Compute stats. */
    for (i = 0; i < DICT_STATS_VECTLEN; i++)
        clvector[i] = 0;
    for (i = 0; i < ht->size; i++)
    {
        dictEntry *he;

        if (ht->table[i] == NULL)
        {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while (he)
        {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN - 1)]++;
        if (chainlen > maxchainlen)
            maxchainlen = chainlen;
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf + l, bufsize - l,
                  "Hash table %d stats (%s):\n"
                  " table size: %ld\n"
                  " number of elements: %ld\n"
                  " different slots: %ld\n"
                  " max chain length: %ld\n"
                  " avg chain length (counted): %.02f\n"
                  " avg chain length (computed): %.02f\n"
                  " Chain length distribution:\n",
                  tableid, (tableid == 0) ? "main hash table" : "rehashing target",
                  ht->size, ht->used, slots, maxchainlen,
                  (float)totchainlen / slots, (float)ht->used / slots);

    for (i = 0; i < DICT_STATS_VECTLEN - 1; i++)
    {
        if (clvector[i] == 0)
            continue;
        if (l >= bufsize)
            break;
        l += snprintf(buf + l, bufsize - l,
                      "   %s%ld: %ld (%.02f%%)\n",
                      (i == DICT_STATS_VECTLEN - 1) ? ">= " : "",
                      i, clvector[i], ((float)clvector[i] / ht->size) * 100);
    }

    /* Unlike snprintf(), return the number of characters actually written. */
    if (bufsize)
        buf[bufsize - 1] = '\0';
    return strlen(buf);
}

void dictGetStats(char *buf, size_t bufsize, dict *d)
{
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf, bufsize, &d->ht[0], 0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0)
    {
        _dictGetStatsHt(buf, bufsize, &d->ht[1], 1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize)
        orig_buf[orig_bufsize - 1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef DICT_BENCHMARK_MAIN

#include "sds.h"

uint64_t hashCallback(const void *key)
{
    return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

int compareCallback(void *privdata, const void *key1, const void *key2)
{
    int l1, l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2)
        return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg)                                      \
    do                                                          \
    {                                                           \
        elapsed = timeInMilliseconds() - start;                 \
        printf(msg ": %ld items in %lld ms\n", count, elapsed); \
    } while (0);

/* dict-benchmark [count] */
int main(int argc, char **argv)
{
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType, NULL);
    long count = 0;

    if (argc == 2)
    {
        count = strtol(argv[1], NULL, 10);
    }
    else
    {
        count = 5000000;
    }

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        int retval = dictAdd(dict, sdsfromlonglong(j), (void *)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict))
    {
        dictRehashMilliseconds(dict, 100);
    }

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict, key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict, key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(rand() % count);
        dictEntry *de = dictFind(dict, key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        dictEntry *de = dictGetRandomKey(dict);
        assert(de != NULL);
    }
    end_benchmark("Accessing random keys");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict, key);
        assert(de == NULL);
        sdsfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(j);
        int retval = dictDelete(dict, key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict, key, (void *)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
}
#endif
