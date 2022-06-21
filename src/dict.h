/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
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

#ifndef __DICT_H
#define __DICT_H

#include "mt19937-64.h"
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#define DICT_OK 0
#define DICT_ERR 1

/** redis-7.0 版本对 dict 做了较大改动，具体参照代码实现 **/

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void)V)

// redis 中的一个 key-value 对
typedef struct dictEntry
{
    void *key; // 存储 redis 的 key-value 中的 key
    union
    {                 // value
        void *val;    // 存储一段具体的内存二进制数据
        uint64_t u64; // 存储一个 64 位无符号整形数据
        int64_t s64;  // 存储一个 64 位有符号整形数据
        double d;     // 存储一个双精度浮点数据
    } v;
    struct dictEntry *next; // 基于链表的哈希表实现，next 用于指向下一个 key-value 对。用单链表的方式解决 hash 冲突
} dictEntry;

// 保存基础哈希操作的函数指针
typedef struct dictType
{
    uint64_t (*hashFunction)(const void *key);                             // 根据 key 计算对应的哈希值
    void *(*keyDup)(void *privdata, const void *key);                      // 复制 key
    void *(*valDup)(void *privdata, const void *obj);                      // 复制 value
    int (*keyCompare)(void *privdata, const void *key1, const void *key2); // 比较 key1 和 key2
    void (*keyDestructor)(void *privdata, void *key);                      // 释放 key
    void (*valDestructor)(void *privdata, void *obj);                      // 释放 value
} dictType;

/* This is our hash table structure. Every dictionary has two of this as we
 * implement incremental rehashing, for the old to the new table. */
typedef struct dictht
{
    dictEntry **table;      // table 是一个数组结构，数组的每一个元素都是一个 "桶"，保存着一个 dictEntry 指针
    unsigned long size;     // table 中桶的数量（table 表示的数组大小）
    unsigned long sizemask; // 桶数量的掩码，大小为 size - 1（用于快速取余，确定 KV 数据落到哪个桶里）
    unsigned long used;     // 该哈希表中，已经保存的 key-value 的数量（每个桶里面都是一个 dictEntry 的单链表，所以 used 可能会超过 size）
} dictht;

/**
 * dict 中有两个 hash 表的原因：
 *     方便在 rehash 时进行数据转移。日常访问用 0 号哈希表，如果 0 号哈希表元素过多，则分配一个 2 倍 0 号哈希表大小的空间给 1 号哈希表，然后进行逐步迁移:
 *
 * redis 执行 rehash 操作时，不会一次性将所有的数据进行 rehash, 而是采用一种增量的方式，逐步的将数据转移到新的桶中：
 * 如果 redis 一次性将所有数据进行 rehash，因为其庞大的数据量并且是单线程，在 rehash 的过程中，redis 无法提供其他服务
 *
 * redis 会定期的将第一个哈希表中的数据 rehash 到第二个哈希表中。
 */
typedef struct dict
{
    dictType *type; // 基础哈希操作的函数指针
    void *privdata;
    dictht ht[2];            // 两个 hash 表
    long rehashidx;          // rehashing not in progress if rehashidx == -1. 下一次需要被 rehash 的桶的索引。当 rehash 结束后，被置为 -1
    unsigned long iterators; // number of iterators currently running。关联在本哈希表上的安全迭代器的数量
} dict;

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext()
 * should be called while iterating.
 *
 * 安全迭代器：运行迭代器的迭代过程中可以进行插入、查找以及其他操作
 * 非安全迭代器：在迭代过程中只能调用 dictNext() 操作，也就是在迭代过程中禁止对哈希表进行改变
 */
typedef struct dictIterator
{
    dict *d;
    long index;
    int table, safe; // safe 为 1 时表示是安全迭代器
    dictEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint; // 指纹值。用于不安全迭代器，防止在迭代过程中对哈希表的错误使用。迭代器释放时，会对该指纹值进行校验
} dictIterator;

typedef void(dictScanFunction)(void *privdata, const dictEntry *de);
typedef void(dictScanBucketFunction)(void *privdata, dictEntry **bucketref);

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE 4

/* ------------------------------- Macros ------------------------------------*/
// 使用自定义的 valDestructor 释放接口释放 entry 的 value
#define dictFreeVal(d, entry)     \
    if ((d)->type->valDestructor) \
    (d)->type->valDestructor((d)->privdata, (entry)->v.val)

// 设置 entry 的 value 为 _val_
#define dictSetVal(d, entry, _val_)                                   \
    do                                                                \
    {                                                                 \
        if ((d)->type->valDup)                                        \
            (entry)->v.val = (d)->type->valDup((d)->privdata, _val_); \
        else                                                          \
            (entry)->v.val = (_val_);                                 \
    } while (0)

#define dictSetSignedIntegerVal(entry, _val_) \
    do                                        \
    {                                         \
        (entry)->v.s64 = _val_;               \
    } while (0)

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do                                          \
    {                                           \
        (entry)->v.u64 = _val_;                 \
    } while (0)

#define dictSetDoubleVal(entry, _val_) \
    do                                 \
    {                                  \
        (entry)->v.d = _val_;          \
    } while (0)

// 用自定义的 keyDestructor 接口释放 entry 的 key
#define dictFreeKey(d, entry)     \
    if ((d)->type->keyDestructor) \
    (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define dictSetKey(d, entry, _key_)                                 \
    do                                                              \
    {                                                               \
        if ((d)->type->keyDup)                                      \
            (entry)->key = (d)->type->keyDup((d)->privdata, _key_); \
        else                                                        \
            (entry)->key = (_key_);                                 \
    } while (0)

#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? (d)->type->keyCompare((d)->privdata, key1, key2) : (key1) == (key2))

#define dictHashKey(d, key) (d)->type->hashFunction(key) // 为 key 计算哈希值
#define dictGetKey(he) ((he)->key)
#define dictGetVal(he) ((he)->v.val)
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)
#define dictSlots(d) ((d)->ht[0].size + (d)->ht[1].size) // 获取 dict 中桶的数量
#define dictSize(d) ((d)->ht[0].used + (d)->ht[1].used)  // 获取 dict 中存储元素的个数
#define dictIsRehashing(d) ((d)->rehashidx != -1)        // 判断是否正在 rehash

/* If our unsigned long type can store a 64 bit number, use a 64 bit PRNG. */
#if ULONG_MAX >= 0xffffffffffffffff
#define randomULong() ((unsigned long)genrand64_int64()) // 获取一个 [0, 2^64 - 1] 之间的随机数
#else
#define randomULong() random()
#endif

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *d, unsigned long size);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);
dictEntry *dictAddOrFind(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
int dictDelete(dict *d, const void *key);
dictEntry *dictUnlink(dict *ht, const void *key);
void dictFreeUnlinkedEntry(dict *d, dictEntry *he);
void dictRelease(dict *d);
dictEntry *dictFind(dict *d, const void *key);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
dictEntry *dictGetFairRandomKey(dict *d);
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);
void dictGetStats(char *buf, size_t bufsize, dict *d);
uint64_t dictGenHashFunction(const void *key, int len);
uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len);
void dictEmpty(dict *d, void(callback)(void *));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(uint8_t *seed);
uint8_t *dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);
uint64_t dictGetHash(dict *d, const void *key);
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif /* __DICT_H */
