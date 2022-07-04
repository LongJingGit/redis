/* quicklist.h - A generic doubly linked quicklist implementation
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this quicklist of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this quicklist of conditions and the following disclaimer in the
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

#include <stdint.h> // for UINTPTR_MAX

#ifndef __QUICKLIST_H__
#define __QUICKLIST_H__

/**
 * quicklist 结合了双端链表 adlist 和压缩链表 ziplist 的特性：
 * 1. 使用 adlist 描述整个 quicklist
 * 2. quicklist 的每一个节点都使用 ziplist 作为底层数据存储
 *
 * 对于链表结构而言，通常对头部和尾部的访问比较频繁，而对链表中间的数据访问并不是特别频繁，因此出于节省空间的目的，会对快速链表的
 * 中间节点的底层数据结构--压缩链表 ziplist 使用 LZF 算法进行压缩
 */

/* Node, quicklist, and Iterator are the only data structures used currently. */

/* quicklistNode is a 32 byte struct describing a ziplist for a quicklist.
 * We use bit fields keep the quicklistNode at 32 bytes.
 * count: 16 bits, max 65536 (max zl bytes is 65k, so max count actually < 32k).
 * encoding: 2 bits, RAW=1, LZF=2.
 * container: 2 bits, NONE=1, ZIPLIST=2.
 * recompress: 1 bit, bool, true if node is temporary decompressed for usage.
 * attempted_compress: 1 bit, boolean, used for verifying during testing.
 * extra: 10 bits, free for future use; pads out the remainder of 32 bits
 *
 * quicklistNode.recompress 字段的说明：
 * 由于快速链表 quicklist 中只有两端的部分链表节点是处于未压缩的状态，因此当我们需要操作一个位于快速链表中间的链表节点时，
 * 我们需要对该链表节点先调用 quicklistDecompressNode() 进行解压，当完成对该节点的操作之后，再调用 quicklistCompressNode() 对其进行压缩。
 * 如果对链表节点的操作在一次函数调用中就可以完成，那么在同一个堆栈中调用解压与压缩函数接口便可以完成相关的操作。
 *
 * 但是在一些情况下，调用者无法在对链表节点进行解压之后立即对其进行重新压缩，压缩操作需要在另外的地方进行，因此 Redis 在定义压缩链表节点数据结构的时候，
 * 特意定义了 quicklistNode.recompress 这个字段用于处理上述这种情况，同时也定义了两个宏，用于处理这种无法原地重压缩的情况：
 *
 * define quicklistDecompressNodeForUse(_node)
 * define quicklistRecompressOnly(_ql, _node)
 *
 * 如果将 recompress 置为 1，说明该节点是一个临时被解压的节点，需要在后续的操作中使用 quicklistRecompressOnly() 将其重新压缩
 *
 * 注意：快速链表节点 quicklistNode 中的指针 zl 指向压缩链表 ziplist。ziplist 是 quicklist 在底层实际保存数据的结构
 *
 **/
typedef struct quicklistNode
{
    struct quicklistNode *prev;
    struct quicklistNode *next;

     /* 每个快速链表节点都使用底层的压缩链表 zl 来存储实际的数据, 而压缩链表 zl 中利用多个数据节点 entry 来存储数据.
      * 后文中提到的快速链表的数据节点实际上就是压缩链表 zl 的数据节点 */
    unsigned char *zl; // 指向压缩链表, 用来存储实际数据的地方. 可能是原生的 ziplist 或者经过 LZF 算法压缩过的 ziplist（由 encoding 字段决定）
    unsigned int sz;   // ziplist size in bytes. 当前链表节点中压缩链表 zl 的原始大小（即使数据是经过 LZF 算法压缩的）

    unsigned int count : 16;  // count of items in ziplist. 当前链表节点中数据节点 entry 的数量
    unsigned int encoding : 2;           // RAW==1 or LZF==2
    unsigned int container : 2;          // NONE==1 or ZIPLIST==2
    unsigned int recompress : 1;         // was this node previous compressed?
    unsigned int attempted_compress : 1; // node can't compress; too small
    unsigned int extra : 10;             // more bits to steal for future usage
} quicklistNode;

/* quicklistLZF is a 4+N byte struct holding 'sz' followed by 'compressed'.
 * 'sz' is byte length of 'compressed' field.
 * 'compressed' is LZF data with total (compressed) length 'sz'
 * NOTE: uncompressed length is stored in quicklistNode->sz.
 * When quicklistNode->zl is compressed, node->zl points to a quicklistLZF */
typedef struct quicklistLZF
{
    unsigned int sz;   // LZF size in bytes. 压缩链表 zl 压缩后的大小，注意和 quicklistNode.sz 字段区分
    char compressed[]; // 压缩数据
} quicklistLZF;

/* Bookmarks are padded with realloc at the end of of the quicklist struct.
 * They should only be used for very big lists if thousands of nodes were the
 * excess memory usage is negligible, and there's a real need to iterate on them
 * in portions.
 * When not used, they don't add any memory overhead, but when used and then
 * deleted, some overhead remains (to avoid resonance).
 * The number of bookmarks used should be kept to minimum since it also adds
 * overhead on node deletion (searching for a bookmark to update). */
typedef struct quicklistBookmark
{
    quicklistNode *node;
    char *name;
} quicklistBookmark;

#if UINTPTR_MAX == 0xffffffff
/* 32-bit */
#define QL_FILL_BITS 14
#define QL_COMP_BITS 14
#define QL_BM_BITS 4
#elif UINTPTR_MAX == 0xffffffffffffffff
/* 64-bit */
#define QL_FILL_BITS 16
#define QL_COMP_BITS 16
#define QL_BM_BITS 4 /* we can encode more, but we rather limit the user \
                        since they cause performance degradation. */
#else
#error unknown arch bits count
#endif

/* quicklist is a 40 byte struct (on 64-bit systems) describing a quicklist.
 * 'count' is the number of total entries.
 * 'len' is the number of quicklist nodes.
 * 'compress' is: 0 if compression disabled, otherwise it's the number
 *                of quicklistNodes to leave uncompressed at ends of quicklist.
 * 'fill' is the user-requested (or default) fill factor.
 * 'bookmakrs are an optional feature that is used by realloc this struct,
 *      so that they don't consume memory when not used. */
typedef struct quicklist
{
    quicklistNode *head;    // 快速链表的头结点
    quicklistNode *tail;    // 快速链表的尾结点

    /**
     * 注意区分数据节点和链表节点：
     * 1. quicklist 里面有多个 quicklistNode, 这些 quicklistNode 就是快速链表的 "链表节点"
     * 2. quicklistNode 中指针 zl 指向的内容实际为压缩链表 ziplist, 每个压缩链表有多个 entry，这些 entry 就是快速链表的 "数据节点"
     *
     * NOTE: 不能用以下方式统计快速链表中数据节点的个数，因为每个快速链表节点底层的压缩链表中的 entry 数量不相等
     *      quicklist.count = quicklist.len * quicklistNode.count
     */
    unsigned long count; // total count of all entries in all ziplists. 所有数据节点 entry 的数量
    unsigned long len;   // number of quicklistNodes. 链表节点的数量

    /**
     * 装载因子.
     * 1. 如果为正数, 表示每一个链表节点中可存储的数据节点 entry 的个数的上限, 也就是 quicklistNode.count 字段的上限。fill 不宜过大，否则会带来性能问题。上限为 FILL_MAX, 最大可以表示 2^16-1 个节点, 如果节点个数超过该值，需要遍历整个快速链表才可以获得链表的节点个数
     * 2. 如果为负数, 表示每个链表节点中压缩链表 zl 的最大内存大小, 也就是 quicklistNode.sz 字段的上限. 上限为 COMPRESS_MAX, 对应着五档内存限制:
     *    2.1 -1: 4096 字节
     *    2.2 -2: 8192 字节
     *    2.3 -3: 16384 字节
     *    2.4 -4: 32768 字节
     *    2.5 -5: 65536 字节
     */
    int fill : QL_FILL_BITS; /* fill factor for individual nodes */

    /**
     * 表示压缩链表的压缩深度。
     * 链表的访问主要集中在两端，因此 quicklist 使用这个字段来确定在链表两端不被压缩的链表节点 quicklistNode 的数量；
     * 另外通常情况下，处于中间的链表节点都是使用的 LZF 算法压缩后的数据，只有在需要访问的时候，才会被临时解压出来.
     *
     * NOTE: 实际上被压缩的是链表节点 quicklistNode 中的 zl 指针指向的压缩链表 ziplist
     */
    unsigned int compress : QL_COMP_BITS; /* depth of end nodes not to compress;0=off */

    unsigned int bookmark_count : QL_BM_BITS;
    quicklistBookmark bookmarks[];
} quicklist;

typedef struct quicklistIter
{
    const quicklist *quicklist; // 迭代器所绑定的 quicklist
    quicklistNode *current;     // 迭代器当前遍历到的链表节点 quicklistNode
    unsigned char *zi;          // 迭代器遍历到的 quicklistNode 中的数据节点 entry 的指针
    long offset;                // offset in current ziplist. 迭代器当前遍历到的数据节点 entry 在 ziplist 中的偏移
    int direction;              // 迭代器的遍历方向
} quicklistIter;

// 用 quicklistEntry 来描述快速链表节点底层的压缩链表 zl 中的数据节点 entry
typedef struct quicklistEntry
{
    const quicklist *quicklist; // 指向当前数据节点 entry 所在的 quicklist
    quicklistNode *node;        // 当前数据节点 entry 所在的链表节点 quicklistNode
    unsigned char *zi;          // 当前数据节点 entry 在链表节点 quicklistNode 中的地址, 实际上是 entry 在链表节点底层的压缩链表 zl 中的地址（和 quicklistIter.zi 字段含义相同）
    unsigned char *value;       // 如果数据节点 entry 中存储的是字符串编码的数据，那么 value 指向字符串的地址，sz 表示字符串的长度
    long long longval;          // 如果数据节点 entry 中存储的是整型编码的数据，longval 存储 entry 中的整型数
    unsigned int sz;
    int offset; // entry 在当前链表节点的 ziplist 中的 index. 和 quicklistIter.offset 字段含义相同
} quicklistEntry;

#define QUICKLIST_HEAD 0
#define QUICKLIST_TAIL -1

/* quicklist node encodings */
#define QUICKLIST_NODE_ENCODING_RAW 1
#define QUICKLIST_NODE_ENCODING_LZF 2

/* quicklist compression disable */
#define QUICKLIST_NOCOMPRESS 0

/* quicklist container formats */
#define QUICKLIST_NODE_CONTAINER_NONE 1
#define QUICKLIST_NODE_CONTAINER_ZIPLIST 2

#define quicklistNodeIsCompressed(node) \
    ((node)->encoding == QUICKLIST_NODE_ENCODING_LZF)

/* Prototypes */
quicklist *quicklistCreate(void);
quicklist *quicklistNew(int fill, int compress);
void quicklistSetCompressDepth(quicklist *quicklist, int depth);
void quicklistSetFill(quicklist *quicklist, int fill);
void quicklistSetOptions(quicklist *quicklist, int fill, int depth);
void quicklistRelease(quicklist *quicklist);
int quicklistPushHead(quicklist *quicklist, void *value, const size_t sz);
int quicklistPushTail(quicklist *quicklist, void *value, const size_t sz);
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where);
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl);
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist,
                                            unsigned char *zl);
quicklist *quicklistCreateFromZiplist(int fill, int compress,
                                      unsigned char *zl);
void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *node,
                          void *value, const size_t sz);
void quicklistInsertBefore(quicklist *quicklist, quicklistEntry *node,
                           void *value, const size_t sz);
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            int sz);
int quicklistDelRange(quicklist *quicklist, const long start, const long stop);
quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction);
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist,
                                         int direction, const long long idx);
int quicklistNext(quicklistIter *iter, quicklistEntry *node);
void quicklistReleaseIterator(quicklistIter *iter);
quicklist *quicklistDup(quicklist *orig);
int quicklistIndex(const quicklist *quicklist, const long long index,
                   quicklistEntry *entry);
void quicklistRewind(quicklist *quicklist, quicklistIter *li);
void quicklistRewindTail(quicklist *quicklist, quicklistIter *li);
void quicklistRotate(quicklist *quicklist);
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       unsigned int *sz, long long *sval,
                       void *(*saver)(unsigned char *data, unsigned int sz));
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 unsigned int *sz, long long *slong);
unsigned long quicklistCount(const quicklist *ql);
int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len);
size_t quicklistGetLzf(const quicklistNode *node, void **data);

/* bookmarks */
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node);
int quicklistBookmarkDelete(quicklist *ql, const char *name);
quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name);
void quicklistBookmarksClear(quicklist *ql);

#ifdef REDIS_TEST
int quicklistTest(int argc, char *argv[]);
#endif

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __QUICKLIST_H__ */
