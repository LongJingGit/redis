/* quicklist.c - A doubly linked list of ziplists
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must start the above copyright notice,
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

#include <string.h> /* for memcpy */
#include "redisassert.h"
#include "quicklist.h"
#include "zmalloc.h"
#include "ziplist.h"
#include "util.h" /* for ll2string */
#include "lzf.h"

#if defined(REDIS_TEST) || defined(REDIS_TEST_VERBOSE)
#include <stdio.h> /* for printf (debug printing), snprintf (genstr) */
#endif

#ifndef REDIS_STATIC
#define REDIS_STATIC static
#endif

/* Optimization levels for size-based filling.
 * Note that the largest possible limit is 16k, so even if each record takes
 * just one byte, it still won't overflow the 16 bit count field. */
// 装载因子为负值时的五档内存限制，分别对应着 -1 -2 -3 -4 -5
static const size_t optimization_level[] = {4096, 8192, 16384, 32768, 65536};

/* Maximum size in bytes of any multi-element ziplist.
 * Larger values will live in their own isolated ziplists.
 * This is used only if we're limited by record count. when we're limited by
 * size, the maximum limit is bigger, but still safe.
 * 8k is a recommended / default size limit */
#define SIZE_SAFETY_LIMIT 8192 // 压缩链表允许的最大长度，如果超过该大小，需要另一个链表节点来存储

/* Minimum ziplist size in bytes for attempting compression. */
#define MIN_COMPRESS_BYTES 48

/* Minimum size reduction in bytes to store compressed quicklistNode data.
 * This also prevents us from storing compression if the compression
 * resulted in a larger size than the original data. */
#define MIN_COMPRESS_IMPROVE 8

/* If not verbose testing, remove all debug printing. */
#ifndef REDIS_TEST_VERBOSE
#define D(...)
#else
#define D(...)                                                   \
    do                                                           \
    {                                                            \
        printf("%s:%s:%d:\t", __FILE__, __FUNCTION__, __LINE__); \
        printf(__VA_ARGS__);                                     \
        printf("\n");                                            \
    } while (0);
#endif

/* Bookmarks forward declarations */
#define QL_MAX_BM ((1 << QL_BM_BITS) - 1)
quicklistBookmark *_quicklistBookmarkFindByName(quicklist *ql, const char *name);
quicklistBookmark *_quicklistBookmarkFindByNode(quicklist *ql, quicklistNode *node);
void _quicklistBookmarkDelete(quicklist *ql, quicklistBookmark *bm);

/* Simple way to give quicklistEntry structs default values with one call. */
#define initEntry(e)                 \
    do                               \
    {                                \
        (e)->zi = (e)->value = NULL; \
        (e)->longval = -123456789;   \
        (e)->quicklist = NULL;       \
        (e)->node = NULL;            \
        (e)->offset = 123456789;     \
        (e)->sz = 0;                 \
    } while (0)

#if __GNUC__ >= 3
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

/* Create a new quicklist.
 * Free with quicklistRelease().
 * 创建一个快速链表
 */
quicklist *quicklistCreate(void)
{
    struct quicklist *quicklist;

    quicklist = zmalloc(sizeof(*quicklist));
    quicklist->head = quicklist->tail = NULL;
    quicklist->len = 0;
    quicklist->count = 0;
    quicklist->compress = 0;
    quicklist->fill = -2;
    quicklist->bookmark_count = 0;
    return quicklist;
}

// 设置压缩深度
#define COMPRESS_MAX ((1 << QL_COMP_BITS) - 1) // 压缩深度的上限
void quicklistSetCompressDepth(quicklist *quicklist, int compress)
{
    if (compress > COMPRESS_MAX)
    {
        compress = COMPRESS_MAX;
    }
    else if (compress < 0)
    {
        compress = 0;
    }
    quicklist->compress = compress;
}

// 设置装载 因子
#define FILL_MAX ((1 << (QL_FILL_BITS - 1)) - 1) // 装载因子的上限（因为 fill 为 16 位）
void quicklistSetFill(quicklist *quicklist, int fill)
{
    if (fill > FILL_MAX)
    {
        fill = FILL_MAX;
    }
    else if (fill < -5)
    {
        fill = -5;
    }
    quicklist->fill = fill;
}

// 设置 quicklist 的压缩深度和装载因子
void quicklistSetOptions(quicklist *quicklist, int fill, int depth)
{
    quicklistSetFill(quicklist, fill);
    quicklistSetCompressDepth(quicklist, depth);
}

/* Create a new quicklist with some default parameters. */
quicklist *quicklistNew(int fill, int compress)
{
    quicklist *quicklist = quicklistCreate();
    quicklistSetOptions(quicklist, fill, compress);
    return quicklist;
}

// 创建一个快速链表的链表节点 quicklistNode
REDIS_STATIC quicklistNode *quicklistCreateNode(void)
{
    quicklistNode *node;
    node = zmalloc(sizeof(*node));
    node->zl = NULL;        // 链表节点底层的压缩链表需要单独创建
    node->count = 0;
    node->sz = 0;
    node->next = node->prev = NULL;
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;
    node->container = QUICKLIST_NODE_CONTAINER_ZIPLIST;
    node->recompress = 0;
    return node;
}

/* Return cached quicklist count */
unsigned long quicklistCount(const quicklist *ql) { return ql->count; }

/* Free entire quicklist. */
// 释放一个快速链表: 包含每个链表节点中的压缩链表, 每个链表节点, 以及最后释放整个快速链表
void quicklistRelease(quicklist *quicklist)
{
    unsigned long len;
    quicklistNode *current, *next;

    current = quicklist->head;
    len = quicklist->len;
    while (len--)
    {
        next = current->next;

        zfree(current->zl); // 释放链表节点中的压缩链表
        quicklist->count -= current->count;

        zfree(current); // 释放链表节点

        quicklist->len--;
        current = next;
    }
    quicklistBookmarksClear(quicklist);
    zfree(quicklist); // 释放整个快速链表
}

/* Compress the ziplist in 'node' and update encoding details.
 * Returns 1 if ziplist compressed successfully.
 * Returns 0 if compression failed or if ziplist too small to compress.
 *
 * 压缩快速链表节点 quicklistNode，实质上压缩的是 链表节点 quicklistNode 中的压缩链表 ziplist
 */
REDIS_STATIC int __quicklistCompressNode(quicklistNode *node)
{
#ifdef REDIS_TEST
    node->attempted_compress = 1;
#endif

    /* Don't bother compressing small values */
    if (node->sz < MIN_COMPRESS_BYTES) // 当前链表节点中的压缩链表的长度小于 48 字节，则不需要压缩
        return 0;

    quicklistLZF *lzf = zmalloc(sizeof(*lzf) + node->sz); // 按照原始的 ziplist 进行内存分配

    /* Cancel if compression fails or doesn't compress small enough */
    // lzf->sz + MIN_COMPRESS_IMPROVE >= node->sz：压缩后数据如果比压缩之前的数据还要大，则不需要压缩
    // node->zl: 链表节点中的压缩链表 ziplist
    // node->sz: 链表节点中的压缩链表 ziplist 的长度
    if (((lzf->sz = lzf_compress(node->zl, node->sz, lzf->compressed, node->sz)) == 0) || lzf->sz + MIN_COMPRESS_IMPROVE >= node->sz)
    {
        /* lzf_compress aborts/rejects compression if value not compressable. */
        zfree(lzf);
        return 0;
    }

    lzf = zrealloc(lzf, sizeof(*lzf) + lzf->sz); // 重新分配内存，按照压缩后的 ziplist 的大小进行内存分配
    zfree(node->zl);       // 释放链表节点上原来未经压缩的压缩链表
    node->zl = (unsigned char *)lzf;             // 将经过压缩的 ziplist 绑定到链表节点的 zl 指针上
    node->encoding = QUICKLIST_NODE_ENCODING_LZF;   // 更改编码，表示该节点是压缩节点
    node->recompress = 0;
    return 1;
}

/* Compress only uncompressed nodes. */
// 压缩一个链表节点: 实际上压缩的是链表节点底层的压缩链表 zl
#define quicklistCompressNode(_node)                                     \
    do                                                                   \
    {                                                                    \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_RAW) \
        {                                                                \
            __quicklistCompressNode((_node));                            \
        }                                                                \
    } while (0)

/* Uncompress the ziplist in 'node' and update encoding details.
 * Returns 1 on successful decode, 0 on failure to decode.
 *
 * 对链表节点 quicklistNode 中的压缩链表 ziplist 进行解压
 */
REDIS_STATIC int __quicklistDecompressNode(quicklistNode *node)
{
#ifdef REDIS_TEST
    node->attempted_compress = 0;
#endif

    void *decompressed = zmalloc(node->sz); // 按照原始的压缩链表的大小分配内存空间
    quicklistLZF *lzf = (quicklistLZF *)node->zl;
    if (lzf_decompress(lzf->compressed, lzf->sz, decompressed, node->sz) == 0)
    {
        /* Someone requested decompress, but we can't decompress.  Not good. */
        zfree(decompressed);
        return 0;
    }

    zfree(lzf);              // 释放原来的压缩数据
    node->zl = decompressed; // 将解压后的数据绑定到链表节点的压缩链表指针 zl 上
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;   // 更新编码方式，表示该节点是未经压缩的节点
    return 1;
}

/* Decompress only compressed nodes. */
// 解压一个链表节点 _node
#define quicklistDecompressNode(_node)                                   \
    do                                                                   \
    {                                                                    \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) \
        {                                                                \
            __quicklistDecompressNode((_node));                          \
        }                                                                \
    } while (0)

/* Force node to not be immediately re-compresable */
// 临时解压一个链表节点，后面需要调用 quicklistRecompressOnly 对该节点重新进行压缩
#define quicklistDecompressNodeForUse(_node)                             \
    do                                                                   \
    {                                                                    \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) \
        {                                                                \
            __quicklistDecompressNode((_node));                          \
            (_node)->recompress = 1;                                     \
        }                                                                \
    } while (0)

/* Extract the raw LZF data from this quicklistNode.
 * Pointer to LZF data is assigned to '*data'.
 * Return value is the length of compressed LZF data. */
size_t quicklistGetLzf(const quicklistNode *node, void **data)
{
    quicklistLZF *lzf = (quicklistLZF *)node->zl;
    *data = lzf->compressed;
    return lzf->sz;
}

// 判断是否允许压缩
#define quicklistAllowsCompression(_ql) ((_ql)->compress != 0)

/* Force 'quicklist' to meet compression guidelines set by compress depth.
 * The only way to guarantee interior nodes get compressed is to iterate
 * to our "interior" compress depth then compress the next node we find.
 * If compress depth is larger than the entire list, we return immediately.
 *
 * 根据压缩深度，对链表节点进行解压
 *
 * 如果传入的 node 不为空，需要判断这个节点是否在压缩深度之内，如果这个节点不是处于压缩深度中的节点，那么对这个节点执行压缩操作。
 */
REDIS_STATIC void __quicklistCompress(const quicklist *quicklist, quicklistNode *node)
{
    /* If length is less than our compress depth (from both sides),
     * we can't compress anything. */
    if (!quicklistAllowsCompression(quicklist) || quicklist->len < (unsigned int)(quicklist->compress * 2))
        return;

#if 0
    /* Optimized cases for small depth counts */
    if (quicklist->compress == 1) {
        quicklistNode *h = quicklist->head, *t = quicklist->tail;
        quicklistDecompressNode(h);
        quicklistDecompressNode(t);
        if (h != node && t != node)
            quicklistCompressNode(node);
        return;
    } else if (quicklist->compress == 2) {
        quicklistNode *h = quicklist->head, *hn = h->next, *hnn = hn->next;
        quicklistNode *t = quicklist->tail, *tp = t->prev, *tpp = tp->prev;
        quicklistDecompressNode(h);
        quicklistDecompressNode(hn);
        quicklistDecompressNode(t);
        quicklistDecompressNode(tp);
        if (h != node && hn != node && t != node && tp != node) {
            quicklistCompressNode(node);
        }
        if (hnn != t) {
            quicklistCompressNode(hnn);
        }
        if (tpp != h) {
            quicklistCompressNode(tpp);
        }
        return;
    }
#endif

    /* Iterate until we reach compress depth for both sides of the list.a
     * Note: because we do length checks at the *top* of this function,
     *       we can skip explicit null checks below. Everything exists. */
    quicklistNode *forward = quicklist->head;
    quicklistNode *reverse = quicklist->tail;
    int depth = 0;
    int in_depth = 0;
    while (depth++ < quicklist->compress)
    {
        quicklistDecompressNode(forward);
        quicklistDecompressNode(reverse);

        if (forward == node || reverse == node)
            in_depth = 1;

        if (forward == reverse)
            return;

        forward = forward->next;
        reverse = reverse->prev;
    }

    if (!in_depth)
        quicklistCompressNode(node);

    // depth 是从 0 开始计算的，所以两端多压缩了一个节点
    if (depth > 2)
    {
        /* At this point, forward and reverse are one node beyond depth */
        quicklistCompressNode(forward);
        quicklistCompressNode(reverse);
    }
}

#define quicklistCompress(_ql, _node)            \
    do                                           \
    {                                            \
        if ((_node)->recompress)                 \
            quicklistCompressNode((_node));      \
        else                                     \
            __quicklistCompress((_ql), (_node)); \
    } while (0)

/* If we previously used quicklistDecompressNodeForUse(), just recompress. */
// 对临时解压的节点重新进行压缩
#define quicklistRecompressOnly(_ql, _node) \
    do                                      \
    {                                       \
        if ((_node)->recompress)            \
            quicklistCompressNode((_node)); \
    } while (0)

/* Insert 'new_node' after 'old_node' if 'after' is 1.
 * Insert 'new_node' before 'old_node' if 'after' is 0.
 * Note: 'new_node' is *always* uncompressed, so if we assign it to
 *       head or tail, we do not need to uncompress it.
 *
 * 向快速链表 quicklist 中插入一个链表节点 new_node
 * 1. 如果 after 为 1, 则将 new_node 插入到 old_node 节点的后面
 * 2. 如果 after 为 0, 则将 new_node 插入到 old_node 节点的前面
 *
 * NOTE:
 * 1. 待插入节点 new_node 应该是一个未压缩的节点.
 * 2. 如果插入到头部或者尾部，插入之后不需要进行压缩; 如果插入到其他位置, 则在插入之后需要调用压缩函数对节点进行压缩
 */
REDIS_STATIC void __quicklistInsertNode(quicklist *quicklist,
                                        quicklistNode *old_node,
                                        quicklistNode *new_node, int after)
{
    // 插入 old_node 的后面
    if (after)
    {
        new_node->prev = old_node;
        if (old_node)
        {
            new_node->next = old_node->next;
            if (old_node->next) // 如果 old_node 不是尾结点, 需要更新 old_node 节点的下一个节点的 prev 指针
                old_node->next->prev = new_node;
            old_node->next = new_node;
        }

        if (quicklist->tail == old_node)    // 如果 old_node 是尾结点, 需要更新尾结点为 new_node
            quicklist->tail = new_node;
    }
    else
    {
        new_node->next = old_node;
        if (old_node)
        {
            new_node->prev = old_node->prev;
            if (old_node->prev)
                old_node->prev->next = new_node;
            old_node->prev = new_node;
        }
        if (quicklist->head == old_node)
            quicklist->head = new_node;
    }
    /* If this insert creates the only element so far, initialize head/tail. */
    if (quicklist->len == 0)
    {
        quicklist->head = quicklist->tail = new_node;
    }

    // 如果插入节点的位置不是头部和尾部
    if (old_node)
        quicklistCompress(quicklist, old_node); // FIXME: 为什么不压缩 new_node

    quicklist->len++;
}

/* Wrappers for node inserting around existing node. */
REDIS_STATIC void _quicklistInsertNodeBefore(quicklist *quicklist,
                                             quicklistNode *old_node,
                                             quicklistNode *new_node)
{
    __quicklistInsertNode(quicklist, old_node, new_node, 0);
}

REDIS_STATIC void _quicklistInsertNodeAfter(quicklist *quicklist,
                                            quicklistNode *old_node,
                                            quicklistNode *new_node)
{
    __quicklistInsertNode(quicklist, old_node, new_node, 1);
}

/**
 * quicklist 定义了装载因子 fill 这个字段，用来对每个链表节点的底层压缩链表的内存大小或者节点长度进行了限制，
 * 因此在向某个链表节点插入数据节点的时候，需要预先进行判断，新的节点插入之后是否满足装载因子的限制。
 *
 * 该接口主要用来判断：新的节点插入之后，ziplist 的内存大小是否满足装载因子的要求
 *
 * fill: 装载因子
 * sz: ziplist 的内存大小
 */
REDIS_STATIC int _quicklistNodeSizeMeetsOptimizationRequirement(const size_t sz,
                                                                const int fill)
{
    // 只判断内存大小是否符合要求，所以要求装载因子小于 0
    if (fill >= 0)
        return 0;

    size_t offset = (-fill) - 1;
    if (offset < (sizeof(optimization_level) / sizeof(*optimization_level)))
    {
        if (sz <= optimization_level[offset])
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
    else
    {
        return 0;
    }
}

#define sizeMeetsSafetyLimit(sz) ((sz) <= SIZE_SAFETY_LIMIT)

/**
 * node: 待插入的链表节点
 * fill: 这个快速链表的装载因子
 * sz: 要插入数据的内存大小。插入一个 entry 节点: <prevlen><encoding><entry-data>
 *
 * 判断是否允许向链表节点 node 中插入数据长度为 sz 的数据
 */
REDIS_STATIC int _quicklistNodeAllowInsert(const quicklistNode *node,
                                           const int fill, const size_t sz)
{
    if (unlikely(!node))
        return 0;

    int ziplist_overhead;
    /* size of previous offset */
    if (sz < 254) // 计算 <prevlen> 字段需要的长度
        ziplist_overhead = 1;
    else
        ziplist_overhead = 5;

    /* size of forward offset */
    if (sz < 64) // 计算 <encoding> 字段需要的长度
        ziplist_overhead += 1;
    else if (likely(sz < 16384))
        ziplist_overhead += 2;
    else
        ziplist_overhead += 5;

    /* new_sz overestimates if 'sz' encodes to an integer type */
    unsigned int new_sz = node->sz + sz + ziplist_overhead; // 计算插入数据节点 entry 之后，压缩链表的内存大小

    if (likely(_quicklistNodeSizeMeetsOptimizationRequirement(new_sz, fill))) // 判断插入新节点之后，压缩链表的内存大小是否满足装载因子的要求
        return 1;
    /* when we return 1 above we know that the limit is a size limit (which is
     * safe, see comments next to optimization_level and SIZE_SAFETY_LIMIT) */
    /**
     * 上面 if 条件判断返回 0 的两种情况：
     * 1. fill 限制的是节点数量：需要再次判断新的 ziplist 的大小是否超过了最大限值，然后再判断节点数量是否符合装载因子的限制
     * 2. 插入新节点之后，ziplist 的内存大小超过了 fill 的限制：再次判断新的 ziplist 是否超过了最大限值？？？（FIXME: 意义是什么？）
     */
    else if (!sizeMeetsSafetyLimit(new_sz)) // 压缩链表的内存大小符合要求
        return 0;
    /**
     * 装载因子限制的是节点数量，判断节点数量是否满足装载因子的限制. 有两种判断方式：
     * 1. node->count < fill
     * 2. node->count + 1 <= fill
     */
    else if ((int)node->count < fill)
        return 1;
    else
        return 0;
}

/**
 * 在 quicklist 的操作过程中，会尝试对两个相邻的链表节点进行合并，合并之后的链表节点必须满足装载因子的限制
 *
 * 判断两个链表节点合并之后是否满足装载因子的限制。
 */
REDIS_STATIC int _quicklistNodeAllowMerge(const quicklistNode *a,
                                          const quicklistNode *b,
                                          const int fill)
{
    if (!a || !b)
        return 0;

    /* approximate merged ziplist size (- 11 to remove one ziplist
     * header/trailer) */
    unsigned int merge_sz = a->sz + b->sz - 11; // ziplist 的头部和尾部 <zlbytes><zltail><zllen><zlend> 总长度为 11 字节
    if (likely(_quicklistNodeSizeMeetsOptimizationRequirement(merge_sz, fill)))
        return 1;
    /* when we return 1 above we know that the limit is a size limit (which is
     * safe, see comments next to optimization_level and SIZE_SAFETY_LIMIT) */
    // 如果上面的关于内存的限制返回 0, 可能是因为装载因子限制的不是内存大小，所以需要判断节点个数是否满足要求，但是内存仍然会有限制
    else if (!sizeMeetsSafetyLimit(merge_sz))
        return 0;
    else if ((int)(a->count + b->count) <= fill)    // 判断节点个数是否满足装载因子的限制
        return 1;
    else
        return 0;
}

// 更新链表节点 quicklistNode.sz
#define quicklistNodeUpdateSz(node)              \
    do                                           \
    {                                            \
        (node)->sz = ziplistBlobLen((node)->zl); \
    } while (0)

/* Add new entry to head node of quicklist.
 *
 * Returns 0 if used existing head.
 * Returns 1 if new head created. */
/**
 * 在快速链表的头结点中插入长度为 sz 的数据 value; 如果不能插入头节点，则创建一个新的链表节点，然后将 value 插入新的链表节点
 * 如果插入头节点, 返回 0; 如果插入的是新创建的节点, 返回 1
 */
int quicklistPushHead(quicklist *quicklist, void *value, size_t sz)
{
    quicklistNode *orig_head = quicklist->head;
    assert(sz < UINT32_MAX); /* TODO: add support for quicklist nodes that are sds encoded (not zipped) */
    if (likely(_quicklistNodeAllowInsert(quicklist->head, quicklist->fill, sz)))    // 判断头结点是否允许插入
    {
        // 头节点允许插入，则将数据 value 插入到头结点的压缩链表 zl 的头部
        quicklist->head->zl = ziplistPush(quicklist->head->zl, value, sz, ZIPLIST_HEAD);
        quicklistNodeUpdateSz(quicklist->head);     // 更新链表节点中压缩链表的长度 sz
    }
    else
    {
        // 头节点不允许插入, 则创建一个新的节点
        quicklistNode *node = quicklistCreateNode();    // 创建一个新的节点
        node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);  // 将 value 插入到 ziplist 的头部

        quicklistNodeUpdateSz(node);
        _quicklistInsertNodeBefore(quicklist, quicklist->head, node); // 将新的链表节点 node 插入到链表头结点的后面
    }

    quicklist->count++;
    quicklist->head->count++;
    return (orig_head != quicklist->head);
}

/* Add new entry to tail node of quicklist.
 *
 * Returns 0 if used existing tail.
 * Returns 1 if new tail created. */
/**
 * 在快速节点的尾结点插入长度为 sz 的数据 value; 如果不能插入尾节点，则创建一个新的链表节点，然后将 value 插入新的链表节点
 * 如果插入尾节点, 返回 0; 如果插入的是新创建的节点, 返回 1
 */
int quicklistPushTail(quicklist *quicklist, void *value, size_t sz)
{
    quicklistNode *orig_tail = quicklist->tail;
    assert(sz < UINT32_MAX); /* TODO: add support for quicklist nodes that are sds encoded (not zipped) */
    if (likely(_quicklistNodeAllowInsert(quicklist->tail, quicklist->fill, sz)))
    {
        // 向链表尾节点的底层压缩链表插入长度为 sz 的数据 value
        quicklist->tail->zl = ziplistPush(quicklist->tail->zl, value, sz, ZIPLIST_TAIL);
        quicklistNodeUpdateSz(quicklist->tail);
    }
    else
    {
        quicklistNode *node = quicklistCreateNode();
        node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_TAIL);

        quicklistNodeUpdateSz(node);
        _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
    }

    quicklist->count++;
    quicklist->tail->count++;
    return (orig_tail != quicklist->tail);
}

/* Create new node consisting of a pre-formed ziplist.
 * Used for loading RDBs where entire ziplists have been stored
 * to be retrieved later. */
// 将压缩链表 zl 整体插入到快速链表
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl)
{
    quicklistNode *node = quicklistCreateNode();    // 创建一个新的链表节点

    node->zl = zl;  // 将压缩链表 zl 绑定到新创建链表节点的 zl 指针上
    node->count = ziplistLen(node->zl);
    node->sz = ziplistBlobLen(zl);

    _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);    // 将新创建的节点 node 插入到快速链表尾结点的后面
    quicklist->count += node->count;
}

/* Append all values of ziplist 'zl' individually into 'quicklist'.
 *
 * This allows us to restore old RDB ziplists into new quicklists
 * with smaller ziplist sizes than the saved RDB ziplist.
 *
 * Returns 'quicklist' argument. Frees passed-in ziplist 'zl' */
// 将压缩链表 zl 中的每一个数据节点依次插入到快速链表的尾结点
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist,
                                            unsigned char *zl)
{
    unsigned char *value;
    unsigned int sz;
    long long longval;
    char longstr[32] = {0};

    unsigned char *p = ziplistIndex(zl, 0);
    while (ziplistGet(p, &value, &sz, &longval))
    {
        if (!value)
        {
            /* Write the longval as a string so we can re-add it */
            sz = ll2string(longstr, sizeof(longstr), longval);
            value = (unsigned char *)longstr;
        }
        quicklistPushTail(quicklist, value, sz);
        p = ziplistNext(zl, p);
    }
    zfree(zl);
    return quicklist;
}

/* Create new (potentially multi-node) quicklist from a single existing ziplist.
 *
 * Returns new quicklist.  Frees passed-in ziplist 'zl'. */
// 使用压缩链表 zl 来创建一个快速链表
quicklist *quicklistCreateFromZiplist(int fill, int compress,
                                      unsigned char *zl)
{
    return quicklistAppendValuesFromZiplist(quicklistNew(fill, compress), zl);
}

#define quicklistDeleteIfEmpty(ql, n)      \
    do                                     \
    {                                      \
        if ((n)->count == 0)               \
        {                                  \
            __quicklistDelNode((ql), (n)); \
            (n) = NULL;                    \
        }                                  \
    } while (0)

// 删除链表节点 node
REDIS_STATIC void __quicklistDelNode(quicklist *quicklist,
                                     quicklistNode *node)
{
    /* Update the bookmark if any */
    quicklistBookmark *bm = _quicklistBookmarkFindByNode(quicklist, node);
    if (bm)
    {
        bm->node = node->next;
        /* if the bookmark was to the last node, delete it. */
        if (!bm->node)
            _quicklistBookmarkDelete(quicklist, bm);
    }

    if (node->next)
        node->next->prev = node->prev;
    if (node->prev)
        node->prev->next = node->next;

    if (node == quicklist->tail)
    {
        quicklist->tail = node->prev;
    }

    if (node == quicklist->head)
    {
        quicklist->head = node->next;
    }

    /* If we deleted a node within our compress depth, we
     * now have compressed nodes needing to be decompressed. */
    __quicklistCompress(quicklist, NULL);

    quicklist->count -= node->count;

    zfree(node->zl);
    zfree(node);
    quicklist->len--;
}

/* Delete one entry from list given the node for the entry and a pointer
 * to the entry in the node.
 *
 * Note: quicklistDelIndex() *requires* uncompressed nodes because you
 *       already had to get *p from an uncompressed node somewhere.
 *
 * Returns 1 if the entire node was deleted, 0 if node still exists.
 * Also updates in/out param 'p' with the next offset in the ziplist.
 *
 * 删除链表节点 node 中指针 p 指向的数据节点
 */
REDIS_STATIC int quicklistDelIndex(quicklist *quicklist, quicklistNode *node,
                                   unsigned char **p)
{
    int gone = 0;

    node->zl = ziplistDelete(node->zl, p);
    node->count--;
    if (node->count == 0)
    {
        gone = 1;
        __quicklistDelNode(quicklist, node);    // 如果链表节点中数据节点为 0, 删除这个链表节点
    }
    else
    {
        quicklistNodeUpdateSz(node);    // 更新 quicklistNode.sz
    }
    quicklist->count--;
    /* If we deleted the node, the original node is no longer valid */
    return gone ? 1 : 0;
}

/* Delete one element represented by 'entry'
 *
 * 'entry' stores enough metadata to delete the proper position in
 * the correct ziplist in the correct quicklist node.
 *
 * 从快速链表中删除 entry 这个数据节点
 */
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry)
{
    quicklistNode *prev = entry->node->prev;
    quicklistNode *next = entry->node->next;
    int deleted_node = quicklistDelIndex((quicklist *)entry->quicklist, entry->node, &entry->zi);

    /* after delete, the zi is now invalid for any future usage. */
    iter->zi = NULL;

    /* If current node is deleted, we must update iterator node and offset. */
    if (deleted_node)
    {
        if (iter->direction == AL_START_HEAD)
        {
            iter->current = next;
            iter->offset = 0;
        }
        else if (iter->direction == AL_START_TAIL)
        {
            iter->current = prev;
            iter->offset = -1;
        }
    }
    /* else if (!deleted_node), no changes needed.
     * we already reset iter->zi above, and the existing iter->offset
     * doesn't move again because:
     *   - [1, 2, 3] => delete offset 1 => [1, 3]: next element still offset 1
     *   - [1, 2, 3] => delete offset 0 => [2, 3]: next element still offset 0
     *  if we deleted the last element at offet N and now
     *  length of this ziplist is N-1, the next call into
     *  quicklistNext() will jump to the next node. */
}

/* Replace quicklist entry at offset 'index' by 'data' with length 'sz'.
 *
 * Returns 1 if replace happened.
 * Returns 0 if replace failed and no changes happened.
 *
 * 将快速链表中 index 索引对应的数据节点中的数据使用 data 和 sz 进行替换
 */
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data, int sz)
{
    quicklistEntry entry;
    if (likely(quicklistIndex(quicklist, index, &entry)))
    {
        /* quicklistIndex provides an uncompressed node */
        entry.node->zl = ziplistDelete(entry.node->zl, &entry.zi);          // 从压缩链表中删除数据节点
        entry.node->zl = ziplistInsert(entry.node->zl, entry.zi, data, sz); // 在删除的位置重新插入新的数据节点
        quicklistNodeUpdateSz(entry.node);                                  // 获取当前链表节点中压缩链表的新的大小
        quicklistCompress(quicklist, entry.node);                           // 重新压缩该链表节点（在 quicklistIndex 中进行了解压）
        return 1;
    }
    else
    {
        return 0;
    }
}

/* Given two nodes, try to merge their ziplists.
 *
 * This helps us not have a quicklist with 3 element ziplists if
 * our fill factor can handle much higher levels.
 *
 * Note: 'a' must be to the LEFT of 'b'.
 *
 * After calling this function, both 'a' and 'b' should be considered
 * unusable.  The return value from this function must be used
 * instead of re-using any of the quicklistNode input arguments.
 *
 * Returns the input node picked to merge against or NULL if
 * merging was not possible.
 *
 * 合并链表节点 a 和 b 的底层的压缩链表, 返回合并后的链表节点. 但是并没有对合并之后的合法性进行校验
 */
REDIS_STATIC quicklistNode *_quicklistZiplistMerge(quicklist *quicklist,
                                                   quicklistNode *a,
                                                   quicklistNode *b)
{
    D("Requested merge (a,b) (%u, %u)", a->count, b->count);

    quicklistDecompressNode(a);
    quicklistDecompressNode(b);
    if ((ziplistMerge(&a->zl, &b->zl))) // 对压缩链表 a->zl 和 b->zl 进行合并（将较短的 zl 合并到较长的 zl，然后释放较短的 zl）
    {
        /* We merged ziplists! Now remove the unused quicklistNode. */
        quicklistNode *keep = NULL, *nokeep = NULL;
        if (!a->zl) // 如果节点 a 底层的压缩链表 zl 被释放了, 说明留下的是 b
        {
            nokeep = a;
            keep = b;
        }
        else if (!b->zl)
        {
            nokeep = b;
            keep = a;
        }

        keep->count = ziplistLen(keep->zl); // 计算 quicklistNode.count
        quicklistNodeUpdateSz(keep);        // 计算 quicklistNode.sz

        nokeep->count = 0;
        __quicklistDelNode(quicklist, nokeep); // 删除被合并的链表节点
        quicklistCompress(quicklist, keep);    // 重新压缩被保留的节点
        return keep;
    }
    else
    {
        /* else, the merge returned NULL and nothing changed. */
        return NULL;
    }
}

/* Attempt to merge ziplists within two nodes on either side of 'center'.
 *
 * We attempt to merge:
 *   - (center->prev->prev, center->prev)
 *   - (center->next, center->next->next)
 *   - (center->prev, center)
 *   - (center, center->next)
 *
 * 尝试将快速链表 quicklist 中链表节点 center 以及该节点左右两边各两个链表节点一共五个节点合并成一个链表节点
 */
REDIS_STATIC void _quicklistMergeNodes(quicklist *quicklist, quicklistNode *center)
{
    int fill = quicklist->fill;
    quicklistNode *prev, *prev_prev, *next, *next_next, *target;
    prev = prev_prev = next = next_next = target = NULL;

    if (center->prev)
    {
        prev = center->prev;
        if (center->prev->prev)
            prev_prev = center->prev->prev;
    }

    if (center->next)
    {
        next = center->next;
        if (center->next->next)
            next_next = center->next->next;
    }

    /* Try to merge prev_prev and prev */
    // 尝试合并 prev 和 prev_prev 这两个节点
    if (_quicklistNodeAllowMerge(prev, prev_prev, fill))
    {
        _quicklistZiplistMerge(quicklist, prev_prev, prev);
        prev_prev = prev = NULL; /* they could have moved, invalidate them. */
    }

    /* Try to merge next and next_next */
    // 尝试合并 next 和 next_next 这两个节点
    if (_quicklistNodeAllowMerge(next, next_next, fill))
    {
        _quicklistZiplistMerge(quicklist, next, next_next);
        next = next_next = NULL; /* they could have moved, invalidate them. */
    }

    /* Try to merge center node and previous node */
    // 尝试合并 center 和 prev 这两个节点
    if (_quicklistNodeAllowMerge(center, center->prev, fill))
    {
        target = _quicklistZiplistMerge(quicklist, center->prev, center);
        center = NULL; /* center could have been deleted, invalidate it. */
    }
    else
    {
        /* else, we didn't merge here, but target needs to be valid below. */
        target = center;
    }

    /* Use result of center merge (or original) to merge with next node. */
    // 尝试合并 target 和 target->next 这两个节点
    if (_quicklistNodeAllowMerge(target, target->next, fill))
    {
        _quicklistZiplistMerge(quicklist, target, target->next);
    }
}

/* Split 'node' into two parts, parameterized by 'offset' and 'after'.
 *
 * The 'after' argument controls which quicklistNode gets returned.
 * If 'after'==1, returned node has elements after 'offset'.
 *                input node keeps elements up to 'offset', including 'offset'.
 * If 'after'==0, returned node has elements up to 'offset', including 'offset'.
 *                input node keeps elements after 'offset'.
 *
 * If 'after'==1, returned node will have elements _after_ 'offset'.
 *                The returned node will have elements [OFFSET+1, END].
 *                The input node keeps elements [0, OFFSET].
 *
 * If 'after'==0, returned node will keep elements up to and including 'offset'.
 *                The returned node will have elements [0, OFFSET].
 *                The input node keeps elements [OFFSET+1, END].
 *
 * The input node keeps all elements not taken by the returned node.
 *
 * Returns newly created node or NULL if split not possible.
 *
 * 将一个链表节点拆分成两个链表节点: 从链表节点 node 中的压缩链表 offset 位置，将该压缩链表拆分成两个链表节点，返回被拆分出来的链表节点
 *
 * 1. 如果 after 为 1, 原 node 节点中保留了压缩链表 zl 的 [0, offset] 位置的数据节点, 拆分出来的节点为压缩链表中 [offset+1, END] 位置的数据节点
 * 2. 如果 after 为 0, 原 node 节点中保留了压缩链表 zl 的 [offset+1, END] 位置的数据节点, 拆分出来的节点为压缩链表 zl 中 [0, offset] 位置的数据节点
 */
REDIS_STATIC quicklistNode *_quicklistSplitNode(quicklistNode *node, int offset, int after)
{
    size_t zl_sz = node->sz;

    quicklistNode *new_node = quicklistCreateNode();
    new_node->zl = zmalloc(zl_sz);

    /* Copy original ziplist so we can split it */
    memcpy(new_node->zl, node->zl, zl_sz);

    /* -1 here means "continue deleting until the list ends" */
    int orig_start = after ? offset + 1 : 0;
    int orig_extent = after ? -1 : offset;
    int new_start = after ? 0 : offset;
    int new_extent = after ? offset + 1 : -1;

    D("After %d (%d); ranges: [%d, %d], [%d, %d]", after, offset, orig_start,
      orig_extent, new_start, new_extent);

    node->zl = ziplistDeleteRange(node->zl, orig_start, orig_extent);
    node->count = ziplistLen(node->zl);
    quicklistNodeUpdateSz(node);

    new_node->zl = ziplistDeleteRange(new_node->zl, new_start, new_extent);
    new_node->count = ziplistLen(new_node->zl);
    quicklistNodeUpdateSz(new_node);

    D("After split lengths: orig (%d), new (%d)", node->count, new_node->count);
    return new_node;
}

/* Insert a new entry before or after existing entry 'entry'.
 *
 * If after==1, the new value is inserted after 'entry', otherwise
 * the new value is inserted before 'entry'.
 *
 * 向快速链表 quicklist 的指定数据节点 entry 的前面或者后面插入一个新的数据节点. 数据节点的值为 value, 数据长度为 sz.
 * 如果 after == 1, 插入到 entry 的后面, 否则插入到 entry 的前面
 */
REDIS_STATIC void _quicklistInsert(quicklist *quicklist, quicklistEntry *entry,
                                   void *value, const size_t sz, int after)
{
    int full = 0, at_tail = 0, at_head = 0, full_next = 0, full_prev = 0;
    int fill = quicklist->fill;
    quicklistNode *node = entry->node;
    quicklistNode *new_node = NULL;
    assert(sz < UINT32_MAX); /* TODO: add support for quicklist nodes that are sds encoded (not zipped) */

    if (!node)
    {
        /* we have no reference node, so let's create only node in the list */
        D("No node given!");
        new_node = quicklistCreateNode();
        new_node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
        __quicklistInsertNode(quicklist, NULL, new_node, after);
        new_node->count++;
        quicklist->count++;
        return;
    }

    /* Populate accounting flags for easier boolean checks later */
    if (!_quicklistNodeAllowInsert(node, fill, sz))
    {
        D("Current node is full with count %d with requested fill %lu",
          node->count, fill);
        full = 1;
    }

    if (after && (entry->offset == node->count))
    {
        D("At Tail of current ziplist");
        at_tail = 1;
        if (!_quicklistNodeAllowInsert(node->next, fill, sz))
        {
            D("Next node is full too.");
            full_next = 1;
        }
    }

    if (!after && (entry->offset == 0))
    {
        D("At Head");
        at_head = 1;
        if (!_quicklistNodeAllowInsert(node->prev, fill, sz))
        {
            D("Prev node is full too.");
            full_prev = 1;
        }
    }

    /* Now determine where and how to insert the new element */
    if (!full && after)
    {
        D("Not full, inserting after current position.");
        quicklistDecompressNodeForUse(node);
        unsigned char *next = ziplistNext(node->zl, entry->zi);
        if (next == NULL)
        {
            node->zl = ziplistPush(node->zl, value, sz, ZIPLIST_TAIL);
        }
        else
        {
            node->zl = ziplistInsert(node->zl, next, value, sz);
        }
        node->count++;
        quicklistNodeUpdateSz(node);
        quicklistRecompressOnly(quicklist, node);
    }
    else if (!full && !after)
    {
        D("Not full, inserting before current position.");
        quicklistDecompressNodeForUse(node);
        node->zl = ziplistInsert(node->zl, entry->zi, value, sz);
        node->count++;
        quicklistNodeUpdateSz(node);
        quicklistRecompressOnly(quicklist, node);
    }
    else if (full && at_tail && node->next && !full_next && after)
    {
        /* If we are: at tail, next has free space, and inserting after:
         *   - insert entry at head of next node. */
        D("Full and tail, but next isn't full; inserting next node head");
        new_node = node->next;
        quicklistDecompressNodeForUse(new_node);
        new_node->zl = ziplistPush(new_node->zl, value, sz, ZIPLIST_HEAD);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        quicklistRecompressOnly(quicklist, new_node);
    }
    else if (full && at_head && node->prev && !full_prev && !after)
    {
        /* If we are: at head, previous has free space, and inserting before:
         *   - insert entry at tail of previous node. */
        D("Full and head, but prev isn't full, inserting prev node tail");
        new_node = node->prev;
        quicklistDecompressNodeForUse(new_node);
        new_node->zl = ziplistPush(new_node->zl, value, sz, ZIPLIST_TAIL);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        quicklistRecompressOnly(quicklist, new_node);
    }
    else if (full && ((at_tail && node->next && full_next && after) ||
                      (at_head && node->prev && full_prev && !after)))
    {
        /* If we are: full, and our prev/next is full, then:
         *   - create new node and attach to quicklist */
        D("\tprovisioning new node...");
        new_node = quicklistCreateNode();
        new_node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        __quicklistInsertNode(quicklist, node, new_node, after);
    }
    else if (full)
    {
        /* else, node is full we need to split it. */
        /* covers both after and !after cases */
        D("\tsplitting node...");
        quicklistDecompressNodeForUse(node);
        new_node = _quicklistSplitNode(node, entry->offset, after);
        new_node->zl = ziplistPush(new_node->zl, value, sz,
                                   after ? ZIPLIST_HEAD : ZIPLIST_TAIL);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        __quicklistInsertNode(quicklist, node, new_node, after);
        _quicklistMergeNodes(quicklist, node);
    }

    quicklist->count++;
}

void quicklistInsertBefore(quicklist *quicklist, quicklistEntry *entry,
                           void *value, const size_t sz)
{
    _quicklistInsert(quicklist, entry, value, sz, 0);
}

void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *entry,
                          void *value, const size_t sz)
{
    _quicklistInsert(quicklist, entry, value, sz, 1);
}

/* Delete a range of elements from the quicklist.
 *
 * elements may span across multiple quicklistNodes, so we
 * have to be careful about tracking where we start and end.
 *
 * Returns 1 if entries were deleted, 0 if nothing was deleted.
 *
 * 从快速链表中删除从 start 开始连续 count 个数的数据节点，需要注意的是，被删除的多个数据节点有可能会跨越多个链表节点
 */
int quicklistDelRange(quicklist *quicklist, const long start, const long count)
{
    if (count <= 0)
        return 0;

    unsigned long extent = count; /* range is inclusive of start position */

    if (start >= 0 && extent > (quicklist->count - start))
    {
        /* if requesting delete more elements than exist, limit to list size. */
        extent = quicklist->count - start;
    }
    else if (start < 0 && extent > (unsigned long)(-start))
    {
        /* else, if at negative offset, limit max size to rest of list. */
        extent = -start; /* c.f. LREM -29 29; just delete until end. */
    }

    quicklistEntry entry;
    if (!quicklistIndex(quicklist, start, &entry))
        return 0;

    D("Quicklist delete request for start %ld, count %ld, extent: %ld", start,
      count, extent);
    quicklistNode *node = entry.node;

    /* iterate over next nodes until everything is deleted. */
    while (extent)
    {
        quicklistNode *next = node->next;

        unsigned long del;
        int delete_entire_node = 0;
        if (entry.offset == 0 && extent >= node->count)
        {
            /* If we are deleting more than the count of this node, we
             * can just delete the entire node without ziplist math. */
            delete_entire_node = 1;
            del = node->count;
        }
        else if (entry.offset >= 0 && extent >= node->count)
        {
            /* If deleting more nodes after this one, calculate delete based
             * on size of current node. */
            del = node->count - entry.offset;
        }
        else if (entry.offset < 0)
        {
            /* If offset is negative, we are in the first run of this loop
             * and we are deleting the entire range
             * from this start offset to end of list.  Since the Negative
             * offset is the number of elements until the tail of the list,
             * just use it directly as the deletion count. */
            del = -entry.offset;

            /* If the positive offset is greater than the remaining extent,
             * we only delete the remaining extent, not the entire offset.
             */
            if (del > extent)
                del = extent;
        }
        else
        {
            /* else, we are deleting less than the extent of this node, so
             * use extent directly. */
            del = extent;
        }

        D("[%ld]: asking to del: %ld because offset: %d; (ENTIRE NODE: %d), "
          "node count: %u",
          extent, del, entry.offset, delete_entire_node, node->count);

        if (delete_entire_node)
        {
            __quicklistDelNode(quicklist, node);
        }
        else
        {
            quicklistDecompressNodeForUse(node);
            node->zl = ziplistDeleteRange(node->zl, entry.offset, del);
            quicklistNodeUpdateSz(node);
            node->count -= del;
            quicklist->count -= del;
            quicklistDeleteIfEmpty(quicklist, node);
            if (node)
                quicklistRecompressOnly(quicklist, node);
        }

        extent -= del;

        node = next;

        entry.offset = 0;
    }
    return 1;
}

/* Passthrough to ziplistCompare() */
int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len)
{
    return ziplistCompare(p1, p2, p2_len);
}

/* Returns a quicklist iterator 'iter'. After the initialization every
 * call to quicklistNext() will return the next element of the quicklist.
 *
 * 给定迭代器方向 direction，获取指向快速链表 quicklist 的头部或者尾部的指针
 */
quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction)
{
    quicklistIter *iter;

    iter = zmalloc(sizeof(*iter));

    if (direction == AL_START_HEAD)
    {
        iter->current = quicklist->head;
        iter->offset = 0;
    }
    else if (direction == AL_START_TAIL)
    {
        iter->current = quicklist->tail;
        iter->offset = -1;
    }

    iter->direction = direction;
    iter->quicklist = quicklist;

    iter->zi = NULL;

    return iter;
}

/* Initialize an iterator at a specific offset 'idx' and make the iterator
 * return nodes in 'direction' direction.
 *
 * 获取快速链表 quicklist 中方向为 direction 的索引 idx 对应的数据节点的迭代器
 */
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist,
                                         const int direction,
                                         const long long idx)
{
    quicklistEntry entry;

    if (quicklistIndex(quicklist, idx, &entry)) // 获取索引为 idx 的数据节点 entry
    {
        quicklistIter *base = quicklistGetIterator(quicklist, direction);
        base->zi = NULL;
        base->current = entry.node;
        base->offset = entry.offset;
        return base;
    }
    else
    {
        return NULL;
    }
}

/* Release iterator.
 * If we still have a valid current node, then re-encode current node. */
void quicklistReleaseIterator(quicklistIter *iter)
{
    // 先尝试对迭代器指向的链表节点进行解压缩
    if (iter->current)
        quicklistCompress(iter->quicklist, iter->current);

    zfree(iter);
}

/**
 * Get next element in iterator.
 *
 * Note: You must NOT insert into the list while iterating over it.
 * You *may* delete from the list while iterating using the
 * quicklistDelEntry() function.
 * If you insert into the quicklist while iterating, you should
 * re-create the iterator after your addition.
 *
 * iter = quicklistGetIterator(quicklist,<direction>);
 * quicklistEntry entry;
 * while (quicklistNext(iter, &entry)) {
 *     if (entry.value)
 *          [[ use entry.value with entry.sz ]]
 *     else
 *          [[ use entry.longval ]]
 * }
 *
 * Populates 'entry' with values for this iteration.
 * Returns 0 when iteration is complete or if iteration not possible.
 * If return value is 0, the contents of 'entry' are not valid.
 *
 * 将迭代器 iter 沿迭代方向向后移动，同时将所指向的数据节点赋值给 quicklistEntry
 *
 * 在对迭代器进行迭代的时候，会根据迭代器的方向，选择调用 ziplistNext 或者 ziplistPrev 来对链表节点的底层压缩链表 ziplist 进行迭代
 *
 * NOTE: 迭代器迭代过程中，禁止对快速链表执行插入操作（迭代器可能失效），但是可以执行删除操作。
 */
int quicklistNext(quicklistIter *iter, quicklistEntry *entry)
{
    initEntry(entry);

    if (!iter)
    {
        D("Returning because no iter!");
        return 0;
    }

    entry->quicklist = iter->quicklist;
    entry->node = iter->current;

    if (!iter->current)
    {
        D("Returning because current node is NULL")
        return 0;
    }

    unsigned char *(*nextFn)(unsigned char *, unsigned char *) = NULL; // 访问 ziplist 中数据节点 entry 的函数指针
    int offset_update = 0;

    if (!iter->zi)
    {
        /* If !zi, use current index. */
        quicklistDecompressNodeForUse(iter->current);             // 临时解压缩当前链表节点
        iter->zi = ziplistIndex(iter->current->zl, iter->offset); // 获取 ziplist 中索引为 offset 的 entry, 然后赋给 zi（有可能返回 nullptr）
    }
    else
    {
        /* else, use existing iterator offset and get prev/next as necessary. */
        if (iter->direction == AL_START_HEAD)
        {
            nextFn = ziplistNext;
            offset_update = 1;
        }
        else if (iter->direction == AL_START_TAIL)
        {
            nextFn = ziplistPrev;
            offset_update = -1;
        }

        iter->zi = nextFn(iter->current->zl, iter->zi); // 调用 ziplistNext 或 ziplistPrev 接口遍历 ziplist 中的 entry（有可能返回 nullptr）
        iter->offset += offset_update;
    }

    entry->zi = iter->zi;
    entry->offset = iter->offset;

    if (iter->zi)
    {
        // 找到了迭代器指向的的数据节点 entry
        /* Populate value from existing ziplist position */
        ziplistGet(entry->zi, &entry->value, &entry->sz, &entry->longval);
        return 1;
    }
    else
    {
        // 在当前迭代器指向的链表节点中无法找到符合要求的 entry 节点，沿着当前迭代器移动到下一个链表节点继续查找
        /* We ran out of ziplist entries. Pick next node, update offset, then re-run retrieval. */
        quicklistCompress(iter->quicklist, iter->current); // 上面对于该链表节点进行了临时解压缩，在这里重新进行压缩
        if (iter->direction == AL_START_HEAD)
        {
            /* Forward traversal */
            D("Jumping to start of next node");
            iter->current = iter->current->next;
            iter->offset = 0;
        }
        else if (iter->direction == AL_START_TAIL)
        {
            /* Reverse traversal */
            D("Jumping to end of previous node");
            iter->current = iter->current->prev;
            iter->offset = -1;
        }
        iter->zi = NULL;
        return quicklistNext(iter, entry); // 递归调用 quicklistNext, 在下一个链表节点中查找符合要求的 entry
    }
}

/* Duplicate the quicklist.
 * On success a copy of the original quicklist is returned.
 *
 * The original quicklist both on success or error is never modified.
 *
 * Returns newly allocated quicklist.
 *
 * 拷贝一个快速链表, 返回拷贝的新的快速链表的指针
 */
quicklist *quicklistDup(quicklist *orig)
{
    quicklist *copy;

    copy = quicklistNew(orig->fill, orig->compress);

    for (quicklistNode *current = orig->head; current;
         current = current->next)
    {
        quicklistNode *node = quicklistCreateNode();

        if (current->encoding == QUICKLIST_NODE_ENCODING_LZF)
        {
            quicklistLZF *lzf = (quicklistLZF *)current->zl;
            size_t lzf_sz = sizeof(*lzf) + lzf->sz;
            node->zl = zmalloc(lzf_sz);
            memcpy(node->zl, current->zl, lzf_sz);
        }
        else if (current->encoding == QUICKLIST_NODE_ENCODING_RAW)
        {
            node->zl = zmalloc(current->sz);
            memcpy(node->zl, current->zl, current->sz);
        }

        node->count = current->count;
        copy->count += node->count;
        node->sz = current->sz;
        node->encoding = current->encoding;

        _quicklistInsertNodeAfter(copy, copy->tail, node);
    }

    /* copy->count must equal orig->count here */
    return copy;
}

/* Populate 'entry' with the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range 0 is returned.
 *
 * Returns 1 if element found
 * Returns 0 if element not found */
/**
 * 获取快速链表 quicklist 中的索引 idx 所对应的数据节点 entry。idx 为正，表示从前到后的索引，否则表示从后到前的索引
 */
int quicklistIndex(const quicklist *quicklist, const long long idx, quicklistEntry *entry)
{
    quicklistNode *n;
    unsigned long long accum = 0;
    unsigned long long index;
    int forward = idx < 0 ? 0 : 1; /* < 0 -> reverse, 0+ -> forward */

    initEntry(entry);
    entry->quicklist = quicklist;

    if (!forward)
    {
        index = (-idx) - 1;
        n = quicklist->tail;
    }
    else
    {
        index = idx;
        n = quicklist->head;
    }

    // 如果索引 index 超过了快速链表中所有数据节点 entry 的总数，则直接退出
    if (index >= quicklist->count)
        return 0;

    // 遍历 quicklist，找到 idx 所在的链表节点
    while (likely(n))
    {
        // n->count: 当前快速链表节点 n 中含有的数据节点 entry 的数量
        if ((accum + n->count) > index)
        {
            // 当前快速链表节点中的数据节点的数量累加上之前所有数据节点的数量大于 index，说明 index 的数据节点就在当前的链表节点
            break;
        }
        else
        {
            D("Skipping over (%p) %u at accum %lld", (void *)n, n->count, accum);
            accum += n->count;
            n = forward ? n->next : n->prev;
        }
    }

    if (!n)
        return 0;

    D("Found node: %p at accum %llu, idx %llu, sub+ %llu, sub- %llu", (void *)n,
      accum, index, index - accum, (-index) - 1 + accum);

    entry->node = n; // n 表示当前 entry 所在的链表节点
    if (forward)
    {
        /* forward = normal head-to-tail offset. */
        entry->offset = index - accum;
    }
    else
    {
        /* reverse = need negative offset for tail-to-head, so undo
         * the result of the original if (index < 0) above. */
        entry->offset = (-index) - 1 + accum;
    }

    quicklistDecompressNodeForUse(entry->node); // 临时解压数据节点 entry 所在的链表节点

    // entry->node->zl: 链表节点中的压缩链表
    // entry->offset: entry 在 ziplist 中的索引
    entry->zi = ziplistIndex(entry->node->zl, entry->offset); // 调用 ziplist 中的接口，获取当前压缩链表中索引为 offset 的数据节点 entry 的地址

    // entry->zi: ziplist 中某一个 entry 的指针
    ziplistGet(entry->zi, &entry->value, &entry->sz, &entry->longval); // 调用 ziplist 接口，解析该数据节点 entry 中的数据
    /* The caller will use our result, so we don't re-compress here.
     * The caller can recompress or delete the node as needed. */
    return 1;
}

/* Rotate quicklist by moving the tail element to the head. */
// 翻转一个快速链表: 依次从链表中弹出尾部的数据节点，然后插入到链表的头部
void quicklistRotate(quicklist *quicklist)
{
    if (quicklist->count <= 1)
        return;

    /* First, get the tail entry */
    unsigned char *p = ziplistIndex(quicklist->tail->zl, -1);
    unsigned char *value;
    long long longval;
    unsigned int sz;
    char longstr[32] = {0};
    ziplistGet(p, &value, &sz, &longval);

    /* If value found is NULL, then ziplistGet populated longval instead */
    if (!value)
    {
        /* Write the longval as a string so we can re-add it */
        sz = ll2string(longstr, sizeof(longstr), longval);
        value = (unsigned char *)longstr;
    }

    /* Add tail entry to head (must happen before tail is deleted). */
    quicklistPushHead(quicklist, value, sz);

    /* If quicklist has only one node, the head ziplist is also the
     * tail ziplist and PushHead() could have reallocated our single ziplist,
     * which would make our pre-existing 'p' unusable. */
    if (quicklist->len == 1)
    {
        p = ziplistIndex(quicklist->tail->zl, -1);
    }

    /* Remove tail entry. */
    quicklistDelIndex(quicklist, quicklist->tail, &p);
}

/* pop from quicklist and return result in 'data' ptr.  Value of 'data'
 * is the return value of 'saver' function pointer if the data is NOT a number.
 *
 * If the quicklist element is a long long, then the return value is returned in
 * 'sval'.
 *
 * Return value of 0 means no elements available.
 * Return value of 1 means check 'data' and 'sval' for values.
 * If 'data' is set, use 'data' and 'sz'.  Otherwise, use 'sval'.
 *
 * 从快速链表中弹出一个数据节点
 * 1. 先选择一个链表节点
 * 2. 弹出链表节点底层压缩链表中的一个数据节点
 */
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       unsigned int *sz, long long *sval,
                       void *(*saver)(unsigned char *data, unsigned int sz))
{
    unsigned char *p;
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    int pos = (where == QUICKLIST_HEAD) ? 0 : -1;

    if (quicklist->count == 0)
        return 0;

    if (data)
        *data = NULL;
    if (sz)
        *sz = 0;
    if (sval)
        *sval = -123456789;

    quicklistNode *node;
    // 选择链表头结点还是尾结点的压缩链表
    if (where == QUICKLIST_HEAD && quicklist->head)
    {
        node = quicklist->head;
    }
    else if (where == QUICKLIST_TAIL && quicklist->tail)
    {
        node = quicklist->tail;
    }
    else
    {
        return 0;
    }

    // 选择压缩链表的数据节点
    p = ziplistIndex(node->zl, pos);
    if (ziplistGet(p, &vstr, &vlen, &vlong))    // 解析数据节点中的数据
    {
        if (vstr)       // 数据节点中存储的数据是字符串
        {
            if (data)
                *data = saver(vstr, vlen);
            if (sz)
                *sz = vlen;
        }
        else        // 数据节点中存储的数据是整型数
        {
            if (data)
                *data = NULL;
            if (sval)
                *sval = vlong;
        }
        quicklistDelIndex(quicklist, node, &p); // 从链表节点 node 中删除数据节点 p
        return 1;
    }
    return 0;
}

/* Return a malloc'd copy of data passed in */
REDIS_STATIC void *_quicklistSaver(unsigned char *data, unsigned int sz)
{
    unsigned char *vstr;
    if (data)
    {
        vstr = zmalloc(sz);
        memcpy(vstr, data, sz);
        return vstr;
    }
    return NULL;
}

/* Default pop function
 *
 * Returns malloc'd value from quicklist */
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 unsigned int *sz, long long *slong)
{
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    if (quicklist->count == 0)
        return 0;
    int ret = quicklistPopCustom(quicklist, where, &vstr, &vlen, &vlong,
                                 _quicklistSaver);
    if (data)
        *data = vstr;
    if (slong)
        *slong = vlong;
    if (sz)
        *sz = vlen;
    return ret;
}

/* Wrapper to allow argument-based switching between HEAD/TAIL pop */
void quicklistPush(quicklist *quicklist, void *value, const size_t sz, int where)
{
    if (where == QUICKLIST_HEAD)
    {
        quicklistPushHead(quicklist, value, sz);
    }
    else if (where == QUICKLIST_TAIL)
    {
        quicklistPushTail(quicklist, value, sz);
    }
}

/* Create or update a bookmark in the list which will be updated to the next node
 * automatically when the one referenced gets deleted.
 * Returns 1 on success (creation of new bookmark or override of an existing one).
 * Returns 0 on failure (reached the maximum supported number of bookmarks).
 * NOTE: use short simple names, so that string compare on find is quick.
 * NOTE: bookmakrk creation may re-allocate the quicklist, so the input pointer
         may change and it's the caller responsibilty to update the reference.
 */
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node)
{
    quicklist *ql = *ql_ref;
    if (ql->bookmark_count >= QL_MAX_BM)
        return 0;
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (bm)
    {
        bm->node = node;
        return 1;
    }
    ql = zrealloc(ql, sizeof(quicklist) + (ql->bookmark_count + 1) * sizeof(quicklistBookmark));
    *ql_ref = ql;
    ql->bookmarks[ql->bookmark_count].node = node;
    ql->bookmarks[ql->bookmark_count].name = zstrdup(name);
    ql->bookmark_count++;
    return 1;
}

/* Find the quicklist node referenced by a named bookmark.
 * When the bookmarked node is deleted the bookmark is updated to the next node,
 * and if that's the last node, the bookmark is deleted (so find returns NULL). */
quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name)
{
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (!bm)
        return NULL;
    return bm->node;
}

/* Delete a named bookmark.
 * returns 0 if bookmark was not found, and 1 if deleted.
 * Note that the bookmark memory is not freed yet, and is kept for future use. */
int quicklistBookmarkDelete(quicklist *ql, const char *name)
{
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (!bm)
        return 0;
    _quicklistBookmarkDelete(ql, bm);
    return 1;
}

quicklistBookmark *_quicklistBookmarkFindByName(quicklist *ql, const char *name)
{
    unsigned i;
    for (i = 0; i < ql->bookmark_count; i++)
    {
        if (!strcmp(ql->bookmarks[i].name, name))
        {
            return &ql->bookmarks[i];
        }
    }
    return NULL;
}

quicklistBookmark *_quicklistBookmarkFindByNode(quicklist *ql, quicklistNode *node)
{
    unsigned i;
    for (i = 0; i < ql->bookmark_count; i++)
    {
        if (ql->bookmarks[i].node == node)
        {
            return &ql->bookmarks[i];
        }
    }
    return NULL;
}

void _quicklistBookmarkDelete(quicklist *ql, quicklistBookmark *bm)
{
    int index = bm - ql->bookmarks;
    zfree(bm->name);
    ql->bookmark_count--;
    memmove(bm, bm + 1, (ql->bookmark_count - index) * sizeof(*bm));
    /* NOTE: We do not shrink (realloc) the quicklist yet (to avoid resonance,
     * it may be re-used later (a call to realloc may NOP). */
}

void quicklistBookmarksClear(quicklist *ql)
{
    while (ql->bookmark_count)
        zfree(ql->bookmarks[--ql->bookmark_count].name);
    /* NOTE: We do not shrink (realloc) the quick list. main use case for this
     * function is just before releasing the allocation. */
}

/* The rest of this file is test cases and test helpers. */
#ifdef REDIS_TEST
#include <stdint.h>
#include <sys/time.h>

#define assert(_e)                                                           \
    do                                                                       \
    {                                                                        \
        if (!(_e))                                                           \
        {                                                                    \
            printf("\n\n=== ASSERTION FAILED ===\n");                        \
            printf("==> %s:%d '%s' is not true\n", __FILE__, __LINE__, #_e); \
            err++;                                                           \
        }                                                                    \
    } while (0)

#define yell(str, ...) printf("ERROR! " str "\n\n", __VA_ARGS__)

#define OK printf("\tOK\n")

#define ERROR                 \
    do                        \
    {                         \
        printf("\tERROR!\n"); \
        err++;                \
    } while (0)

#define ERR(x, ...)                                              \
    do                                                           \
    {                                                            \
        printf("%s:%s:%d:\t", __FILE__, __FUNCTION__, __LINE__); \
        printf("ERROR! " x "\n", __VA_ARGS__);                   \
        err++;                                                   \
    } while (0)

#define TEST(name) printf("test — %s\n", name);
#define TEST_DESC(name, ...) printf("test — " name "\n", __VA_ARGS__);

#define QL_TEST_VERBOSE 0

#define UNUSED(x) (void)(x)
static void ql_info(quicklist *ql)
{
#if QL_TEST_VERBOSE
    printf("Container length: %lu\n", ql->len);
    printf("Container size: %lu\n", ql->count);
    if (ql->head)
        printf("\t(zsize head: %d)\n", ziplistLen(ql->head->zl));
    if (ql->tail)
        printf("\t(zsize tail: %d)\n", ziplistLen(ql->tail->zl));
    printf("\n");
#else
    UNUSED(ql);
#endif
}

/* Return the UNIX time in microseconds */
static long long ustime(void)
{
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
static long long mstime(void) { return ustime() / 1000; }

/* Iterate over an entire quicklist.
 * Print the list if 'print' == 1.
 *
 * Returns physical count of elements found by iterating over the list. */
static int _itrprintr(quicklist *ql, int print, int forward)
{
    quicklistIter *iter =
        quicklistGetIterator(ql, forward ? AL_START_HEAD : AL_START_TAIL);
    quicklistEntry entry;
    int i = 0;
    int p = 0;
    quicklistNode *prev = NULL;
    while (quicklistNext(iter, &entry))
    {
        if (entry.node != prev)
        {
            /* Count the number of list nodes too */
            p++;
            prev = entry.node;
        }
        if (print)
        {
            printf("[%3d (%2d)]: [%.*s] (%lld)\n", i, p, entry.sz,
                   (char *)entry.value, entry.longval);
        }
        i++;
    }
    quicklistReleaseIterator(iter);
    return i;
}
static int itrprintr(quicklist *ql, int print)
{
    return _itrprintr(ql, print, 1);
}

static int itrprintr_rev(quicklist *ql, int print)
{
    return _itrprintr(ql, print, 0);
}

#define ql_verify(a, b, c, d, e)                    \
    do                                              \
    {                                               \
        err += _ql_verify((a), (b), (c), (d), (e)); \
    } while (0)

/* Verify list metadata matches physical list contents. */
static int _ql_verify(quicklist *ql, uint32_t len, uint32_t count,
                      uint32_t head_count, uint32_t tail_count)
{
    int errors = 0;

    ql_info(ql);
    if (len != ql->len)
    {
        yell("quicklist length wrong: expected %d, got %u", len, ql->len);
        errors++;
    }

    if (count != ql->count)
    {
        yell("quicklist count wrong: expected %d, got %lu", count, ql->count);
        errors++;
    }

    int loopr = itrprintr(ql, 0);
    if (loopr != (int)ql->count)
    {
        yell("quicklist cached count not match actual count: expected %lu, got "
             "%d",
             ql->count, loopr);
        errors++;
    }

    int rloopr = itrprintr_rev(ql, 0);
    if (loopr != rloopr)
    {
        yell("quicklist has different forward count than reverse count!  "
             "Forward count is %d, reverse count is %d.",
             loopr, rloopr);
        errors++;
    }

    if (ql->len == 0 && !errors)
    {
        OK;
        return errors;
    }

    if (ql->head && head_count != ql->head->count &&
        head_count != ziplistLen(ql->head->zl))
    {
        yell("quicklist head count wrong: expected %d, "
             "got cached %d vs. actual %d",
             head_count, ql->head->count, ziplistLen(ql->head->zl));
        errors++;
    }

    if (ql->tail && tail_count != ql->tail->count &&
        tail_count != ziplistLen(ql->tail->zl))
    {
        yell("quicklist tail count wrong: expected %d, "
             "got cached %u vs. actual %d",
             tail_count, ql->tail->count, ziplistLen(ql->tail->zl));
        errors++;
    }

    if (quicklistAllowsCompression(ql))
    {
        quicklistNode *node = ql->head;
        unsigned int low_raw = ql->compress;
        unsigned int high_raw = ql->len - ql->compress;

        for (unsigned int at = 0; at < ql->len; at++, node = node->next)
        {
            if (node && (at < low_raw || at >= high_raw))
            {
                if (node->encoding != QUICKLIST_NODE_ENCODING_RAW)
                {
                    yell("Incorrect compression: node %d is "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %u; size: %u; recompress: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress);
                    errors++;
                }
            }
            else
            {
                if (node->encoding != QUICKLIST_NODE_ENCODING_LZF &&
                    !node->attempted_compress)
                {
                    yell("Incorrect non-compression: node %d is NOT "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %u; size: %u; recompress: %d; attempted: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress, node->attempted_compress);
                    errors++;
                }
            }
        }
    }

    if (!errors)
        OK;
    return errors;
}

/* Generate new string concatenating integer i against string 'prefix' */
static char *genstr(char *prefix, int i)
{
    static char result[64] = {0};
    snprintf(result, sizeof(result), "%s%d", prefix, i);
    return result;
}

/* main test, but callable from other files */
int quicklistTest(int argc, char *argv[])
{
    UNUSED(argc);
    UNUSED(argv);

    unsigned int err = 0;
    int optimize_start =
        -(int)(sizeof(optimization_level) / sizeof(*optimization_level));

    printf("Starting optimization offset at: %d\n", optimize_start);

    int options[] = {0, 1, 2, 3, 4, 5, 6, 10};
    size_t option_count = sizeof(options) / sizeof(*options);
    long long runtime[option_count];

    for (int _i = 0; _i < (int)option_count; _i++)
    {
        printf("Testing Option %d\n", options[_i]);
        long long start = mstime();

        TEST("create list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("add to tail of empty list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "hello", 6);
            /* 1 for head and 1 for tail because 1 node = head = tail */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("add to head of empty list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            /* 1 for head and 1 for tail because 1 node = head = tail */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        for (int f = optimize_start; f < 32; f++)
        {
            TEST_DESC("add to tail 5x at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 32; f++)
        {
            TEST_DESC("add to head 5x at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 512; f++)
        {
            TEST_DESC("add to tail 500x at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 64);
                if (ql->count != 500)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 16, 500, 32, 20);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 512; f++)
        {
            TEST_DESC("add to head 500x at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 500)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 16, 500, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("rotate empty")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistRotate(ql);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        for (int f = optimize_start; f < 32; f++)
        {
            TEST("rotate one val once")
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushHead(ql, "hello", 6);
                quicklistRotate(ql);
                /* Ignore compression verify because ziplist is
                 * too small to compress. */
                ql_verify(ql, 1, 1, 1, 1);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 3; f++)
        {
            TEST_DESC("rotate 500 val 5000 times at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushHead(ql, "900", 3);
                quicklistPushHead(ql, "7000", 4);
                quicklistPushHead(ql, "-1200", 5);
                quicklistPushHead(ql, "42", 2);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 64);
                ql_info(ql);
                for (int i = 0; i < 5000; i++)
                {
                    ql_info(ql);
                    quicklistRotate(ql);
                }
                if (f == 1)
                    ql_verify(ql, 504, 504, 1, 1);
                else if (f == 2)
                    ql_verify(ql, 252, 504, 2, 2);
                else if (f == 32)
                    ql_verify(ql, 16, 504, 32, 24);
                quicklistRelease(ql);
            }
        }

        TEST("pop empty")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPop(ql, QUICKLIST_HEAD, NULL, NULL, NULL);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop 1 string from 1")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            char *populate = genstr("hello", 331);
            quicklistPushHead(ql, populate, 32);
            unsigned char *data;
            unsigned int sz;
            long long lv;
            ql_info(ql);
            quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
            assert(data != NULL);
            assert(sz == 32);
            if (strcmp(populate, (char *)data))
                ERR("Pop'd value (%.*s) didn't equal original value (%s)", sz,
                    data, populate);
            zfree(data);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 1 number from 1")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "55513", 5);
            unsigned char *data;
            unsigned int sz;
            long long lv;
            ql_info(ql);
            quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
            assert(data == NULL);
            assert(lv == 55513);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 500 from 500")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_info(ql);
            for (int i = 0; i < 500; i++)
            {
                unsigned char *data;
                unsigned int sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                assert(ret == 1);
                assert(data != NULL);
                assert(sz == 32);
                if (strcmp(genstr("hello", 499 - i), (char *)data))
                    ERR("Pop'd value (%.*s) didn't equal original value (%s)",
                        sz, data, genstr("hello", 499 - i));
                zfree(data);
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 5000 from 500")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            for (int i = 0; i < 5000; i++)
            {
                unsigned char *data;
                unsigned int sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                if (i < 500)
                {
                    assert(ret == 1);
                    assert(data != NULL);
                    assert(sz == 32);
                    if (strcmp(genstr("hello", 499 - i), (char *)data))
                        ERR("Pop'd value (%.*s) didn't equal original value "
                            "(%s)",
                            sz, data, genstr("hello", 499 - i));
                    zfree(data);
                }
                else
                {
                    assert(ret == 0);
                }
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("iterate forward over 500 list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
            quicklistEntry entry;
            int i = 499, count = 0;
            while (quicklistNext(iter, &entry))
            {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i--;
                count++;
            }
            if (count != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            quicklistReleaseIterator(iter);
            quicklistRelease(ql);
        }

        TEST("iterate reverse over 500 list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
            quicklistEntry entry;
            int i = 0;
            while (quicklistNext(iter, &entry))
            {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i++;
            }
            if (i != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            quicklistReleaseIterator(iter);
            quicklistRelease(ql);
        }

        TEST("insert before with 0 elements")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertBefore(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("insert after with 0 elements")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("insert after 1 element")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 2, 2, 2);
            quicklistRelease(ql);
        }

        TEST("insert before 1 element")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 2, 2, 2);
            quicklistRelease(ql);
        }

        for (int f = optimize_start; f < 12; f++)
        {
            TEST_DESC("insert once in elements while iterating at fill %d at "
                      "compress %d\n",
                      f, options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistSetFill(ql, 1);
                quicklistPushTail(ql, "def", 3); /* force to unique node */
                quicklistSetFill(ql, f);
                quicklistPushTail(ql, "bob", 3); /* force to reset for +3 */
                quicklistPushTail(ql, "foo", 3);
                quicklistPushTail(ql, "zoo", 3);

                itrprintr(ql, 0);
                /* insert "bar" before "bob" while iterating over list. */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                while (quicklistNext(iter, &entry))
                {
                    if (!strncmp((char *)entry.value, "bob", 3))
                    {
                        /* Insert as fill = 1 so it spills into new node. */
                        quicklistInsertBefore(ql, &entry, "bar", 3);
                        break; /* didn't we fix insert-while-iterating? */
                    }
                }
                itrprintr(ql, 0);

                /* verify results */
                quicklistIndex(ql, 0, &entry);
                if (strncmp((char *)entry.value, "abc", 3))
                    ERR("Value 0 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 1, &entry);
                if (strncmp((char *)entry.value, "def", 3))
                    ERR("Value 1 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 2, &entry);
                if (strncmp((char *)entry.value, "bar", 3))
                    ERR("Value 2 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 3, &entry);
                if (strncmp((char *)entry.value, "bob", 3))
                    ERR("Value 3 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 4, &entry);
                if (strncmp((char *)entry.value, "foo", 3))
                    ERR("Value 4 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 5, &entry);
                if (strncmp((char *)entry.value, "zoo", 3))
                    ERR("Value 5 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 1024; f++)
        {
            TEST_DESC(
                "insert [before] 250 new in middle of 500 elements at fill"
                " %d at compress %d",
                f, options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++)
                {
                    quicklistEntry entry;
                    quicklistIndex(ql, 250, &entry);
                    quicklistInsertBefore(ql, &entry, genstr("abc", i), 32);
                }
                if (f == 32)
                    ql_verify(ql, 25, 750, 32, 20);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 1024; f++)
        {
            TEST_DESC("insert [after] 250 new in middle of 500 elements at "
                      "fill %d at compress %d",
                      f, options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++)
                {
                    quicklistEntry entry;
                    quicklistIndex(ql, 250, &entry);
                    quicklistInsertAfter(ql, &entry, genstr("abc", i), 32);
                }

                if (ql->count != 750)
                    ERR("List size not 750, but rather %ld", ql->count);

                if (f == 32)
                    ql_verify(ql, 26, 750, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("duplicate empty list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 0, 0, 0, 0);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 1 element")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, genstr("hello", 3), 32);
            ql_verify(ql, 1, 1, 1, 1);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 1, 1, 1, 1);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 500")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 16, 500, 20, 32);

            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 16, 500, 20, 32);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        for (int f = optimize_start; f < 512; f++)
        {
            TEST_DESC("index 1,200 from 500 list at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, 1, &entry);
                if (!strcmp((char *)entry.value, "hello2"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistIndex(ql, 200, &entry);
                if (!strcmp((char *)entry.value, "hello201"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index -1,-2 from 500 list at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, -1, &entry);
                if (!strcmp((char *)entry.value, "hello500"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistIndex(ql, -2, &entry);
                if (!strcmp((char *)entry.value, "hello499"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index -100 from 500 list at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, -100, &entry);
                if (!strcmp((char *)entry.value, "hello401"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index too big +1 from 50 list at fill %d at compress %d",
                      f, options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 50; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                if (quicklistIndex(ql, 50, &entry))
                    ERR("Index found at 50 with 50 list: %.*s", entry.sz,
                        entry.value);
                else
                    OK;
                quicklistRelease(ql);
            }
        }

        TEST("delete range empty list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistDelRange(ql, 5, 20);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node in list of one node")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 32);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node with overflow counts")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 128);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete middle 100 of 500 list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, 200, 100);
            ql_verify(ql, 14, 400, 32, 20);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 1);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list with overflow counts")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 128);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 100 from 500 list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            quicklistDelRange(ql, -100, 100);
            ql_verify(ql, 13, 400, 32, 16);
            quicklistRelease(ql);
        }

        TEST("delete -10 count 5 from 50 list")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 50; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 2, 50, 32, 18);
            quicklistDelRange(ql, -10, 5);
            ql_verify(ql, 2, 45, 32, 13);
            quicklistRelease(ql);
        }

        TEST("numbers only list read")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "1111", 4);
            quicklistPushTail(ql, "2222", 4);
            quicklistPushTail(ql, "3333", 4);
            quicklistPushTail(ql, "4444", 4);
            ql_verify(ql, 1, 4, 4, 4);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111, %lld", entry.longval);
            quicklistIndex(ql, 1, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222, %lld", entry.longval);
            quicklistIndex(ql, 2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333, %lld", entry.longval);
            quicklistIndex(ql, 3, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444, %lld", entry.longval);
            if (quicklistIndex(ql, 4, &entry))
                ERR("Index past elements: %lld", entry.longval);
            quicklistIndex(ql, -1, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -3, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -4, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111 (reverse), %lld", entry.longval);
            if (quicklistIndex(ql, -5, &entry))
                ERR("Index past elements (reverse), %lld", entry.longval);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            char num[32];
            long long nums[5000];
            for (int i = 0; i < 5000; i++)
            {
                nums[i] = -5157318210846258176 + i;
                int sz = ll2string(num, sizeof(num), nums[i]);
                quicklistPushTail(ql, num, sz);
            }
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistEntry entry;
            for (int i = 0; i < 5000; i++)
            {
                quicklistIndex(ql, i, &entry);
                if (entry.longval != nums[i])
                    ERR("[%d] Not longval %lld but rather %lld", i, nums[i],
                        entry.longval);
                entry.longval = 0xdeadbeef;
            }
            quicklistIndex(ql, 5000, &entry);
            if (strncmp((char *)entry.value, "xxxxxxxxxxxxxxxxxxxx", 20))
                ERR("String val not match: %s", entry.value);
            ql_verify(ql, 157, 5001, 32, 9);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read B")
        {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "99", 2);
            quicklistPushTail(ql, "98", 2);
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistPushTail(ql, "96", 2);
            quicklistPushTail(ql, "95", 2);
            quicklistReplaceAtIndex(ql, 1, "foo", 3);
            quicklistReplaceAtIndex(ql, -1, "bar", 3);
            quicklistRelease(ql);
            OK;
        }

        for (int f = optimize_start; f < 16; f++)
        {
            TEST_DESC("lrem test at fill %d at compress %d", f, options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                char *words[] = {"abc", "foo", "bar", "foobar", "foobared",
                                 "zap", "bar", "test", "foo"};
                char *result[] = {"abc", "foo", "foobar", "foobared",
                                  "zap", "test", "foo"};
                char *resultB[] = {"abc", "foo", "foobar",
                                   "foobared", "zap", "test"};
                for (int i = 0; i < 9; i++)
                    quicklistPushTail(ql, words[i], strlen(words[i]));

                /* lrem 0 bar */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                int i = 0;
                while (quicklistNext(iter, &entry))
                {
                    if (quicklistCompare(entry.zi, (unsigned char *)"bar", 3))
                    {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                /* check result of lrem 0 bar */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                int ok = 1;
                while (quicklistNext(iter, &entry))
                {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    if (strncmp((char *)entry.value, result[i], entry.sz))
                    {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, entry.sz, entry.value, result[i]);
                        ok = 0;
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                quicklistPushTail(ql, "foo", 3);

                /* lrem -2 foo */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                int del = 2;
                while (quicklistNext(iter, &entry))
                {
                    if (quicklistCompare(entry.zi, (unsigned char *)"foo", 3))
                    {
                        quicklistDelEntry(iter, &entry);
                        del--;
                    }
                    if (!del)
                        break;
                    i++;
                }
                quicklistReleaseIterator(iter);

                /* check result of lrem -2 foo */
                /* (we're ignoring the '2' part and still deleting all foo
                 * because
                 * we only have two foo) */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                size_t resB = sizeof(resultB) / sizeof(*resultB);
                while (quicklistNext(iter, &entry))
                {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    if (strncmp((char *)entry.value, resultB[resB - 1 - i],
                                entry.sz))
                    {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, entry.sz, entry.value, resultB[resB - 1 - i]);
                        ok = 0;
                    }
                    i++;
                }

                quicklistReleaseIterator(iter);
                /* final result of all tests */
                if (ok)
                    OK;
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 16; f++)
        {
            TEST_DESC("iterate reverse + delete at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistPushTail(ql, "def", 3);
                quicklistPushTail(ql, "hij", 3);
                quicklistPushTail(ql, "jkl", 3);
                quicklistPushTail(ql, "oop", 3);

                quicklistEntry entry;
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
                int i = 0;
                while (quicklistNext(iter, &entry))
                {
                    if (quicklistCompare(entry.zi, (unsigned char *)"hij", 3))
                    {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                if (i != 5)
                    ERR("Didn't iterate 5 times, iterated %d times.", i);

                /* Check results after deletion of "hij" */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                char *vals[] = {"abc", "def", "jkl", "oop"};
                while (quicklistNext(iter, &entry))
                {
                    if (!quicklistCompare(entry.zi, (unsigned char *)vals[i],
                                          3))
                    {
                        ERR("Value at %d didn't match %s\n", i, vals[i]);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 800; f++)
        {
            TEST_DESC("iterator at index test at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 760; i++)
                {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }

                quicklistEntry entry;
                quicklistIter *iter =
                    quicklistGetIteratorAtIdx(ql, AL_START_HEAD, 437);
                int i = 437;
                while (quicklistNext(iter, &entry))
                {
                    if (entry.longval != nums[i])
                        ERR("Expected %lld, but got %lld", entry.longval,
                            nums[i]);
                    i++;
                }
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++)
        {
            TEST_DESC("ltrim test A at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 32; i++)
                {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 1, 32, 32, 32);
                /* ltrim 25 53 (keep [25,32] inclusive = 7 remaining) */
                quicklistDelRange(ql, 0, 25);
                quicklistDelRange(ql, 0, 0);
                quicklistEntry entry;
                for (int i = 0; i < 7; i++)
                {
                    quicklistIndex(ql, i, &entry);
                    if (entry.longval != nums[25 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[25 + i]);
                }
                if (f == 32)
                    ql_verify(ql, 1, 7, 7, 7);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++)
        {
            TEST_DESC("ltrim test B at fill %d at compress %d", f,
                      options[_i])
            {
                /* Force-disable compression because our 33 sequential
                 * integers don't compress and the check always fails. */
                quicklist *ql = quicklistNew(f, QUICKLIST_NOCOMPRESS);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++)
                {
                    nums[i] = i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 5 16 (keep [5,16] inclusive = 12 remaining) */
                quicklistDelRange(ql, 0, 5);
                quicklistDelRange(ql, -16, 16);
                if (f == 32)
                    ql_verify(ql, 1, 12, 12, 12);
                quicklistEntry entry;
                quicklistIndex(ql, 0, &entry);
                if (entry.longval != 5)
                    ERR("A: longval not 5, but %lld", entry.longval);
                else
                    OK;
                quicklistIndex(ql, -1, &entry);
                if (entry.longval != 16)
                    ERR("B! got instead: %lld", entry.longval);
                else
                    OK;
                quicklistPushTail(ql, "bobobob", 7);
                quicklistIndex(ql, -1, &entry);
                if (strncmp((char *)entry.value, "bobobob", 7))
                    ERR("Tail doesn't match bobobob, it's %.*s instead",
                        entry.sz, entry.value);
                for (int i = 0; i < 12; i++)
                {
                    quicklistIndex(ql, i, &entry);
                    if (entry.longval != nums[5 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[5 + i]);
                }
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++)
        {
            TEST_DESC("ltrim test C at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++)
                {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
                quicklistDelRange(ql, 0, 3);
                quicklistDelRange(ql, -29,
                                  4000); /* make sure not loop forever */
                if (f == 32)
                    ql_verify(ql, 1, 1, 1, 1);
                quicklistEntry entry;
                quicklistIndex(ql, 0, &entry);
                if (entry.longval != -5157318210846258173)
                    ERROR;
                else
                    OK;
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++)
        {
            TEST_DESC("ltrim test D at fill %d at compress %d", f,
                      options[_i])
            {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++)
                {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                quicklistDelRange(ql, -12, 3);
                if (ql->count != 30)
                    ERR("Didn't delete exactly three elements!  Count is: %lu",
                        ql->count);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 72; f++)
        {
            TEST_DESC("create quicklist from ziplist at fill %d at compress %d",
                      f, options[_i])
            {
                unsigned char *zl = ziplistNew();
                long long nums[64];
                char num[64];
                for (int i = 0; i < 33; i++)
                {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    zl =
                        ziplistPush(zl, (unsigned char *)num, sz, ZIPLIST_TAIL);
                }
                for (int i = 0; i < 33; i++)
                {
                    zl = ziplistPush(zl, (unsigned char *)genstr("hello", i),
                                     32, ZIPLIST_TAIL);
                }
                quicklist *ql = quicklistCreateFromZiplist(f, options[_i], zl);
                if (f == 1)
                    ql_verify(ql, 66, 66, 1, 1);
                else if (f == 32)
                    ql_verify(ql, 3, 66, 32, 2);
                else if (f == 66)
                    ql_verify(ql, 1, 66, 66, 66);
                quicklistRelease(ql);
            }
        }

        long long stop = mstime();
        runtime[_i] = stop - start;
    }

    /* Run a longer test of compression depth outside of primary test loop. */
    int list_sizes[] = {250, 251, 500, 999, 1000};
    long long start = mstime();
    for (int list = 0; list < (int)(sizeof(list_sizes) / sizeof(*list_sizes));
         list++)
    {
        for (int f = optimize_start; f < 128; f++)
        {
            for (int depth = 1; depth < 40; depth++)
            {
                /* skip over many redundant test cases */
                TEST_DESC("verify specific compression of interior nodes with "
                          "%d list "
                          "at fill %d at compress %d",
                          list_sizes[list], f, depth)
                {
                    quicklist *ql = quicklistNew(f, depth);
                    for (int i = 0; i < list_sizes[list]; i++)
                    {
                        quicklistPushTail(ql, genstr("hello TAIL", i + 1), 64);
                        quicklistPushHead(ql, genstr("hello HEAD", i + 1), 64);
                    }

                    quicklistNode *node = ql->head;
                    unsigned int low_raw = ql->compress;
                    unsigned int high_raw = ql->len - ql->compress;

                    for (unsigned int at = 0; at < ql->len;
                         at++, node = node->next)
                    {
                        if (at < low_raw || at >= high_raw)
                        {
                            if (node->encoding != QUICKLIST_NODE_ENCODING_RAW)
                            {
                                ERR("Incorrect compression: node %d is "
                                    "compressed at depth %d ((%u, %u); total "
                                    "nodes: %u; size: %u)",
                                    at, depth, low_raw, high_raw, ql->len,
                                    node->sz);
                            }
                        }
                        else
                        {
                            if (node->encoding != QUICKLIST_NODE_ENCODING_LZF)
                            {
                                ERR("Incorrect non-compression: node %d is NOT "
                                    "compressed at depth %d ((%u, %u); total "
                                    "nodes: %u; size: %u; attempted: %d)",
                                    at, depth, low_raw, high_raw, ql->len,
                                    node->sz, node->attempted_compress);
                            }
                        }
                    }
                    quicklistRelease(ql);
                }
            }
        }
    }
    long long stop = mstime();

    printf("\n");
    for (size_t i = 0; i < option_count; i++)
        printf("Test Loop %02d: %0.2f seconds.\n", options[i],
               (float)runtime[i] / 1000);
    printf("Compressions: %0.2f seconds.\n", (float)(stop - start) / 1000);
    printf("\n");

    TEST("bookmark get updated to next item")
    {
        quicklist *ql = quicklistNew(1, 0);
        quicklistPushTail(ql, "1", 1);
        quicklistPushTail(ql, "2", 1);
        quicklistPushTail(ql, "3", 1);
        quicklistPushTail(ql, "4", 1);
        quicklistPushTail(ql, "5", 1);
        assert(ql->len == 5);
        /* add two bookmarks, one pointing to the node before the last. */
        assert(quicklistBookmarkCreate(&ql, "_dummy", ql->head->next));
        assert(quicklistBookmarkCreate(&ql, "_test", ql->tail->prev));
        /* test that the bookmark returns the right node, delete it and see that the bookmark points to the last node */
        assert(quicklistBookmarkFind(ql, "_test") == ql->tail->prev);
        assert(quicklistDelRange(ql, -2, 1));
        assert(quicklistBookmarkFind(ql, "_test") == ql->tail);
        /* delete the last node, and see that the bookmark was deleted. */
        assert(quicklistDelRange(ql, -1, 1));
        assert(quicklistBookmarkFind(ql, "_test") == NULL);
        /* test that other bookmarks aren't affected */
        assert(quicklistBookmarkFind(ql, "_dummy") == ql->head->next);
        assert(quicklistBookmarkFind(ql, "_missing") == NULL);
        assert(ql->len == 3);
        quicklistBookmarksClear(ql); /* for coverage */
        assert(quicklistBookmarkFind(ql, "_dummy") == NULL);
        quicklistRelease(ql);
    }

    TEST("bookmark limit")
    {
        int i;
        quicklist *ql = quicklistNew(1, 0);
        quicklistPushHead(ql, "1", 1);
        for (i = 0; i < QL_MAX_BM; i++)
            assert(quicklistBookmarkCreate(&ql, genstr("", i), ql->head));
        /* when all bookmarks are used, creation fails */
        assert(!quicklistBookmarkCreate(&ql, "_test", ql->head));
        /* delete one and see that we can now create another */
        assert(quicklistBookmarkDelete(ql, "0"));
        assert(quicklistBookmarkCreate(&ql, "_test", ql->head));
        /* delete one and see that the rest survive */
        assert(quicklistBookmarkDelete(ql, "_test"));
        for (i = 1; i < QL_MAX_BM; i++)
            assert(quicklistBookmarkFind(ql, genstr("", i)) == ql->head);
        /* make sure the deleted ones are indeed gone */
        assert(!quicklistBookmarkFind(ql, "0"));
        assert(!quicklistBookmarkFind(ql, "_test"));
        quicklistRelease(ql);
    }

    if (!err)
        printf("ALL TESTS PASSED!\n");
    else
        ERR("Sorry, not all tests passed!  In fact, %d tests failed.", err);

    return err;
}
#endif
