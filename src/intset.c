/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"
#include "redisassert.h"

/* Note that these encodings are ordered, so:
 * INTSET_ENC_INT16 < INTSET_ENC_INT32 < INTSET_ENC_INT64. */
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/* Return the required encoding for the provided value. */
static uint8_t _intsetValueEncoding(int64_t v)
{
    if (v < INT32_MIN || v > INT32_MAX)
        return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX)
        return INTSET_ENC_INT32;
    else
        return INTSET_ENC_INT16;
}

/* Return the value at pos, given an encoding. */
// 根据编码方式 enc，从 intset 中解析出 pos 位置上的数据
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc)
{
    int64_t v64;
    int32_t v32;
    int16_t v16;

    if (enc == INTSET_ENC_INT64)
    {
        memcpy(&v64, ((int64_t *)is->contents) + pos, sizeof(v64));
        memrev64ifbe(&v64);
        return v64;
    }
    else if (enc == INTSET_ENC_INT32)
    {
        memcpy(&v32, ((int32_t *)is->contents) + pos, sizeof(v32));
        memrev32ifbe(&v32);
        return v32;
    }
    else
    {
        memcpy(&v16, ((int16_t *)is->contents) + pos, sizeof(v16));
        memrev16ifbe(&v16);
        return v16;
    }
}

/* Return the value at pos, using the configured encoding. */
// 返回整数集合 is 中指定位置 pos 的数据
static int64_t _intsetGet(intset *is, int pos)
{
    return _intsetGetEncoded(is, pos, intrev32ifbe(is->encoding));
}

/* Set the value at pos, using the configured encoding. */
// 在整数集合 is 的指定位置 pos，存储数据 value
static void _intsetSet(intset *is, int pos, int64_t value)
{
    uint32_t encoding = intrev32ifbe(is->encoding);

    if (encoding == INTSET_ENC_INT64)
    {
        ((int64_t *)is->contents)[pos] = value;
        memrev64ifbe(((int64_t *)is->contents) + pos);
    }
    else if (encoding == INTSET_ENC_INT32)
    {
        ((int32_t *)is->contents)[pos] = value;
        memrev32ifbe(((int32_t *)is->contents) + pos);
    }
    else
    {
        ((int16_t *)is->contents)[pos] = value;
        memrev16ifbe(((int16_t *)is->contents) + pos);
    }
}

/* Create an empty intset. */
intset *intsetNew(void)
{
    intset *is = zmalloc(sizeof(intset));
    is->encoding = intrev32ifbe(INTSET_ENC_INT16);
    is->length = 0;
    return is;
}

/* Resize the intset */
// 重新为 intset 分配内存，可以存储 len 个 intset.encoding 指定类型的数据
static intset *intsetResize(intset *is, uint32_t len)
{
    uint64_t size = (uint64_t)len * intrev32ifbe(is->encoding);
    assert(size <= SIZE_MAX - sizeof(intset));
    is = zrealloc(is, sizeof(intset) + size);
    return is;
}

/* Search for the position of "value". Return 1 when the value was found and
 * sets "pos" to the position of the value within the intset. Return 0 when
 * the value is not present in the intset and sets "pos" to the position
 * where "value" can be inserted.
 *
 * 因为整数集合 intset 是有序的，所以可以使用二分法以 O(logN) 的时间复杂度在整数集合 is 中查找对应的整数 value
 *
 * 如果传入的 pos 指针不为 nullptr，那么在查找成功返回 1 的时候，会将 value 的位置指针赋给 pos；
 * 当查询失败返回 0 的时候，会将 value 应该在整数集合 intset 中所处的位置指针赋给 pos
 */
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos)
{
    int min = 0, max = intrev32ifbe(is->length) - 1, mid = -1;
    int64_t cur = -1;

    /* The value can never be found when the set is empty */
    if (intrev32ifbe(is->length) == 0)
    {
        if (pos)
            *pos = 0;
        return 0;
    }
    else
    {
        /* Check for the case where we know we cannot find the value,
         * but do know the insert position. */
        if (value > _intsetGet(is, max))
        {
            if (pos)
                *pos = intrev32ifbe(is->length);
            return 0;
        }
        else if (value < _intsetGet(is, 0))
        {
            if (pos)
                *pos = 0;
            return 0;
        }
    }

    while (max >= min)
    {
        mid = ((unsigned int)min + (unsigned int)max) >> 1;
        cur = _intsetGet(is, mid);
        if (value > cur)
        {
            min = mid + 1;
        }
        else if (value < cur)
        {
            max = mid - 1;
        }
        else
        {
            break;
        }
    }

    if (value == cur)
    {
        if (pos)
            *pos = mid;
        return 1;
    }
    else
    {
        if (pos)
            *pos = min;
        return 0;
    }
}

/* Upgrades the intset to a larger encoding and inserts the given integer. */
/**
 * 将整数集合 is 的数据编码方式提升到 value 对应的编码方式（value 的编码方式要大于 is 的编码方式），然后将 value 插入到 is 的指定位置
 *
 * 调整编码方式的时候，要将 intset 中原有数据的 2 字节变成 4 字节, 4 字节变成 8 字节
 */
static intset *intsetUpgradeAndAdd(intset *is, int64_t value)
{
    uint8_t curenc = intrev32ifbe(is->encoding);  // 获取当前 intset 的编码方式
    uint8_t newenc = _intsetValueEncoding(value); // 获取要插入数据的编码方式
    int length = intrev32ifbe(is->length);        // 获取当前 intset 中保存的数据个数
    int prepend = value < 0 ? 1 : 0;              // 确定 value 要插入 intset 的头部或者尾部

    /* First set new encoding and resize */
    is->encoding = intrev32ifbe(newenc);                 // 将 intset 的编码方式设置为 value 的编码方式
    is = intsetResize(is, intrev32ifbe(is->length) + 1); // 按照新的编码方式对原有的 intset 进行扩容（+1 是因为要插入一个新的数据 value）

    /* Upgrade back-to-front so we don't overwrite values.
     * Note that the "prepend" variable is used to make sure we have an empty
     * space at either the beginning or the end of the intset. */
    // 逐个读取 intset 中的数据，按照新的编码方式，调整 intset 中原有数据的大小：从 2 字节变成 4 字节, 或者从 4 字节变成 8 字节
    while (length--)
        _intsetSet(is, length + prepend, _intsetGetEncoded(is, length, curenc));

    /* Set the value at the beginning or the end. */

    // value 一定会被插入整数集合 is 中的起始位置或者结束位置。这是因为 value 的编码方式大于目前整数集合中编码方式，
    // 也就是说，value 要么大于 intset 中的所有数据，要么小于 intset 中的所有数据
    if (prepend)
        _intsetSet(is, 0, value);
    else
        _intsetSet(is, intrev32ifbe(is->length), value);

    is->length = intrev32ifbe(intrev32ifbe(is->length) + 1);

    return is;
}

// 将整数集合 is 中从 from 开始的所有元素调用 memmove 函数向前或者向后移动到 to 这个位置
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to)
{
    void *src, *dst;
    uint32_t bytes = intrev32ifbe(is->length) - from;
    uint32_t encoding = intrev32ifbe(is->encoding);

    if (encoding == INTSET_ENC_INT64)
    {
        src = (int64_t *)is->contents + from;
        dst = (int64_t *)is->contents + to;
        bytes *= sizeof(int64_t);
    }
    else if (encoding == INTSET_ENC_INT32)
    {
        src = (int32_t *)is->contents + from;
        dst = (int32_t *)is->contents + to;
        bytes *= sizeof(int32_t);
    }
    else
    {
        src = (int16_t *)is->contents + from;
        dst = (int16_t *)is->contents + to;
        bytes *= sizeof(int16_t);
    }
    memmove(dst, src, bytes);
}

/* Insert an integer in the intset */
// 将 value 插入到整数集合 is 中, 结果保存到 success 的指针中
intset *intsetAdd(intset *is, int64_t value, uint8_t *success)
{
    uint8_t valenc = _intsetValueEncoding(value); // 获取 value 的编码类型
    uint32_t pos;
    if (success)
        *success = 1;

    /* Upgrade encoding if necessary. If we need to upgrade, we know that
     * this value should be either appended (if > 0) or prepended (if < 0),
     * because it lies outside the range of existing values. */
    // 判断要插入的 value 的编码类型是否大于整数集合的编码类型，如果大于的话，需要提升整数集合的编码类型
    if (valenc > intrev32ifbe(is->encoding))
    {
        /* This always succeeds, so we don't need to curry *success. */
        return intsetUpgradeAndAdd(is, value);
    }
    else
    {
        /* Abort if the value is already present in the set.
         * This call will populate "pos" with the right position to insert
         * the value when it cannot be found. */
        if (intsetSearch(is, value, &pos)) // 寻找 intset 中是否已经存在 value 了
        {
            if (success)
                *success = 0;
            return is;
        }

        is = intsetResize(is, intrev32ifbe(is->length) + 1); // 扩容 intset

        // 要插入的位置 pos 如果不是 intset 的尾部，需要将 pos 位置后面的所有元素向后移动一个元素的位置
        if (pos < intrev32ifbe(is->length))
            intsetMoveTail(is, pos, pos + 1);
    }

    _intsetSet(is, pos, value);                              // 插入 value
    is->length = intrev32ifbe(intrev32ifbe(is->length) + 1); // 设置 length
    return is;
}

/* Delete integer from intset */
// 从整数集合 is 中删除元素 value, 将结果保存到 success. 返回值为新的 intset 指针
intset *intsetRemove(intset *is, int64_t value, int *success)
{
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
    if (success)
        *success = 0;

    // 1. 如果要删除的 value 的编码大于整数集合的编码，说明 value 肯定不存在于整数集合中：删除失败
    // 2. 如果在整数集合 intset 中查找不到要删除的元素 value：删除失败
    if (valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, &pos))
    {
        uint32_t len = intrev32ifbe(is->length);

        /* We know we can delete */
        if (success)
            *success = 1;

        /* Overwrite value with tail and update length */
        if (pos < (len - 1))
            intsetMoveTail(is, pos + 1, pos);
        is = intsetResize(is, len - 1);
        is->length = intrev32ifbe(len - 1);
    }
    return is;
}

/* Determine whether a value belongs to this set */
uint8_t intsetFind(intset *is, int64_t value)
{
    uint8_t valenc = _intsetValueEncoding(value);
    return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, NULL);
}

/* Return random member */
int64_t intsetRandom(intset *is)
{
    return _intsetGet(is, rand() % intrev32ifbe(is->length));
}

/* Get the value at the given position. When this position is
 * out of range the function returns 0, when in range it returns 1. */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value)
{
    if (pos < intrev32ifbe(is->length))
    {
        *value = _intsetGet(is, pos);
        return 1;
    }
    return 0;
}

/* Return intset length */
uint32_t intsetLen(const intset *is)
{
    return intrev32ifbe(is->length);
}

/* Return intset blob size in bytes. */
size_t intsetBlobLen(intset *is)
{
    return sizeof(intset) + (size_t)intrev32ifbe(is->length) * intrev32ifbe(is->encoding);
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include <time.h>

#if 0
static void intsetRepr(intset *is) {
    for (uint32_t i = 0; i < intrev32ifbe(is->length); i++) {
        printf("%lld\n", (uint64_t)_intsetGet(is,i));
    }
    printf("\n");
}

static void error(char *err) {
    printf("%s\n", err);
    exit(1);
}
#endif

static void ok(void)
{
    printf("OK\n");
}

static long long usec(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec) * 1000000) + tv.tv_usec;
}

#define assert(_e) ((_e) ? (void)0 : (_assert(#_e, __FILE__, __LINE__), exit(1)))
static void _assert(char *estr, char *file, int line)
{
    printf("\n\n=== ASSERTION FAILED ===\n");
    printf("==> %s:%d '%s' is not true\n", file, line, estr);
}

static intset *createSet(int bits, int size)
{
    uint64_t mask = (1 << bits) - 1;
    uint64_t value;
    intset *is = intsetNew();

    for (int i = 0; i < size; i++)
    {
        if (bits > 32)
        {
            value = (rand() * rand()) & mask;
        }
        else
        {
            value = rand() & mask;
        }
        is = intsetAdd(is, value, NULL);
    }
    return is;
}

static void checkConsistency(intset *is)
{
    for (uint32_t i = 0; i < (intrev32ifbe(is->length) - 1); i++)
    {
        uint32_t encoding = intrev32ifbe(is->encoding);

        if (encoding == INTSET_ENC_INT16)
        {
            int16_t *i16 = (int16_t *)is->contents;
            assert(i16[i] < i16[i + 1]);
        }
        else if (encoding == INTSET_ENC_INT32)
        {
            int32_t *i32 = (int32_t *)is->contents;
            assert(i32[i] < i32[i + 1]);
        }
        else
        {
            int64_t *i64 = (int64_t *)is->contents;
            assert(i64[i] < i64[i + 1]);
        }
    }
}

#define UNUSED(x) (void)(x)
int intsetTest(int argc, char **argv)
{
    uint8_t success;
    int i;
    intset *is;
    srand(time(NULL));

    UNUSED(argc);
    UNUSED(argv);

    printf("Value encodings: ");
    {
        assert(_intsetValueEncoding(-32768) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(+32767) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(-32769) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+32768) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483648) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+2147483647) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483649) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+2147483648) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(-9223372036854775808ull) ==
               INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+9223372036854775807ull) ==
               INTSET_ENC_INT64);
        ok();
    }

    printf("Basic adding: ");
    {
        is = intsetNew();
        is = intsetAdd(is, 5, &success);
        assert(success);
        is = intsetAdd(is, 6, &success);
        assert(success);
        is = intsetAdd(is, 4, &success);
        assert(success);
        is = intsetAdd(is, 4, &success);
        assert(!success);
        ok();
    }

    printf("Large number of random adds: ");
    {
        uint32_t inserts = 0;
        is = intsetNew();
        for (i = 0; i < 1024; i++)
        {
            is = intsetAdd(is, rand() % 0x800, &success);
            if (success)
                inserts++;
        }
        assert(intrev32ifbe(is->length) == inserts);
        checkConsistency(is);
        ok();
    }

    printf("Upgrade from int16 to int32: ");
    {
        is = intsetNew();
        is = intsetAdd(is, 32, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is, 65535, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is, 32));
        assert(intsetFind(is, 65535));
        checkConsistency(is);

        is = intsetNew();
        is = intsetAdd(is, 32, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is, -65535, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is, 32));
        assert(intsetFind(is, -65535));
        checkConsistency(is);
        ok();
    }

    printf("Upgrade from int16 to int64: ");
    {
        is = intsetNew();
        is = intsetAdd(is, 32, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is, 4294967295, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is, 32));
        assert(intsetFind(is, 4294967295));
        checkConsistency(is);

        is = intsetNew();
        is = intsetAdd(is, 32, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is, -4294967295, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is, 32));
        assert(intsetFind(is, -4294967295));
        checkConsistency(is);
        ok();
    }

    printf("Upgrade from int32 to int64: ");
    {
        is = intsetNew();
        is = intsetAdd(is, 65535, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is, 4294967295, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is, 65535));
        assert(intsetFind(is, 4294967295));
        checkConsistency(is);

        is = intsetNew();
        is = intsetAdd(is, 65535, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is, -4294967295, NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is, 65535));
        assert(intsetFind(is, -4294967295));
        checkConsistency(is);
        ok();
    }

    printf("Stress lookups: ");
    {
        long num = 100000, size = 10000;
        int i, bits = 20;
        long long start;
        is = createSet(bits, size);
        checkConsistency(is);

        start = usec();
        for (i = 0; i < num; i++)
            intsetSearch(is, rand() % ((1 << bits) - 1), NULL);
        printf("%ld lookups, %ld element set, %lldusec\n",
               num, size, usec() - start);
    }

    printf("Stress add+delete: ");
    {
        int i, v1, v2;
        is = intsetNew();
        for (i = 0; i < 0xffff; i++)
        {
            v1 = rand() % 0xfff;
            is = intsetAdd(is, v1, NULL);
            assert(intsetFind(is, v1));

            v2 = rand() % 0xfff;
            is = intsetRemove(is, v2, NULL);
            assert(!intsetFind(is, v2));
        }
        checkConsistency(is);
        ok();
    }

    return 0;
}
#endif
