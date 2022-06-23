/* Listpack -- A lists of strings serialization format
 *
 * This file implements the specification you can find at:
 *
 *  https://github.com/antirez/listpack
 *
 * Copyright (c) 2017, Salvatore Sanfilippo <antirez at gmail dot com>
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
 * 紧凑列表可以被认为是对压缩链表的一种优化。对于压缩链表，曾经有 Redis 用户反馈在访问压缩链表的时候程序出现了崩溃，
 * Redis 的作者以及其他几个维护者在审核代码的过程中虽然没有查明用户所上报的崩溃的具体原因，但是意外的发现了另外一个由于
 * 压缩链表连锁更新所导致的 bug，作者在 c495d095ae495ea5253443ee4562aaa30681a854 这次提交中针对这个 bug 进行了修改。
 * 虽然没有找到引起崩溃的 bug，但是 Redis 的作者以及其他维护者一致认为压缩链表的结构所导致的连锁更新这一问题会增加压缩链表
 * 在实际应用之中的复杂性，因此他们认为需要设计一种替换压缩链表的紧凑型数据结构，而这便是紧凑列表产生的原因。
 *
 * 紧凑列表吸纳了压缩链表的优势同时又进行了重新实现，可以以更紧凑的方式来表示数据，同时还能够更加快速的对数据进行解析。
 * 重要的是，紧凑列表中单一节点的表示形式被重新设计以便更好地拓展这种数据形式。
 *
 * 紧凑列表的内存结构如下：
 *
 *      <lpbytes> <lplen> <entry> ... <entry> <lpend>
 *
 * uint32_t lpbytes:  表示 listpack 占用的字节总数。包含头部以及结尾终止符
 * uint16_t lplen:    表示 listpack 中数据节点的个数。最大能表示为 2^16-1，如果超过的话，需要遍历整个 listpack 才能获得真实的节点数量
 * uint8_t lpend:     0xFF，用来标记紧凑列表的结束
 *
 * NOTE: 和 ziplist 相比，缺少了最后一个节点的的偏移字段 zltail。
 *
 * 紧凑列表中的节点 entry 的内存结构如下：
 *
 *      <encoding> <entry-data> <entry-tot-len>
 *
 * 和 ziplist 类似，对于一些小整数，我们可以直接保存在 encoding 中，而不用 <entry-data> 字段，所以内存结构如下：
 *
 *      <encoding> <entry-tot-len>
 *
 * NOTE: 这两个字段都属于变长字段。
 *
 * <encoding>: 标记该节点的编码方式。按照整型编码或者按照字符串编码。当节点使用的是字符串编码时，该字段还会指明字符串数据的字节长度。如果当前节点的数据是一个小整数，则本字段还可以存储这个数据信息。
 *
 *  1. 以下三种 <encoding> 字段表示当前节点的数据是按照字符串进行编码的，此时 <encoding> 属于变长字段：
 *     [10XXXXXX] <encoding> 字段占 1 个字节，使用 <encoding> 字段后面的 6 个比特位来存储字符串长度。最多可以表示长度不超过 2^6-1 的字符串
 *     [1110XXXX] <encoding> 字段占 2 个字节，需要使用 <encoding> 的后 4 个比特位以及额外的 1 个字节。最多可以表示长度不超过 2^12-1 的字符串
 *     [11110000] <encoding> 字段占 5 个字节，需要额外使用 4 个字节来存储字符串的长度。最多可以表示长度不超过 2^32-1 的字符串
 *
 *  2. 以下六种 <encoding> 字段表示节点的数据是按照位整数来进行编码的，此时 <encoding> 字段只占 1 个字节：
 *     [0XXXXXXX] 表示节点的数据是按照 7 位整数来进行编码的，直接使用 <encoding> 字段后面的 7 个比特位来存储数据
 *     [110XXXXX] 表示节点的数据是使用 13 位整数来进行编码的，需要使用 <encoding> 字段的后 5 个比特位以及额外的 1 个字节来存储数据
 *     [11110001] 表明节点的数据是按照 16 位整数进行编码的，需要额外的 2 个字节作为 <entry-data> 来存储数据
 *     [11110010] 表明节点的数据是按照 24 位整数进行编码的，需要额外的 3 个字节作为 <entry-data> 来存储数据
 *     [11110011] 表明节点的数据是按照 32 位整数进行编码的，需要额外的 4 个字节作为 <entry-data> 来存储数据
 *     [11110100] 表明节点的数据是按照 64 位整数进行编码的，需要额外的 8 个字节作为 <entry-data> 来存储数据
 *
 *  redis 定义了一系列宏来使用 <encoding> 字段:
 *
 *     LP_ENCODING_6BIT_STR         [10XXXXXX]
 *     LP_ENCODING_12BIT_STR        [1110XXXX]
 *     LP_ENCODING_32BIT_STR        [11110000]
 *
 *     LP_ENCODING_7BIT_UINT        [0XXXXXXX]
 *     LP_ENCODING_13BIT_INT        [110XXXXX]
 *     LP_ENCODING_16BIT_INT        [11110001]
 *     LP_ENCODING_24BIT_INT        [11110010]
 *     LP_ENCODING_32BIT_INT        [11110011]
 *     LP_ENCODING_64BIT_INT        [11110100]
 *
 *
 * <entry-tot-len>: 表示节点的 <encoding> 字段和 <entry-data> 两个字段的总长度
 *
 *     [0XXXXXXX]  <entry-tot-len> 字段占 1 个字节，可表示的长度范围是 [1, 2^7-1] --> [1, 127]
 *     [0XXXXXXX][1XXXXXXXX]  <entry-tot-len> 字段占 2 个字节，可表示的长度范围是 [2^7, 2^14-1] --> [128, 16383]
 *     [0XXXXXXX][1XXXXXXX][1XXXXXXX]  <entry-tot-len> 字段占 3 个字节，可表示的长度范围是 [2^14, 2^21-1] --> [16384, 2097151]
 *     [0XXXXXXX][1XXXXXXX][1XXXXXXX][1XXXXXXX]  <entry-tot-len> 字段占 4 个字节，可表示的长度范围是 [2^21, 2^28-1] --> [2097152, 268435455]
 *     [0XXXXXXX][1XXXXXXX][1XXXXXXX][1XXXXXXX][1XXXXXXX]  <entry-tot-len> 字段占 5 个字节，可表示的长度范围是 [2^28, 2^35-1]
 *
 * redis 采用了一种特殊的方式来设计这个字段：
 *  1. 只使用每个字节低位的 7 个比特位来存储数据，而高位的 1 个比特位用来标记是否达到了字段的起始位置。
 *  2. 长度数据编码的高位存储在这个字段的低位字节上，而编码的低位则存储在该字段的高位字节上。为了方便反向遍历时，从高到低地遍历字节，以获取长度编码数据
 *
 * 通过这种设计，便可以实现对给定元素节点的反向遍历，比如给定紧凑列表指针 lp 以及元素节点指针 p, 那么这个反向遍历的过程是：
 *  1. 如果 lp 指针与 p 正好相差 6 个字节（也就是紧凑列表头部数据的长度），那么说明 p 对应的节点为列表的第一个节点，无法反向遍历。
 *  2. 如果不是情况 1，那么将 p 指针减去 1 个字节，使之指向前一个节点 <entry-tot-len> 的最后一个字节
 *  3. 用 p 指针向前依次扫描每一个字节，直到找到高位比特为 0 的字节，并将中间扫描的数据进行累加，计算出前一个节点 <encoding> 以及 <entry-data> 这两个字段的长度
 *
 * NOTE: listpack 的 <entry-tot-len> 字段和 ziplist 的 <prevlen> 字段含义不同。压缩链表 ziplist 中，每一个节点的元素还包含前一个节点的长度数据，正是这种设计，导致在对 ziplist 的节点进行操作时，可能引发连锁更新。在紧凑列表 listpack 中，每个节点元素只包含和该节点有关的数据，无论插入新节点还是删除老节点，都不会产生连锁更新的问题。
 *
 */

#include <stdint.h>
#include <limits.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "listpack.h"
#include "listpack_malloc.h"

#define LP_HDR_SIZE 6 /* 32 bit total len + 16 bit number of elements. */
#define LP_HDR_NUMELE_UNKNOWN UINT16_MAX
#define LP_MAX_INT_ENCODING_LEN 9
#define LP_MAX_BACKLEN_SIZE 5
#define LP_MAX_ENTRY_BACKLEN 34359738367ULL
#define LP_ENCODING_INT 0
#define LP_ENCODING_STRING 1

#define LP_ENCODING_7BIT_UINT 0
#define LP_ENCODING_7BIT_UINT_MASK 0x80
#define LP_ENCODING_IS_7BIT_UINT(byte) (((byte)&LP_ENCODING_7BIT_UINT_MASK) == LP_ENCODING_7BIT_UINT)

#define LP_ENCODING_6BIT_STR 0x80
#define LP_ENCODING_6BIT_STR_MASK 0xC0
#define LP_ENCODING_IS_6BIT_STR(byte) (((byte)&LP_ENCODING_6BIT_STR_MASK) == LP_ENCODING_6BIT_STR)

#define LP_ENCODING_13BIT_INT 0xC0
#define LP_ENCODING_13BIT_INT_MASK 0xE0
#define LP_ENCODING_IS_13BIT_INT(byte) (((byte)&LP_ENCODING_13BIT_INT_MASK) == LP_ENCODING_13BIT_INT)

#define LP_ENCODING_12BIT_STR 0xE0
#define LP_ENCODING_12BIT_STR_MASK 0xF0
#define LP_ENCODING_IS_12BIT_STR(byte) (((byte)&LP_ENCODING_12BIT_STR_MASK) == LP_ENCODING_12BIT_STR)

#define LP_ENCODING_16BIT_INT 0xF1
#define LP_ENCODING_16BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_16BIT_INT(byte) (((byte)&LP_ENCODING_16BIT_INT_MASK) == LP_ENCODING_16BIT_INT)

#define LP_ENCODING_24BIT_INT 0xF2
#define LP_ENCODING_24BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_24BIT_INT(byte) (((byte)&LP_ENCODING_24BIT_INT_MASK) == LP_ENCODING_24BIT_INT)

#define LP_ENCODING_32BIT_INT 0xF3
#define LP_ENCODING_32BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_32BIT_INT(byte) (((byte)&LP_ENCODING_32BIT_INT_MASK) == LP_ENCODING_32BIT_INT)

#define LP_ENCODING_64BIT_INT 0xF4
#define LP_ENCODING_64BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_64BIT_INT(byte) (((byte)&LP_ENCODING_64BIT_INT_MASK) == LP_ENCODING_64BIT_INT)

#define LP_ENCODING_32BIT_STR 0xF0
#define LP_ENCODING_32BIT_STR_MASK 0xFF
#define LP_ENCODING_IS_32BIT_STR(byte) (((byte)&LP_ENCODING_32BIT_STR_MASK) == LP_ENCODING_32BIT_STR)

#define LP_EOF 0xFF

#define LP_ENCODING_6BIT_STR_LEN(p) ((p)[0] & 0x3F)
#define LP_ENCODING_12BIT_STR_LEN(p) ((((p)[0] & 0xF) << 8) | (p)[1])
#define LP_ENCODING_32BIT_STR_LEN(p) (((uint32_t)(p)[1] << 0) |  \
                                      ((uint32_t)(p)[2] << 8) |  \
                                      ((uint32_t)(p)[3] << 16) | \
                                      ((uint32_t)(p)[4] << 24))

#define lpGetTotalBytes(p) (((uint32_t)(p)[0] << 0) |  \
                            ((uint32_t)(p)[1] << 8) |  \
                            ((uint32_t)(p)[2] << 16) | \
                            ((uint32_t)(p)[3] << 24))

#define lpGetNumElements(p) (((uint32_t)(p)[4] << 0) | \
                             ((uint32_t)(p)[5] << 8))
#define lpSetTotalBytes(p, v)        \
    do                               \
    {                                \
        (p)[0] = (v)&0xff;           \
        (p)[1] = ((v) >> 8) & 0xff;  \
        (p)[2] = ((v) >> 16) & 0xff; \
        (p)[3] = ((v) >> 24) & 0xff; \
    } while (0)

#define lpSetNumElements(p, v)      \
    do                              \
    {                               \
        (p)[4] = (v)&0xff;          \
        (p)[5] = ((v) >> 8) & 0xff; \
    } while (0)

/* Convert a string into a signed 64 bit integer.
 * The function returns 1 if the string could be parsed into a (non-overflowing)
 * signed 64 bit int, 0 otherwise. The 'value' will be set to the parsed value
 * when the function returns success.
 *
 * Note that this function demands that the string strictly represents
 * a int64 value: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. *
 *
 * -----------------------------------------------------------------------------
 *
 * Credits: this function was adapted from the Redis source code, file
 * "utils.c", function string2ll(), and is copyright:
 *
 * Copyright(C) 2011, Pieter Noordhuis
 * Copyright(C) 2011, Salvatore Sanfilippo
 *
 * The function is released under the BSD 3-clause license.
 */
int lpStringToInt64(const char *s, unsigned long slen, int64_t *value)
{
    const char *p = s;
    unsigned long plen = 0;
    int negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0')
    {
        if (value != NULL)
            *value = 0;
        return 1;
    }

    if (p[0] == '-')
    {
        negative = 1;
        p++;
        plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9')
    {
        v = p[0] - '0';
        p++;
        plen++;
    }
    else if (p[0] == '0' && slen == 1)
    {
        *value = 0;
        return 1;
    }
    else
    {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9')
    {
        if (v > (UINT64_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (UINT64_MAX - (p[0] - '0'))) /* Overflow. */
            return 0;
        v += p[0] - '0';

        p++;
        plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative)
    {
        if (v > ((uint64_t)(-(INT64_MIN + 1)) + 1)) /* Overflow. */
            return 0;
        if (value != NULL)
            *value = -v;
    }
    else
    {
        if (v > INT64_MAX) /* Overflow. */
            return 0;
        if (value != NULL)
            *value = v;
    }
    return 1;
}

/* Create a new, empty listpack.
 * On success the new listpack is returned, otherwise an error is returned. */
unsigned char *lpNew(void)
{
    unsigned char *lp = lp_malloc(LP_HDR_SIZE + 1);
    if (lp == NULL)
        return NULL;
    lpSetTotalBytes(lp, LP_HDR_SIZE + 1);
    lpSetNumElements(lp, 0);
    lp[LP_HDR_SIZE] = LP_EOF;
    return lp;
}

/* Free the specified listpack. */
void lpFree(unsigned char *lp)
{
    lp_free(lp);
}

/* Given an element 'ele' of size 'size', determine if the element can be
 * represented inside the listpack encoded as integer, and returns
 * LP_ENCODING_INT if so. Otherwise returns LP_ENCODING_STR if no integer
 * encoding is possible.
 *
 * If the LP_ENCODING_INT is returned, the function stores the integer encoded
 * representation of the element in the 'intenc' buffer.
 *
 * Regardless of the returned encoding, 'enclen' is populated by reference to
 * the number of bytes that the string or integer encoded element will require
 * in order to be represented. */
int lpEncodeGetType(unsigned char *ele, uint32_t size, unsigned char *intenc, uint64_t *enclen)
{
    int64_t v;
    if (lpStringToInt64((const char *)ele, size, &v))
    {
        if (v >= 0 && v <= 127)
        {
            /* Single byte 0-127 integer. */
            intenc[0] = v;
            *enclen = 1;
        }
        else if (v >= -4096 && v <= 4095)
        {
            /* 13 bit integer. */
            if (v < 0)
                v = ((int64_t)1 << 13) + v;
            intenc[0] = (v >> 8) | LP_ENCODING_13BIT_INT;
            intenc[1] = v & 0xff;
            *enclen = 2;
        }
        else if (v >= -32768 && v <= 32767)
        {
            /* 16 bit integer. */
            if (v < 0)
                v = ((int64_t)1 << 16) + v;
            intenc[0] = LP_ENCODING_16BIT_INT;
            intenc[1] = v & 0xff;
            intenc[2] = v >> 8;
            *enclen = 3;
        }
        else if (v >= -8388608 && v <= 8388607)
        {
            /* 24 bit integer. */
            if (v < 0)
                v = ((int64_t)1 << 24) + v;
            intenc[0] = LP_ENCODING_24BIT_INT;
            intenc[1] = v & 0xff;
            intenc[2] = (v >> 8) & 0xff;
            intenc[3] = v >> 16;
            *enclen = 4;
        }
        else if (v >= -2147483648 && v <= 2147483647)
        {
            /* 32 bit integer. */
            if (v < 0)
                v = ((int64_t)1 << 32) + v;
            intenc[0] = LP_ENCODING_32BIT_INT;
            intenc[1] = v & 0xff;
            intenc[2] = (v >> 8) & 0xff;
            intenc[3] = (v >> 16) & 0xff;
            intenc[4] = v >> 24;
            *enclen = 5;
        }
        else
        {
            /* 64 bit integer. */
            uint64_t uv = v;
            intenc[0] = LP_ENCODING_64BIT_INT;
            intenc[1] = uv & 0xff;
            intenc[2] = (uv >> 8) & 0xff;
            intenc[3] = (uv >> 16) & 0xff;
            intenc[4] = (uv >> 24) & 0xff;
            intenc[5] = (uv >> 32) & 0xff;
            intenc[6] = (uv >> 40) & 0xff;
            intenc[7] = (uv >> 48) & 0xff;
            intenc[8] = uv >> 56;
            *enclen = 9;
        }
        return LP_ENCODING_INT;
    }
    else
    {
        if (size < 64)
            *enclen = 1 + size;
        else if (size < 4096)
            *enclen = 2 + size;
        else
            *enclen = 5 + (uint64_t)size;
        return LP_ENCODING_STRING;
    }
}

/* Store a reverse-encoded variable length field, representing the length
 * of the previous element of size 'l', in the target buffer 'buf'.
 * The function returns the number of bytes used to encode it, from
 * 1 to 5. If 'buf' is NULL the function just returns the number of bytes
 * needed in order to encode the backlen. */
unsigned long lpEncodeBacklen(unsigned char *buf, uint64_t l)
{
    if (l <= 127)
    {
        if (buf)
            buf[0] = l;
        return 1;
    }
    else if (l < 16383)
    {
        if (buf)
        {
            buf[0] = l >> 7;
            buf[1] = (l & 127) | 128;
        }
        return 2;
    }
    else if (l < 2097151)
    {
        if (buf)
        {
            buf[0] = l >> 14;
            buf[1] = ((l >> 7) & 127) | 128;
            buf[2] = (l & 127) | 128;
        }
        return 3;
    }
    else if (l < 268435455)
    {
        if (buf)
        {
            buf[0] = l >> 21;
            buf[1] = ((l >> 14) & 127) | 128;
            buf[2] = ((l >> 7) & 127) | 128;
            buf[3] = (l & 127) | 128;
        }
        return 4;
    }
    else
    {
        if (buf)
        {
            buf[0] = l >> 28;
            buf[1] = ((l >> 21) & 127) | 128;
            buf[2] = ((l >> 14) & 127) | 128;
            buf[3] = ((l >> 7) & 127) | 128;
            buf[4] = (l & 127) | 128;
        }
        return 5;
    }
}

/* Decode the backlen and returns it. If the encoding looks invalid (more than
 * 5 bytes are used), UINT64_MAX is returned to report the problem. */
uint64_t lpDecodeBacklen(unsigned char *p)
{
    uint64_t val = 0;
    uint64_t shift = 0;
    do
    {
        val |= (uint64_t)(p[0] & 127) << shift;
        if (!(p[0] & 128))
            break;
        shift += 7;
        p--;
        if (shift > 28)
            return UINT64_MAX;
    } while (1);
    return val;
}

/* Encode the string element pointed by 's' of size 'len' in the target
 * buffer 's'. The function should be called with 'buf' having always enough
 * space for encoding the string. This is done by calling lpEncodeGetType()
 * before calling this function. */
void lpEncodeString(unsigned char *buf, unsigned char *s, uint32_t len)
{
    if (len < 64)
    {
        buf[0] = len | LP_ENCODING_6BIT_STR;
        memcpy(buf + 1, s, len);
    }
    else if (len < 4096)
    {
        buf[0] = (len >> 8) | LP_ENCODING_12BIT_STR;
        buf[1] = len & 0xff;
        memcpy(buf + 2, s, len);
    }
    else
    {
        buf[0] = LP_ENCODING_32BIT_STR;
        buf[1] = len & 0xff;
        buf[2] = (len >> 8) & 0xff;
        buf[3] = (len >> 16) & 0xff;
        buf[4] = (len >> 24) & 0xff;
        memcpy(buf + 5, s, len);
    }
}

/* Return the encoded length of the listpack element pointed by 'p'. If the
 * element encoding is wrong then 0 is returned. */
uint32_t lpCurrentEncodedSize(unsigned char *p)
{
    if (LP_ENCODING_IS_7BIT_UINT(p[0]))
        return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0]))
        return 1 + LP_ENCODING_6BIT_STR_LEN(p);
    if (LP_ENCODING_IS_13BIT_INT(p[0]))
        return 2;
    if (LP_ENCODING_IS_16BIT_INT(p[0]))
        return 3;
    if (LP_ENCODING_IS_24BIT_INT(p[0]))
        return 4;
    if (LP_ENCODING_IS_32BIT_INT(p[0]))
        return 5;
    if (LP_ENCODING_IS_64BIT_INT(p[0]))
        return 9;
    if (LP_ENCODING_IS_12BIT_STR(p[0]))
        return 2 + LP_ENCODING_12BIT_STR_LEN(p);
    if (LP_ENCODING_IS_32BIT_STR(p[0]))
        return 5 + LP_ENCODING_32BIT_STR_LEN(p);
    if (p[0] == LP_EOF)
        return 1;
    return 0;
}

/* Skip the current entry returning the next. It is invalid to call this
 * function if the current element is the EOF element at the end of the
 * listpack, however, while this function is used to implement lpNext(),
 * it does not return NULL when the EOF element is encountered. */
unsigned char *lpSkip(unsigned char *p)
{
    unsigned long entrylen = lpCurrentEncodedSize(p);
    entrylen += lpEncodeBacklen(NULL, entrylen);
    p += entrylen;
    return p;
}

/* If 'p' points to an element of the listpack, calling lpNext() will return
 * the pointer to the next element (the one on the right), or NULL if 'p'
 * already pointed to the last element of the listpack. */
unsigned char *lpNext(unsigned char *lp, unsigned char *p)
{
    ((void)lp); /* lp is not used for now. However lpPrev() uses it. */
    p = lpSkip(p);
    if (p[0] == LP_EOF)
        return NULL;
    return p;
}

/* If 'p' points to an element of the listpack, calling lpPrev() will return
 * the pointer to the previous element (the one on the left), or NULL if 'p'
 * already pointed to the first element of the listpack. */
unsigned char *lpPrev(unsigned char *lp, unsigned char *p)
{
    if (p - lp == LP_HDR_SIZE)
        return NULL;
    p--; /* Seek the first backlen byte of the last element. */
    uint64_t prevlen = lpDecodeBacklen(p);
    prevlen += lpEncodeBacklen(NULL, prevlen);
    return p - prevlen + 1; /* Seek the first byte of the previous entry. */
}

/* Return a pointer to the first element of the listpack, or NULL if the
 * listpack has no elements. */
unsigned char *lpFirst(unsigned char *lp)
{
    lp += LP_HDR_SIZE; /* Skip the header. */
    if (lp[0] == LP_EOF)
        return NULL;
    return lp;
}

/* Return a pointer to the last element of the listpack, or NULL if the
 * listpack has no elements. */
unsigned char *lpLast(unsigned char *lp)
{
    unsigned char *p = lp + lpGetTotalBytes(lp) - 1; /* Seek EOF element. */
    return lpPrev(lp, p);                            /* Will return NULL if EOF is the only element. */
}

/* Return the number of elements inside the listpack. This function attempts
 * to use the cached value when within range, otherwise a full scan is
 * needed. As a side effect of calling this function, the listpack header
 * could be modified, because if the count is found to be already within
 * the 'numele' header field range, the new value is set. */
uint32_t lpLength(unsigned char *lp)
{
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN)
        return numele;

    /* Too many elements inside the listpack. We need to scan in order
     * to get the total number. */
    uint32_t count = 0;
    unsigned char *p = lpFirst(lp);
    while (p)
    {
        count++;
        p = lpNext(lp, p);
    }

    /* If the count is again within range of the header numele field,
     * set it. */
    if (count < LP_HDR_NUMELE_UNKNOWN)
        lpSetNumElements(lp, count);
    return count;
}

/* Return the listpack element pointed by 'p'.
 *
 * The function changes behavior depending on the passed 'intbuf' value.
 * Specifically, if 'intbuf' is NULL:
 *
 * If the element is internally encoded as an integer, the function returns
 * NULL and populates the integer value by reference in 'count'. Otherwise if
 * the element is encoded as a string a pointer to the string (pointing inside
 * the listpack itself) is returned, and 'count' is set to the length of the
 * string.
 *
 * If instead 'intbuf' points to a buffer passed by the caller, that must be
 * at least LP_INTBUF_SIZE bytes, the function always returns the element as
 * it was a string (returning the pointer to the string and setting the
 * 'count' argument to the string length by reference). However if the element
 * is encoded as an integer, the 'intbuf' buffer is used in order to store
 * the string representation.
 *
 * The user should use one or the other form depending on what the value will
 * be used for. If there is immediate usage for an integer value returned
 * by the function, than to pass a buffer (and convert it back to a number)
 * is of course useless.
 *
 * If the function is called against a badly encoded ziplist, so that there
 * is no valid way to parse it, the function returns like if there was an
 * integer encoded with value 12345678900000000 + <unrecognized byte>, this may
 * be an hint to understand that something is wrong. To crash in this case is
 * not sensible because of the different requirements of the application using
 * this lib.
 *
 * Similarly, there is no error returned since the listpack normally can be
 * assumed to be valid, so that would be a very high API cost. However a function
 * in order to check the integrity of the listpack at load time is provided,
 * check lpIsValid(). */
unsigned char *lpGet(unsigned char *p, int64_t *count, unsigned char *intbuf)
{
    int64_t val;
    uint64_t uval, negstart, negmax;

    if (LP_ENCODING_IS_7BIT_UINT(p[0]))
    {
        negstart = UINT64_MAX; /* 7 bit ints are always positive. */
        negmax = 0;
        uval = p[0] & 0x7f;
    }
    else if (LP_ENCODING_IS_6BIT_STR(p[0]))
    {
        *count = LP_ENCODING_6BIT_STR_LEN(p);
        return p + 1;
    }
    else if (LP_ENCODING_IS_13BIT_INT(p[0]))
    {
        uval = ((p[0] & 0x1f) << 8) | p[1];
        negstart = (uint64_t)1 << 12;
        negmax = 8191;
    }
    else if (LP_ENCODING_IS_16BIT_INT(p[0]))
    {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2] << 8;
        negstart = (uint64_t)1 << 15;
        negmax = UINT16_MAX;
    }
    else if (LP_ENCODING_IS_24BIT_INT(p[0]))
    {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2] << 8 |
               (uint64_t)p[3] << 16;
        negstart = (uint64_t)1 << 23;
        negmax = UINT32_MAX >> 8;
    }
    else if (LP_ENCODING_IS_32BIT_INT(p[0]))
    {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2] << 8 |
               (uint64_t)p[3] << 16 |
               (uint64_t)p[4] << 24;
        negstart = (uint64_t)1 << 31;
        negmax = UINT32_MAX;
    }
    else if (LP_ENCODING_IS_64BIT_INT(p[0]))
    {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2] << 8 |
               (uint64_t)p[3] << 16 |
               (uint64_t)p[4] << 24 |
               (uint64_t)p[5] << 32 |
               (uint64_t)p[6] << 40 |
               (uint64_t)p[7] << 48 |
               (uint64_t)p[8] << 56;
        negstart = (uint64_t)1 << 63;
        negmax = UINT64_MAX;
    }
    else if (LP_ENCODING_IS_12BIT_STR(p[0]))
    {
        *count = LP_ENCODING_12BIT_STR_LEN(p);
        return p + 2;
    }
    else if (LP_ENCODING_IS_32BIT_STR(p[0]))
    {
        *count = LP_ENCODING_32BIT_STR_LEN(p);
        return p + 5;
    }
    else
    {
        uval = 12345678900000000ULL + p[0];
        negstart = UINT64_MAX;
        negmax = 0;
    }

    /* We reach this code path only for integer encodings.
     * Convert the unsigned value to the signed one using two's complement
     * rule. */
    if (uval >= negstart)
    {
        /* This three steps conversion should avoid undefined behaviors
         * in the unsigned -> signed conversion. */
        uval = negmax - uval;
        val = uval;
        val = -val - 1;
    }
    else
    {
        val = uval;
    }

    /* Return the string representation of the integer or the value itself
     * depending on intbuf being NULL or not. */
    if (intbuf)
    {
        *count = snprintf((char *)intbuf, LP_INTBUF_SIZE, "%lld", (long long)val);
        return intbuf;
    }
    else
    {
        *count = val;
        return NULL;
    }
}

/* Insert, delete or replace the specified element 'ele' of length 'len' at
 * the specified position 'p', with 'p' being a listpack element pointer
 * obtained with lpFirst(), lpLast(), lpIndex(), lpNext(), lpPrev() or
 * lpSeek().
 *
 * The element is inserted before, after, or replaces the element pointed
 * by 'p' depending on the 'where' argument, that can be LP_BEFORE, LP_AFTER
 * or LP_REPLACE.
 *
 * If 'ele' is set to NULL, the function removes the element pointed by 'p'
 * instead of inserting one.
 *
 * Returns NULL on out of memory or when the listpack total length would exceed
 * the max allowed size of 2^32-1, otherwise the new pointer to the listpack
 * holding the new element is returned (and the old pointer passed is no longer
 * considered valid)
 *
 * If 'newp' is not NULL, at the end of a successful call '*newp' will be set
 * to the address of the element just added, so that it will be possible to
 * continue an interation with lpNext() and lpPrev().
 *
 * For deletion operations ('ele' set to NULL) 'newp' is set to the next
 * element, on the right of the deleted one, or to NULL if the deleted element
 * was the last one. */
unsigned char *lpInsert(unsigned char *lp, unsigned char *ele, uint32_t size, unsigned char *p, int where, unsigned char **newp)
{
    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
    unsigned char backlen[LP_MAX_BACKLEN_SIZE];

    uint64_t enclen; /* The length of the encoded element. */

    /* An element pointer set to NULL means deletion, which is conceptually
     * replacing the element with a zero-length element. So whatever we
     * get passed as 'where', set it to LP_REPLACE. */
    if (ele == NULL)
        where = LP_REPLACE;

    /* If we need to insert after the current element, we just jump to the
     * next element (that could be the EOF one) and handle the case of
     * inserting before. So the function will actually deal with just two
     * cases: LP_BEFORE and LP_REPLACE. */
    if (where == LP_AFTER)
    {
        p = lpSkip(p);
        where = LP_BEFORE;
    }

    /* Store the offset of the element 'p', so that we can obtain its
     * address again after a reallocation. */
    unsigned long poff = p - lp;

    /* Calling lpEncodeGetType() results into the encoded version of the
     * element to be stored into 'intenc' in case it is representable as
     * an integer: in that case, the function returns LP_ENCODING_INT.
     * Otherwise if LP_ENCODING_STR is returned, we'll have to call
     * lpEncodeString() to actually write the encoded string on place later.
     *
     * Whatever the returned encoding is, 'enclen' is populated with the
     * length of the encoded element. */
    int enctype;
    if (ele)
    {
        enctype = lpEncodeGetType(ele, size, intenc, &enclen);
    }
    else
    {
        enctype = -1;
        enclen = 0;
    }

    /* We need to also encode the backward-parsable length of the element
     * and append it to the end: this allows to traverse the listpack from
     * the end to the start. */
    unsigned long backlen_size = ele ? lpEncodeBacklen(backlen, enclen) : 0;
    uint64_t old_listpack_bytes = lpGetTotalBytes(lp);
    uint32_t replaced_len = 0;
    if (where == LP_REPLACE)
    {
        replaced_len = lpCurrentEncodedSize(p);
        replaced_len += lpEncodeBacklen(NULL, replaced_len);
    }

    uint64_t new_listpack_bytes = old_listpack_bytes + enclen + backlen_size - replaced_len;
    if (new_listpack_bytes > UINT32_MAX)
        return NULL;

    /* We now need to reallocate in order to make space or shrink the
     * allocation (in case 'when' value is LP_REPLACE and the new element is
     * smaller). However we do that before memmoving the memory to
     * make room for the new element if the final allocation will get
     * larger, or we do it after if the final allocation will get smaller. */

    unsigned char *dst = lp + poff; /* May be updated after reallocation. */

    /* Realloc before: we need more room. */
    if (new_listpack_bytes > old_listpack_bytes)
    {
        if ((lp = lp_realloc(lp, new_listpack_bytes)) == NULL)
            return NULL;
        dst = lp + poff;
    }

    /* Setup the listpack relocating the elements to make the exact room
     * we need to store the new one. */
    if (where == LP_BEFORE)
    {
        memmove(dst + enclen + backlen_size, dst, old_listpack_bytes - poff);
    }
    else
    { /* LP_REPLACE. */
        long lendiff = (enclen + backlen_size) - replaced_len;
        memmove(dst + replaced_len + lendiff,
                dst + replaced_len,
                old_listpack_bytes - poff - replaced_len);
    }

    /* Realloc after: we need to free space. */
    if (new_listpack_bytes < old_listpack_bytes)
    {
        if ((lp = lp_realloc(lp, new_listpack_bytes)) == NULL)
            return NULL;
        dst = lp + poff;
    }

    /* Store the entry. */
    if (newp)
    {
        *newp = dst;
        /* In case of deletion, set 'newp' to NULL if the next element is
         * the EOF element. */
        if (!ele && dst[0] == LP_EOF)
            *newp = NULL;
    }
    if (ele)
    {
        if (enctype == LP_ENCODING_INT)
        {
            memcpy(dst, intenc, enclen);
        }
        else
        {
            lpEncodeString(dst, ele, size);
        }
        dst += enclen;
        memcpy(dst, backlen, backlen_size);
        dst += backlen_size;
    }

    /* Update header. */
    if (where != LP_REPLACE || ele == NULL)
    {
        uint32_t num_elements = lpGetNumElements(lp);
        if (num_elements != LP_HDR_NUMELE_UNKNOWN)
        {
            if (ele)
                lpSetNumElements(lp, num_elements + 1);
            else
                lpSetNumElements(lp, num_elements - 1);
        }
    }
    lpSetTotalBytes(lp, new_listpack_bytes);

#if 0
    /* This code path is normally disabled: what it does is to force listpack
     * to return *always* a new pointer after performing some modification to
     * the listpack, even if the previous allocation was enough. This is useful
     * in order to spot bugs in code using listpacks: by doing so we can find
     * if the caller forgets to set the new pointer where the listpack reference
     * is stored, after an update. */
    unsigned char *oldlp = lp;
    lp = lp_malloc(new_listpack_bytes);
    memcpy(lp,oldlp,new_listpack_bytes);
    if (newp) {
        unsigned long offset = (*newp)-oldlp;
        *newp = lp + offset;
    }
    /* Make sure the old allocation contains garbage. */
    memset(oldlp,'A',new_listpack_bytes);
    lp_free(oldlp);
#endif

    return lp;
}

/* Append the specified element 'ele' of length 'len' at the end of the
 * listpack. It is implemented in terms of lpInsert(), so the return value is
 * the same as lpInsert(). */
unsigned char *lpAppend(unsigned char *lp, unsigned char *ele, uint32_t size)
{
    uint64_t listpack_bytes = lpGetTotalBytes(lp);
    unsigned char *eofptr = lp + listpack_bytes - 1;
    return lpInsert(lp, ele, size, eofptr, LP_BEFORE, NULL);
}

/* Remove the element pointed by 'p', and return the resulting listpack.
 * If 'newp' is not NULL, the next element pointer (to the right of the
 * deleted one) is returned by reference. If the deleted element was the
 * last one, '*newp' is set to NULL. */
unsigned char *lpDelete(unsigned char *lp, unsigned char *p, unsigned char **newp)
{
    return lpInsert(lp, NULL, 0, p, LP_REPLACE, newp);
}

/* Return the total number of bytes the listpack is composed of. */
uint32_t lpBytes(unsigned char *lp)
{
    return lpGetTotalBytes(lp);
}

/* Seek the specified element and returns the pointer to the seeked element.
 * Positive indexes specify the zero-based element to seek from the head to
 * the tail, negative indexes specify elements starting from the tail, where
 * -1 means the last element, -2 the penultimate and so forth. If the index
 * is out of range, NULL is returned. */
unsigned char *lpSeek(unsigned char *lp, long index)
{
    int forward = 1; /* Seek forward by default. */

    /* We want to seek from left to right or the other way around
     * depending on the listpack length and the element position.
     * However if the listpack length cannot be obtained in constant time,
     * we always seek from left to right. */
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN)
    {
        if (index < 0)
            index = (long)numele + index;
        if (index < 0)
            return NULL; /* Index still < 0 means out of range. */
        if (index >= (long)numele)
            return NULL; /* Out of range the other side. */
        /* We want to scan right-to-left if the element we are looking for
         * is past the half of the listpack. */
        if (index > (long)numele / 2)
        {
            forward = 0;
            /* Right to left scanning always expects a negative index. Convert
             * our index to negative form. */
            index -= numele;
        }
    }
    else
    {
        /* If the listpack length is unspecified, for negative indexes we
         * want to always scan right-to-left. */
        if (index < 0)
            forward = 0;
    }

    /* Forward and backward scanning is trivially based on lpNext()/lpPrev(). */
    if (forward)
    {
        unsigned char *ele = lpFirst(lp);
        while (index > 0 && ele)
        {
            ele = lpNext(lp, ele);
            index--;
        }
        return ele;
    }
    else
    {
        unsigned char *ele = lpLast(lp);
        while (index < -1 && ele)
        {
            ele = lpPrev(lp, ele);
            index++;
        }
        return ele;
    }
}
