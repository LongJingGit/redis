/* SDSLib 2.0 -- A C dynamic strings library
 *
 * Copyright (c) 2006-2015, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2015, Oran Agra
 * Copyright (c) 2015, Redis Labs, Inc
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

#ifndef __SDS_H
#define __SDS_H

#define SDS_MAX_PREALLOC (1024 * 1024)
extern const char *SDS_NOINIT;

#include <sys/types.h>
#include <stdarg.h>
#include <stdint.h>

typedef char *sds;

/* Note: sdshdr5 is never used, we just access the flags byte directly.
 * However is here to document the layout of type 5 SDS strings.
 */
struct __attribute__((__packed__)) sdshdr5
{
    unsigned char flags; /* 3 lsb of type, and 5 msb of string length */ // 低三位保存 header 类型信息，高五位表示已经使用的缓存长度
    char buf[];
};

/**
 * sds 变量实际上就是 sdshdr.buf，而因为整个 sdshdr 是一段连续分配的内存区域，所以可以通过 sds 变量向前偏移一个字节 sds[-1] 就是 flags 字段，
 * 通过位运算，就可以知道 sdshdr 的类型，然后通过指针偏移，就可以得到 sdshdr 的指针，然后获取整个头部信息
 *
 * sdshdr8 最大可分配的缓存为 1<<8. 根据实际要保存字符串的大小，选择合适的数据类型，可以达到节约内存的目的
 *
 * __attribute__((__packed__)): 采用 1 字节内存对齐
 */
struct __attribute__((__packed__)) sdshdr8
{
    uint8_t len; /* used */                                       // buf 中已经使用的缓存长度, 也就是 sds 变量的内存大小
    uint8_t alloc; /* excluding the header and null terminator */ // 表示除了 header 和 buf 中 nullptr 结束符之外分配的缓存长度
    unsigned char flags; /* 3 lsb of type, 5 unused bits */       // 低三位保存 header 类型信息

    char buf[]; // 动态分配的缓存，也是 sds 变量指向的地址。保存真实的数据，且最后以 '\0' 结尾（分配缓存时，一般会多分配一个字节的长度放置 \0 结束符）
};

struct __attribute__((__packed__)) sdshdr16
{
    uint16_t len;        /* used */
    uint16_t alloc;      /* excluding the header and null terminator */
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    char buf[];
};
struct __attribute__((__packed__)) sdshdr32
{
    uint32_t len;        /* used */
    uint32_t alloc;      /* excluding the header and null terminator */
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    char buf[];
};
struct __attribute__((__packed__)) sdshdr64
{
    uint64_t len;        /* used */
    uint64_t alloc;      /* excluding the header and null terminator */
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    char buf[];
};

#define SDS_TYPE_5 0
#define SDS_TYPE_8 1
#define SDS_TYPE_16 2
#define SDS_TYPE_32 3
#define SDS_TYPE_64 4
#define SDS_TYPE_MASK 7
#define SDS_TYPE_BITS 3
#define SDS_HDR_VAR(T, s) struct sdshdr##T *sh = (void *)((s) - (sizeof(struct sdshdr##T))); // 计算并返回 sds 变量对应的 sdshdr 指针
#define SDS_HDR(T, s) ((struct sdshdr##T *)((s) - (sizeof(struct sdshdr##T)))) // 获取 sds 变量对应的 sdshdr 的头指针
#define SDS_TYPE_5_LEN(f) ((f) >> SDS_TYPE_BITS)

static inline size_t sdslen(const sds s)
{
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK)
    {
    case SDS_TYPE_5:
        return SDS_TYPE_5_LEN(flags);
    case SDS_TYPE_8:
        return SDS_HDR(8, s)->len;
    case SDS_TYPE_16:
        return SDS_HDR(16, s)->len;
    case SDS_TYPE_32:
        return SDS_HDR(32, s)->len;
    case SDS_TYPE_64:
        return SDS_HDR(64, s)->len;
    }
    return 0;
}

static inline size_t sdsavail(const sds s)
{
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK)
    {
    case SDS_TYPE_5:
    {
        return 0;
    }
    case SDS_TYPE_8:
    {
        SDS_HDR_VAR(8, s);
        return sh->alloc - sh->len;
    }
    case SDS_TYPE_16:
    {
        SDS_HDR_VAR(16, s);
        return sh->alloc - sh->len;
    }
    case SDS_TYPE_32:
    {
        SDS_HDR_VAR(32, s);
        return sh->alloc - sh->len;
    }
    case SDS_TYPE_64:
    {
        SDS_HDR_VAR(64, s);
        return sh->alloc - sh->len;
    }
    }
    return 0;
}

static inline void sdssetlen(sds s, size_t newlen)
{
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK)
    {
    case SDS_TYPE_5:
    {
        unsigned char *fp = ((unsigned char *)s) - 1;
        *fp = SDS_TYPE_5 | (newlen << SDS_TYPE_BITS);
    }
    break;
    case SDS_TYPE_8:
        SDS_HDR(8, s)->len = newlen;
        break;
    case SDS_TYPE_16:
        SDS_HDR(16, s)->len = newlen;
        break;
    case SDS_TYPE_32:
        SDS_HDR(32, s)->len = newlen;
        break;
    case SDS_TYPE_64:
        SDS_HDR(64, s)->len = newlen;
        break;
    }
}

static inline void sdsinclen(sds s, size_t inc)
{
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK)
    {
    case SDS_TYPE_5:
    {
        unsigned char *fp = ((unsigned char *)s) - 1;
        unsigned char newlen = SDS_TYPE_5_LEN(flags) + inc;
        *fp = SDS_TYPE_5 | (newlen << SDS_TYPE_BITS);
    }
    break;
    case SDS_TYPE_8:
        SDS_HDR(8, s)->len += inc;
        break;
    case SDS_TYPE_16:
        SDS_HDR(16, s)->len += inc;
        break;
    case SDS_TYPE_32:
        SDS_HDR(32, s)->len += inc;
        break;
    case SDS_TYPE_64:
        SDS_HDR(64, s)->len += inc;
        break;
    }
}

/* sdsalloc() = sdsavail() + sdslen() */
static inline size_t sdsalloc(const sds s)
{
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK)
    {
    case SDS_TYPE_5:
        return SDS_TYPE_5_LEN(flags);
    case SDS_TYPE_8:
        return SDS_HDR(8, s)->alloc;
    case SDS_TYPE_16:
        return SDS_HDR(16, s)->alloc;
    case SDS_TYPE_32:
        return SDS_HDR(32, s)->alloc;
    case SDS_TYPE_64:
        return SDS_HDR(64, s)->alloc;
    }
    return 0;
}

static inline void sdssetalloc(sds s, size_t newlen)
{
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK)
    {
    case SDS_TYPE_5:
        /* Nothing to do, this type has no total allocation info. */
        break;
    case SDS_TYPE_8:
        SDS_HDR(8, s)->alloc = newlen;
        break;
    case SDS_TYPE_16:
        SDS_HDR(16, s)->alloc = newlen;
        break;
    case SDS_TYPE_32:
        SDS_HDR(32, s)->alloc = newlen;
        break;
    case SDS_TYPE_64:
        SDS_HDR(64, s)->alloc = newlen;
        break;
    }
}

sds sdsnewlen(const void *init, size_t initlen);
sds sdsnew(const char *init);
sds sdsempty(void);
sds sdsdup(const sds s);
void sdsfree(sds s);
sds sdsgrowzero(sds s, size_t len);
sds sdscatlen(sds s, const void *t, size_t len);
sds sdscat(sds s, const char *t);
sds sdscatsds(sds s, const sds t);
sds sdscpylen(sds s, const char *t, size_t len);
sds sdscpy(sds s, const char *t);

sds sdscatvprintf(sds s, const char *fmt, va_list ap);
#ifdef __GNUC__
sds sdscatprintf(sds s, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
sds sdscatprintf(sds s, const char *fmt, ...);
#endif

sds sdscatfmt(sds s, char const *fmt, ...);
sds sdstrim(sds s, const char *cset);
void sdsrange(sds s, ssize_t start, ssize_t end);
void sdsupdatelen(sds s);
void sdsclear(sds s);
int sdscmp(const sds s1, const sds s2);
sds *sdssplitlen(const char *s, ssize_t len, const char *sep, int seplen, int *count);
void sdsfreesplitres(sds *tokens, int count);
void sdstolower(sds s);
void sdstoupper(sds s);
sds sdsfromlonglong(long long value);
sds sdscatrepr(sds s, const char *p, size_t len);
sds *sdssplitargs(const char *line, int *argc);
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen);
sds sdsjoin(char **argv, int argc, char *sep);
sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen);

/* Low level functions exposed to the user API */
sds sdsMakeRoomFor(sds s, size_t addlen);
void sdsIncrLen(sds s, ssize_t incr);
sds sdsRemoveFreeSpace(sds s);
size_t sdsAllocSize(sds s);
void *sdsAllocPtr(sds s);

/* Export the allocator used by SDS to the program using SDS.
 * Sometimes the program SDS is linked to, may use a different set of
 * allocators, but may want to allocate or free things that SDS will
 * respectively free or allocate. */
void *sds_malloc(size_t size);
void *sds_realloc(void *ptr, size_t size);
void sds_free(void *ptr);

#ifdef REDIS_TEST
int sdsTest(int argc, char *argv[]);
#endif

#endif
