/* String -> String Map data structure optimized for size.
 * This file implements a data structure mapping strings to other strings
 * implementing an O(n) lookup data structure designed to be very memory
 * efficient.
 *
 * The Redis Hash type uses this data structure for hashes composed of a small
 * number of elements, to switch to a hash table once a given number of
 * elements is reached.
 *
 * Given that many times Redis Hashes are used to represent objects composed
 * of few fields, this is a very big win in terms of used memory.
 *
 * --------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

/* Memory layout of a zipmap, for the map "foo" => "bar", "hello" => "world":
 *
 * <zmlen><len>"foo"<len><free>"bar"<len>"hello"<len><free>"world"
 *
 * <zmlen> is 1 byte length that holds the current size of the zipmap.
 * When the zipmap length is greater than or equal to 254, this value
 * is not used and the zipmap needs to be traversed to find out the length.
 *
 * <len> is the length of the following string (key or value).
 * <len> lengths are encoded in a single value or in a 5 bytes value.
 * If the first byte value (as an unsigned 8 bit value) is between 0 and
 * 253, it's a single-byte length. If it is 254 then a four bytes unsigned
 * integer follows (in the host byte ordering). A value of 255 is used to
 * signal the end of the hash.
 *
 * <free> is the number of free unused bytes after the string, resulting
 * from modification of values associated to a key. For instance if "foo"
 * is set to "bar", and later "foo" will be set to "hi", it will have a
 * free byte to use if the value will enlarge again later, or even in
 * order to add a key/value pair if it fits.
 *
 * <free> is always an unsigned 8 bit number, because if after an
 * update operation there are more than a few free bytes, the zipmap will be
 * reallocated to make sure it is as small as possible.
 *
 * The most compact representation of the above two elements hash is actually:
 *
 * "\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff"
 *
 * Note that because keys and values are prefixed length "objects",
 * the lookup will take O(N) where N is the number of elements
 * in the zipmap and *not* the number of bytes needed to represent the zipmap.
 * This lowers the constant times considerably.
 *
 * 字符串到字符串的映射机制。可以达到 O(N) 的时间复杂度，有很高的的内存使用效率。
 *
 * 以字符串 foo -> bar, hello -> world 的映射为例，其内存结构如下：
 *
 * <zmlen>  <len>   <key>   <len>   <free>  <value>   <len>   <key>   <len>   <free>   <value>   <len>
 * 0x02     0x03    foo     0x03    0x00    bar       0x05    hello   0x05    0x00     world     0xFF
 *
 * zmlen: 1 字节. 表示 zipmap 中 key-value 的数量。最大可表示 254，如果超过 254，需要遍历 zipmap 来获取 zipmap 的大小
 *
 * len: 表示后面跟随的 key/value 的字符串的长度。
 *  1. 当字符串的长度小于 254 时，len 为 1 字节；
 *  2. 当超过 254 时，len 为 5 字节，第一个字节为 0xFE，标记使用 5 字节来存储 len 字段，后面四字节表示字符串的长度
 *
 * free: 1 字节. 表示在字符串后未使用的字节的数量。这通常时由于修改 key 对应的 value 的而产生的，例如将 foo->bar 中修改 bar 为 b，那么就会产生两个字节的未使用内存。如果在更新 zipmap 后，出现无法使用一个字节来表示未使用数据长度的情况的话，那么会重新分配 zipmap，以确保内存的使用效率
 *
 * 0xFF: 1 字节. 标记 zipmap 连续分配内存的结尾
 */

#include <stdio.h>
#include <string.h>
#include "zmalloc.h"
#include "endianconv.h"

#define ZIPMAP_BIGLEN 254
#define ZIPMAP_END 255

/* The following defines the max value for the <free> field described in the
 * comments above, that is, the max number of trailing bytes in a value. */
#define ZIPMAP_VALUE_MAX_FREE 4

/* The following macro returns the number of bytes needed to encode the length
 * for the integer value _l, that is, 1 byte for lengths < ZIPMAP_BIGLEN and
 * 5 bytes for all the other lengths. */
#define ZIPMAP_LEN_BYTES(_l) (((_l) < ZIPMAP_BIGLEN) ? 1 : sizeof(unsigned int) + 1)

/* Create a new empty zipmap. */
unsigned char *zipmapNew(void)
{
    unsigned char *zm = zmalloc(2);

    zm[0] = 0; /* Length */
    zm[1] = ZIPMAP_END;
    return zm;
}

/* Decode the encoded length pointed by 'p' */
// p: 指向 zipmap 中的某一个元素的 len 字段的指针。zipmap 中的一个元素可能是 <len> + <key> 或者 <len> + <free> + <value>
// 计算 key 或者 value 的 payload 的长度
static unsigned int zipmapDecodeLength(unsigned char *p)
{
    unsigned int len = *p;

    if (len < ZIPMAP_BIGLEN)       // 注意: 如果 key 为 5 字节，那么第一个字节为 0xFE, 即 ZIPMAP_BIGLEN
        return len;

    // 如果长度超过了 254，需要用 5 个字节来表示长度。第一个字节为 0xFE，后面四个字节表示实际的字符串长度
    memcpy(&len, p + 1, sizeof(unsigned int));
    memrev32ifbe(&len);
    return len;
}

/* Encode the length 'l' writing it in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. */
// 将 len 保存到 p 指向内存中. 如果 p 为空，则判断存储 len 长度的数据，<len> 字段需要多少字节: 1 字节还是 5 字节
static unsigned int zipmapEncodeLength(unsigned char *p, unsigned int len)
{
    if (p == NULL)
    {
        return ZIPMAP_LEN_BYTES(len);
    }
    else
    {
        if (len < ZIPMAP_BIGLEN)
        {
            p[0] = len;
            return 1;
        }
        else
        {
            p[0] = ZIPMAP_BIGLEN;
            memcpy(p + 1, &len, sizeof(len));
            memrev32ifbe(p + 1);
            return 1 + sizeof(len);
        }
    }
}

/* Search for a matching key, returning a pointer to the entry inside the
 * zipmap. Returns NULL if the key is not found.
 *
 * If NULL is returned, and totlen is not NULL, it is set to the entire
 * size of the zipmap, so that the calling function will be able to
 * reallocate the original zipmap to make room for more entries.
 *
 * 在 zm 中查找 key，如果没找到，则返回 NULL，且如果传入了 totlen 指针，那么会把 zm 的总长度赋给 totlen
 *
 * zm:     zipmap 的指针
 * key:    要查找的 key
 * keylen: 要查找的 key 的 len
 *
 * 由于 zipmap 是一段连续的内存，所以是按照顺序查找的方式进行的，时间复杂度为 O(N)
 */
static unsigned char *zipmapLookupRaw(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned int *totlen)
{
    unsigned char *p = zm + 1, *k = NULL;
    unsigned int l, llen;

    while (*p != ZIPMAP_END)
    {
        unsigned char free;

        /* Match or skip the key */
        l = zipmapDecodeLength(p);
        llen = zipmapEncodeLength(NULL, l);
        if (key != NULL && k == NULL && l == klen && !memcmp(p + llen, key, l))
        {
            /* Only return when the user doesn't care
             * for the total length of the zipmap. */
            if (totlen != NULL)
            {
                k = p;
            }
            else
            {
                return p;
            }
        }

        p += llen + l;
        /* Skip the value as well */
        l = zipmapDecodeLength(p);
        p += zipmapEncodeLength(NULL, l);
        free = p[0];
        p += l + 1 + free; /* +1 to skip the free byte */
    }

    if (totlen != NULL)
        *totlen = (unsigned int)(p - zm) + 1;

    return k;
}

// 计算 key-value (长度分别为 klen 和 vlen) 在 zipmap 中所占用的内存大小
static unsigned long zipmapRequiredLength(unsigned int klen, unsigned int vlen)
{
    unsigned int l;

    l = klen + vlen + 3; // 两个 <len> 字段 + 一个 <free> 字段, 共 3 字节
    if (klen >= ZIPMAP_BIGLEN)
        l += 4;
    if (vlen >= ZIPMAP_BIGLEN)
        l += 4;
    return l;
}

/* Return the total amount used by a key (encoded length + payload) */
// 计算 key 的总大小，<len> + payload
static unsigned int zipmapRawKeyLength(unsigned char *p)
{
    unsigned int l = zipmapDecodeLength(p); // key 的 payload 的长度，即 <len> 字段内保存的值
    return zipmapEncodeLength(NULL, l) + l;  // 根据 len 的大小判断 <len> 字段需要多少字节
}

/* Return the total amount used by a value
 * (encoded length + single byte free count + payload) */
// 获取 value 的总大小：len + free + payload
static unsigned int zipmapRawValueLength(unsigned char *p)
{
    unsigned int l = zipmapDecodeLength(p); // value 的 payload 的长度
    unsigned int used;

    used = zipmapEncodeLength(NULL, l); // 计算 len 字段是 1 字节还是 4 字节
    used += p[used] + 1 + l;            // 1 表示 free 字段为 1 字节
    return used;
}

/* If 'p' points to a key, this function returns the total amount of
 * bytes used to store this entry (entry = key + associated value + trailing
 * free space if any).
 *
 * 计算 key-value 的总长度
 */
static unsigned int zipmapRawEntryLength(unsigned char *p)
{
    unsigned int l = zipmapRawKeyLength(p);
    return l + zipmapRawValueLength(p + l);
}

// 对 zipmap 进行内存重新分配。原来 zipmap 的地址可能失效, 返回值为 zipmap 的新地址
static inline unsigned char *zipmapResize(unsigned char *zm, unsigned int len)
{
    zm = zrealloc(zm, len);
    zm[len - 1] = ZIPMAP_END; // ZIPMAP_END: 0xFF 表示 zipmap 的结束
    return zm;
}

/* Set key to value, creating the key if it does not already exist.
 * If 'update' is not NULL, *update is set to 1 if the key was
 * already preset, otherwise to 0.
 *
 * 将 zipmap 中 key 的 value 值设置为 val。
 * 1. 如果 key 不存在，则将 key-value 插入到 zipmap 的尾部
 * 2. 如果 key 存在，则会覆盖 key 原来的 value
 */
unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update)
{
    unsigned int zmlen, offset;
    unsigned int freelen, reqlen = zipmapRequiredLength(klen, vlen);
    unsigned int empty, vempty;
    unsigned char *p;

    freelen = reqlen;
    if (update)
        *update = 0;
    p = zipmapLookupRaw(zm, key, klen, &zmlen);
    if (p == NULL)
    {
        /* Key not found: enlarge */
        zm = zipmapResize(zm, zmlen + reqlen);      // 没找到 key, 需要对 zipmap 进行扩容, 使得可以容纳 key-value 的插入
        p = zm + zmlen - 1;
        zmlen = zmlen + reqlen;

        /* Increase zipmap length (this is an insert) */
        if (zm[0] < ZIPMAP_BIGLEN)
            zm[0]++;
    }
    else
    {
        /* Key found. Is there enough space for the new value? */
        /* Compute the total length: */
        if (update)
            *update = 1;
        freelen = zipmapRawEntryLength(p); // 计算原来 key-value 的长度
        if (freelen < reqlen)
        {
            /* Store the offset of this key within the current zipmap, so
             * it can be resized. Then, move the tail backwards so this
             * pair fits at the current position. */
            // 原来 key-value 使用的内存无法保存新的 key-value，需要再分配 (reqlen-freelen) 大小的内存
            offset = p - zm;        // key 在 zipmap 中的偏移，需要保存 key 的偏移，因为调用 zipmapResize 之后 zm 的地址可能发生变化
            zm = zipmapResize(zm, zmlen - freelen + reqlen);    // zm 为内存重新分配之后 zipmap 的地址
            p = zm + offset;    // 重新通过偏移得到 key 的地址

            /* The +1 in the number of bytes to be moved is caused by the
             * end-of-zipmap byte. Note: the *original* zmlen is used. */
            // 将原来 key-value 后面所有的内存向后移动 (reqlen-freelen) 个字节，为新的 key-value 预留足够的空间
            memmove(p + reqlen, p + freelen, zmlen - (offset + freelen + 1));
            zmlen = zmlen - freelen + reqlen;
            freelen = reqlen;
        }
    }

    // key 不存在或者原来的 key-value 使用的内存可以保存新的 key-value

    /* We now have a suitable block where the key/value entry can
     * be written. If there is too much free space, move the tail
     * of the zipmap a few bytes to the front and shrink the zipmap,
     * as we want zipmaps to be very space efficient. */
    empty = freelen - reqlen;
    if (empty >= ZIPMAP_VALUE_MAX_FREE)
    {
        /* First, move the tail <empty> bytes to the front, then resize
         * the zipmap to be <empty> bytes smaller. */
        offset = p - zm;
        memmove(p + reqlen, p + freelen, zmlen - (offset + freelen + 1));
        zmlen -= empty;
        zm = zipmapResize(zm, zmlen);
        p = zm + offset;
        vempty = 0;
    }
    else
    {
        vempty = empty;
    }

    /* Just write the key + value and we are done. */
    /* Key: */
    p += zipmapEncodeLength(p, klen);
    memcpy(p, key, klen);
    p += klen;
    /* Value: */
    p += zipmapEncodeLength(p, vlen);
    *p++ = vempty;
    memcpy(p, val, vlen);
    return zm;
}

/* Remove the specified key. If 'deleted' is not NULL the pointed integer is
 * set to 0 if the key was not found, to 1 if it was found and deleted.
 * 从 zipmap 中删除 key, 并将删除的 key 通过 deleted 返回
 */
unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted)
{
    unsigned int zmlen, freelen;
    unsigned char *p = zipmapLookupRaw(zm, key, klen, &zmlen);
    if (p)
    {
        freelen = zipmapRawEntryLength(p);
        memmove(p, p + freelen, zmlen - ((p - zm) + freelen + 1));
        zm = zipmapResize(zm, zmlen - freelen);

        /* Decrease zipmap length */
        if (zm[0] < ZIPMAP_BIGLEN) // 只有当 zmlen 小于 254 时，zmlen 表示 zipmap 的节点个数; 否则需要遍历整个 zipmap, 所以没必要更新 zmlen 字段
            zm[0]--;

        if (deleted)
            *deleted = 1;
    }
    else
    {
        if (deleted)
            *deleted = 0;
    }
    return zm;
}

/* Call before iterating through elements via zipmapNext() */
// 通常与 zipmapNext() 接口搭配在一起使用
unsigned char *zipmapRewind(unsigned char *zm)
{
    return zm + 1; // 跳过 <zmlen> 字段，返回指向第一个 key-value 内存的指针
}

/* This function is used to iterate through all the zipmap elements.
 * In the first call the first argument is the pointer to the zipmap + 1.
 * In the next calls what zipmapNext returns is used as first argument.
 * Example:
 *
 * unsigned char *i = zipmapRewind(my_zipmap);
 * while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
 *     printf("%d bytes key at $p\n", klen, key);
 *     printf("%d bytes value at $p\n", vlen, value);
 * }
 *
 * 根据传入的 key-value 指针，获取其 key 以及 value，并返回下一个 key-value 的指针
 *
 * 和 zipmapRewind() 搭配，可以实现 zipmap 的遍历
 */
unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen)
{
    if (zm[0] == ZIPMAP_END)
        return NULL;
    if (key)
    {
        *key = zm;
        *klen = zipmapDecodeLength(zm);
        *key += ZIPMAP_LEN_BYTES(*klen);
    }
    zm += zipmapRawKeyLength(zm);
    if (value)
    {
        *value = zm + 1;
        *vlen = zipmapDecodeLength(zm);
        *value += ZIPMAP_LEN_BYTES(*vlen);
    }
    zm += zipmapRawValueLength(zm);
    return zm;
}

/* Search a key and retrieve the pointer and len of the associated value.
 * If the key is found the function returns 1, otherwise 0.
 * 从 zipmap 中获取指定 key 对应的 value. 如果 key 存在，返回 1, 并将 value 的大小通过 vlen 返回
 */
int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen)
{
    unsigned char *p;

    if ((p = zipmapLookupRaw(zm, key, klen, NULL)) == NULL) // p 指向查找到的 key 的位置
        return 0;
    p += zipmapRawKeyLength(p);     // 计算 key 的内存大小: <len>+key，然后将 p 移动到下一个元素 value: value 有三个字段 <len><free><value>
    *vlen = zipmapDecodeLength(p);  // 计算 value 的内存大小: <len> 字段中存储的数值
    /**
     * ZIPMAP_LEN_BYTES(*vlen): 获取 <len> 字段的长度: 1 字节还是 5 字节
     * +1: <free> 字段的大小为 1 字节
     */
    *value = p + ZIPMAP_LEN_BYTES(*vlen) + 1; // 将 p 移动到 value 元素的 value 字段
    return 1;
}

/* Return 1 if the key exists, otherwise 0 is returned. */
// 判断 key 是否存在
int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen)
{
    return zipmapLookupRaw(zm, key, klen, NULL) != NULL;
}

/* Return the number of entries inside a zipmap */
// 返回 zipmap 中 key-value 的数量
unsigned int zipmapLen(unsigned char *zm)
{
    unsigned int len = 0;
    if (zm[0] < ZIPMAP_BIGLEN)
    {
        len = zm[0];
    }
    else
    {
        unsigned char *p = zipmapRewind(zm);
        while ((p = zipmapNext(p, NULL, NULL, NULL, NULL)) != NULL)
            len++;

        /* Re-store length if small enough */
        if (len < ZIPMAP_BIGLEN)
            zm[0] = len;
    }
    return len;
}

/* Return the raw size in bytes of a zipmap, so that we can serialize
 * the zipmap on disk (or everywhere is needed) just writing the returned
 * amount of bytes of the C array starting at the zipmap pointer.
 *
 * 返回 zipmap 占用的字节总大小
 */
size_t zipmapBlobLen(unsigned char *zm)
{
    unsigned int totlen;
    zipmapLookupRaw(zm, NULL, 0, &totlen);
    return totlen;
}

#ifdef REDIS_TEST
static void zipmapRepr(unsigned char *p)
{
    unsigned int l;

    printf("{status %u}", *p++);
    while (1)
    {
        if (p[0] == ZIPMAP_END)
        {
            printf("{end}");
            break;
        }
        else
        {
            unsigned char e;

            l = zipmapDecodeLength(p);
            printf("{key %u}", l);
            p += zipmapEncodeLength(NULL, l);
            if (l != 0 && fwrite(p, l, 1, stdout) == 0)
                perror("fwrite");
            p += l;

            l = zipmapDecodeLength(p);
            printf("{value %u}", l);
            p += zipmapEncodeLength(NULL, l);
            e = *p++;
            if (l != 0 && fwrite(p, l, 1, stdout) == 0)
                perror("fwrite");
            p += l + e;
            if (e)
            {
                printf("[");
                while (e--)
                    printf(".");
                printf("]");
            }
        }
    }
    printf("\n");
}

#define UNUSED(x) (void)(x)
int zipmapTest(int argc, char *argv[])
{
    unsigned char *zm;

    UNUSED(argc);
    UNUSED(argv);

    zm = zipmapNew();

    zm = zipmapSet(zm, (unsigned char *)"name", 4, (unsigned char *)"foo", 3, NULL);
    zm = zipmapSet(zm, (unsigned char *)"surname", 7, (unsigned char *)"foo", 3, NULL);
    zm = zipmapSet(zm, (unsigned char *)"age", 3, (unsigned char *)"foo", 3, NULL);
    zipmapRepr(zm);

    zm = zipmapSet(zm, (unsigned char *)"hello", 5, (unsigned char *)"world!", 6, NULL);
    zm = zipmapSet(zm, (unsigned char *)"foo", 3, (unsigned char *)"bar", 3, NULL);
    zm = zipmapSet(zm, (unsigned char *)"foo", 3, (unsigned char *)"!", 1, NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm, (unsigned char *)"foo", 3, (unsigned char *)"12345", 5, NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm, (unsigned char *)"new", 3, (unsigned char *)"xx", 2, NULL);
    zm = zipmapSet(zm, (unsigned char *)"noval", 5, (unsigned char *)"", 0, NULL);
    zipmapRepr(zm);
    zm = zipmapDel(zm, (unsigned char *)"new", 3, NULL);
    zipmapRepr(zm);

    printf("\nLook up large key:\n");
    {
        unsigned char buf[512];
        unsigned char *value;
        unsigned int vlen, i;
        for (i = 0; i < 512; i++)
            buf[i] = 'a';

        zm = zipmapSet(zm, buf, 512, (unsigned char *)"long", 4, NULL);
        if (zipmapGet(zm, buf, 512, &value, &vlen))
        {
            printf("  <long key> is associated to the %d bytes value: %.*s\n",
                   vlen, vlen, value);
        }
    }

    printf("\nPerform a direct lookup:\n");
    {
        unsigned char *value;
        unsigned int vlen;

        if (zipmapGet(zm, (unsigned char *)"foo", 3, &value, &vlen))
        {
            printf("  foo is associated to the %d bytes value: %.*s\n",
                   vlen, vlen, value);
        }
    }
    printf("\nIterate through elements:\n");
    {
        unsigned char *i = zipmapRewind(zm);
        unsigned char *key, *value;
        unsigned int klen, vlen;

        while ((i = zipmapNext(i, &key, &klen, &value, &vlen)) != NULL)
        {
            printf("  %d:%.*s => %d:%.*s\n", klen, klen, key, vlen, vlen, value);
        }
    }
    return 0;
}
#endif
