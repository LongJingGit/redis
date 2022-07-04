/* The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient. It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters. It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT
 * ======================
 *
 * The general layout of the ziplist is as follows:
 *
 * <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * NOTE: all fields are stored in little endian, if not specified otherwise.
 *
 * <uint32_t zlbytes> is an unsigned integer to hold the number of bytes that
 * the ziplist occupies, including the four bytes of the zlbytes field itself.
 * This value needs to be stored to be able to resize the entire structure
 * without the need to traverse it first.
 *
 * <uint32_t zltail> is the offset to the last entry in the list. This allows
 * a pop operation on the far side of the list without the need for full
 * traversal.
 *
 * <uint16_t zllen> is the number of entries. When there are more than
 * 2^16-2 entries, this value is set to 2^16-1 and we need to traverse the
 * entire list to know how many items it holds.
 *
 * <uint8_t zlend> is a special entry representing the end of the ziplist.
 * Is encoded as a single byte equal to 255. No other normal entry starts
 * with a byte set to the value of 255.
 *
 * ZIPLIST ENTRIES
 * ===============
 *
 * Every entry in the ziplist is prefixed by metadata that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the entry encoding is
 * provided. It represents the entry type, integer or string, and in the case
 * of strings it also represents the length of the string payload.
 * So a complete entry is stored like this:
 *
 * <prevlen> <encoding> <entry-data>
 *
 * Sometimes the encoding represents the entry itself, like for small integers
 * as we'll see later. In such a case the <entry-data> part is missing, and we
 * could have just:
 *
 * <prevlen> <encoding>
 *
 * The length of the previous entry, <prevlen>, is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte representing the length as an unsinged 8 bit integer. When the length
 * is greater than or equal to 254, it will consume 5 bytes. The first byte is
 * set to 254 (FE) to indicate a larger value is following. The remaining 4
 * bytes take the length of the previous entry as value.
 *
 * So practically an entry is encoded in the following way:
 *
 * <prevlen from 0 to 253> <encoding> <entry>
 *
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 *
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 *
 * The encoding field of the entry depends on the content of the
 * entry. When the entry is a string, the first 2 bits of the encoding first
 * byte will hold the type of encoding used to store the length of the string,
 * followed by the actual length of the string. When the entry is an integer
 * the first 2 bits are both set to 1. The following 2 bits are used to specify
 * what kind of integer will be stored after this header. An overview of the
 * different types and encodings is as follows. The first byte is always enough
 * to determine the kind of entry.
 *
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      "pppppp" represents the unsigned 6 bit length.
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      IMPORTANT: The 14 bit number is stored in big endian.
 * |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      Only the 4 bytes following the first byte represents the length
 *      up to 2^32-1. The 6 lower bits of the first byte are not used and
 *      are set to zero.
 *      IMPORTANT: The 32 bit number is stored in big endian.
 * |11000000| - 3 bytes
 *      Integer encoded as int16_t (2 bytes).
 * |11010000| - 5 bytes
 *      Integer encoded as int32_t (4 bytes).
 * |11100000| - 9 bytes
 *      Integer encoded as int64_t (8 bytes).
 * |11110000| - 4 bytes
 *      Integer encoded as 24 bit signed (3 bytes).
 * |11111110| - 2 bytes
 *      Integer encoded as 8 bit signed (1 byte).
 * |1111xxxx| - (with xxxx between 0001 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 * |11111111| - End of ziplist special entry.
 *
 * Like for the ziplist header, all the integers are represented in little
 * endian byte order, even when this code is compiled in big endian systems.
 *
 * EXAMPLES OF ACTUAL ZIPLISTS
 * ===========================
 *
 * The following is a ziplist containing the two elements representing
 * the strings "2" and "5". It is composed of 15 bytes, that we visually
 * split into sections:
 *
 *  [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
 *        |             |          |       |       |     |
 *     zlbytes        zltail    entries   "2"     "5"   end
 *
 * The first 4 bytes represent the number 15, that is the number of bytes
 * the whole ziplist is composed of. The second 4 bytes are the offset
 * at which the last ziplist entry is found, that is 12, in fact the
 * last entry, that is "5", is at offset 12 inside the ziplist.
 * The next 16 bit integer represents the number of elements inside the
 * ziplist, its value is 2 since there are just two elements inside.
 * Finally "00 f3" is the first entry representing the number 2. It is
 * composed of the previous entry length, which is zero because this is
 * our first entry, and the byte F3 which corresponds to the encoding
 * |1111xxxx| with xxxx between 0001 and 1101. We need to remove the "F"
 * higher order bits 1111, and subtract 1 from the "3", so the entry value
 * is "2". The next entry has a prevlen of 02, since the first entry is
 * composed of exactly two bytes. The entry itself, F6, is encoded exactly
 * like the first entry, and 6-1 = 5, so the value of the entry is 5.
 * Finally the special entry FF signals the end of the ziplist.
 *
 * Adding another element to the above string with the value "Hello World"
 * allows us to show how the ziplist encodes small strings. We'll just show
 * the hex dump of the entry itself. Imagine the bytes as following the
 * entry that stores "5" in the ziplist above:
 *
 * [02] [0b] [48 65 6c 6c 6f 20 57 6f 72 6c 64]
 *
 * The first byte, 02, is the length of the previous entry. The next
 * byte represents the encoding in the pattern |00pppppp| that means
 * that the entry is a string of length <pppppp>, so 0B means that
 * an 11 bytes string follows. From the third byte (48) to the last (64)
 * there are just the ASCII characters for "Hello World".
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2017, Salvatore Sanfilippo <antirez at gmail dot com>
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
 * 双向链表 adlist 存在的问题：存储小数据的时候，内存使用效率过低。比如只保存 unsigned char 数据时，需要为这个节点保存 24 字节的额外数据，
 * 其中包含 listNode.prev, listNode.next 和 listNode.value 指针。同时由于对于链表节点占用内存的反复申请和释放，容易导致内存碎片的产生。
 *
 * 为了解决这一问题，redis 设计了 ziplist 来对这种场景下链表的应用进行了优化。具体可参考代码实现
 *
 * 压缩链表是一种专门为了提升内存使用效率而设计的，经过特殊编码的双端链表数据结构。既可以用来保存整形数值，也可以用来保存字符串数值。为了节约内存，同时也是体现压缩之含义，当保存一个整型数值时，压缩链表会使用一个真正的整型数来保存，而不是使用字符串的形式来存储。这一点很容易理解，一个整数可以根据其数值的大小使用1个字节，2个字节，4个字节或者8个字节来表示，如果使用字符串的形式来存储的话，其所需的字节数大小一定不小于使用整型数所需的字节数。
 *
 * 压缩链表允许在链表两端以 O(1) 的时间复杂度执行 Pop 或者 Push 操作，当然这只是一种理想状态下的情况，由于压缩链表实际上是内存中一段连续分配的内存，因此这些操作需要对压缩链表所使用的内存进行重新分配，所以其真实的时间复杂度是和链表所使用的内存大小相关的。
 *
 * ---------------------------------------------------------------------------------------------------------
 *
 * ziplist 的内存结构如下: （注意：如果没有特殊指明的情况下，都是小端序）
 *
 *      <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * uint32_t zlbytes: ziplist 所占用内存的大小（以字节为单位），包含 zlbytes 本身
 * uint32_t zltail:  ziplist 的最后一个节点距离 ziplist 头部的偏移字节数。借助这个字段，可以在不遍历整个链表的过程中，在链表的尾部执行 pop 操作。
 * uint16_t zllen:   ziplist 中总共包含的节点数。最大能表示为 2^16-1，如果超过的话，需要遍历整个链表才能获得真实的节点数量
 * uint8_t zlend:    0xFF，用来标记压缩列表的结束
 * entry:            表示 ziplist 中的每一个节点。同一个链表中的节点，其长度大概率是不同的，因此需要特殊的方式来获取节点的长度
 *
 * 本质上，压缩链表并没有一个专门的数据结构用于描述它，所有的相关操作都是通过指针与解码出来的偏移进行的。
 *
 * ---------------------------------------------------------------------------------------------------------
 *
 * ziplist entry 的结构大致如下所示：
 *
 * <prevlen> <encoding> <entry-data>
 *
 * 对于一些小整数，我们可以直接保存在 encoding 中，而不用 <entry-data> 字段，结构如下：
 *
 * <prevlen> <encoding>
 *
 * NOTE: 这几个字段的长度是不固定的
 *
 * <prevlen>：标记了该节点的前序节点的长度，以便我们可以向链表的头部反向遍历链表。这个字段的编码格式有两种：
 *
 *   1. 当前节点的前序节点的长度为 0 到 253 个字节时，<prevlen> 字段只需要 1 个字节(8 位的无符号整数)，便可以编码对应的长度。
 *
 *       <prevlen from 0 to 253> <encoding> <entry-data>
 *
 *   2. 当前节点的前序节点的长度大于等于 254 个字节时，<prevlen> 字段则需要 5 个字节，其中第一个字节会被设置成 0xFE 也就是 254，这是一个特殊标记，用于表明这里保存了一个比较大的数值，需要继续读取后续的 4 个字节以解码出前序节点的长度。而为什么不选择 0xFF 作为特殊标记的原因在于，0xFF 是整个链表结束的标记。
 *
 *      0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry-data>
 *
 * <encoding>: 表示该节点的编码方式，具体是按照整型进行编码还是按照字符串进行编码。当节点使用的是字符串编码时，该字段还会指明字符串数据的字节长度。如果当前节点的数据是一个小整数（0 到 12），则本字段还可以存储这个数据信息。
 *
 * 对于 <encoding> 字段，根据第一个字节的前两个比特，可以确定是当前的节点是使用整型编码还是字符串编码.
 *   1. 当 encoding 字段的前两个比特位为 11 的时，也就是出现 [11xxxxxx]，表示当前节点是用整型编码的，此时 <encoding> 字段只需要 1 个字节。
 *      [1100 0000] 表明节点的数据是按照 16 位整数进行编码的，需要额外的 2 个字节作为 <entry-data> 来存储数据
 *      [1101 0000] 表明节点的数据是按照 32 位整数进行编码的，需要额外的 4 个字节作为 <entry-data> 来存储数据
 *      [1110 0000] 表明节点的数据是按照 64 位整数进行编码的，需要额外的 8 个字节作为 <entry-data> 来存储数据
 *      [1111 0000] 表明节点的数据是按照 24 位整数进行编码的，需要额外的 3 个字节作为 <entry-data> 来存储数据
 *      [1111 1110] 表明节点的数据是按照 8 位整数进行编码的，需要额外的 1 个字节作为 <entry-data> 来存储数据
 *      [1111 XXXX] 这种方式表明是使用 <encoding> 字段直接来存储数据的小整数，也就是使用 <encoding> 字节的后 4 个比特来表示。这样的小整数的取值范围在 [0~12] 之间，但是实际存储在 <encoding> 中的数值范围是 [1~13]。统一进行了 +1。（这是由于如果用 0 进行编码的话，其二进制的表示为 [11110000]，这样会与 24 位整数编码的 <encoding> 字段冲突；而采用加 1 的方式来避免字段冲突的话，也就意味着最大可以使用的数值为 [11111101]，也就是 13（实际数值为12）， 因为 [11111110] 已经被用作8位整数编码的标记）
 *
 *   2. 当 encoding 字段的前两个比特位不都是 11 的情况时，表示该节点的数据是以字符串形式进行编码的
 *      [00PP PPPP]  <encoding> 字段占用 1 个字节。可以用来表示长度不超过 63 个字节的字符串，也就是使用 <encoding> 剩余的 6 个比特来表示字符串长度。
 *      [01PP PPPP | QQQQ QQQQ] <encoding> 字段会占用 2 个字节。使用首字节剩余的 6 比特以及额外的 1 个字节，可以表示长度不超过 16383 个字节的字符串。
 *      [10000000 | PPPPPPPP | RRRRRRRR | SSSSSSSS | TTTTTTTT]  当字符串长度超过 16383 时，使用这种编码方式，此时 <encoding> 字段占用 5 个字节，使用额外的 4 个字节，最多可以表示长度不超过 2^32-1 的字符串
 *
 *
 * 抽象一些来说，<prevlen><encoding> 这两个字段在一定意义上起到了经典双端链表中 prev 以及 next 指针的作用。通过解码一个给定节点的这两个字段中存储的数据，我们可以指针移动的方式，实现对链表正向以及反向的遍历。
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "endianconv.h"
#include "redisassert.h"

#define ZIP_END 255         /* Special "end of ziplist" entry. */
#define ZIP_BIG_PREVLEN 254 /* ZIP_BIG_PREVLEN - 1 is the max number of bytes of      \
                               the previous entry, for the "prevlen" field prefixing  \
                               each entry, to be represented with just a single byte. \
                               Otherwise it is represented as FE AA BB CC DD, where   \
                               AA BB CC DD are a 4 bytes unsigned integer             \
                               representing the previous entry len. */

/* Different encoding/length possibilities */
#define ZIP_STR_MASK 0xc0           // [1100 0000]
#define ZIP_INT_MASK 0x30           // [0011 0000]
#define ZIP_STR_06B (0 << 6)        // [0000 0000]
#define ZIP_STR_14B (1 << 6)        // [0100 0000]
#define ZIP_STR_32B (2 << 6)        // [1000 0000]
#define ZIP_INT_16B (0xc0 | 0 << 4) // [1100 0000]      16 位整数编码
#define ZIP_INT_32B (0xc0 | 1 << 4) // [1101 0000]      32 位整数编码
#define ZIP_INT_64B (0xc0 | 2 << 4) // [1110 0000]      64 位整数编码
#define ZIP_INT_24B (0xc0 | 3 << 4) // [1111 0000]      24 位整数编码
#define ZIP_INT_8B 0xfe             // [1111 1110]      8 位整数编码

/* 4 bit integer immediate encoding |1111xxxx| with xxxx between
 * 0001 and 1101. */
#define ZIP_INT_IMM_MASK 0x0f /* Mask to extract the 4 bits value. To add \
                                 one is needed to reconstruct the value. */

// 小整数的存储范围 [1 ~ 13]
#define ZIP_INT_IMM_MIN 0xf1 /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd /* 11111101 */

#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/* Macro to determine if the entry is a string. String entries never start
 * with "11" as most significant bits of the first byte. */
#define ZIP_IS_STR(enc) (((enc)&ZIP_STR_MASK) < ZIP_STR_MASK) // 判断是否按照字符串编码的

/* Utility macros.*/

/* Return total bytes a ziplist is composed of. */
// 获取 <zlbytes> 字段: 返回压缩链表的字节数
#define ZIPLIST_BYTES(zl) (*((uint32_t *)(zl)))

/* Return the offset of the last item inside the ziplist. */
// 获取 <zltail> 字段: 最后一个节点在 ziplist 中的偏移
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t *)((zl) + sizeof(uint32_t))))

/* Return the length of a ziplist, or UINT16_MAX if the length cannot be
 * determined without scanning the whole ziplist. */
// 获取 <zllen> 字段: ziplist 的节点数量
#define ZIPLIST_LENGTH(zl) (*((uint16_t *)((zl) + sizeof(uint32_t) * 2)))

/* The size of a ziplist header: two 32 bit integers for the total
 * bytes count and last item offset. One 16 bit integer for the number
 * of items field. */
// 获取 ziplist 头长度，包含 <zlbytes><zltail><zllen> 三个字段
#define ZIPLIST_HEADER_SIZE (sizeof(uint32_t) * 2 + sizeof(uint16_t))

/* Size of the "end of ziplist" entry. Just one byte. */
// 获取 <zlend> 字段: ziplist 尾部长度
#define ZIPLIST_END_SIZE (sizeof(uint8_t))

/* Return the pointer to the first entry of a ziplist. */
// 获取 ziplist 第一个节点的指针
#define ZIPLIST_ENTRY_HEAD(zl) ((zl) + ZIPLIST_HEADER_SIZE)

/* Return the pointer to the last entry of a ziplist, using the
 * last entry offset inside the ziplist header. */
// 获取 ziplist 最后一个节点的指针
#define ZIPLIST_ENTRY_TAIL(zl) ((zl) + intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))

/* Return the pointer to the last byte of a ziplist, which is, the
 * end of ziplist FF entry. */
// 获取指向结束标记 <zlend> 的指针
#define ZIPLIST_ENTRY_END(zl) ((zl) + intrev32ifbe(ZIPLIST_BYTES(zl)) - 1)

/* Increment the number of items field in the ziplist header. Note that this
 * macro should never overflow the unsigned 16 bit integer, since entries are
 * always pushed one at a time. When UINT16_MAX is reached we want the count
 * to stay there to signal that a full scan is needed to get the number of
 * items inside the ziplist. */
// 压缩链表 zl 的 <zllen> 字段增加 incr
#define ZIPLIST_INCR_LENGTH(zl, incr)                                                   \
    {                                                                                   \
        if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX)                              \
            ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl)) + incr); \
    }

/* Don't let ziplists grow over 1GB in any case, don't wanna risk overflow in
 * zlbytes*/
#define ZIPLIST_MAX_SAFETY_SIZE (1 << 30) // ziplist 的最大字节数: 1GB

int ziplistSafeToAdd(unsigned char *zl, size_t add)
{
    size_t len = zl ? ziplistBlobLen(zl) : 0;
    if (len + add > ZIPLIST_MAX_SAFETY_SIZE)
        return 0;
    return 1;
}

/* We use this function to receive information about a ziplist entry.
 * Note that this is not how the data is actually encoded, is just what we
 * get filled by a function in order to operate more easily.
 *
 * 需要注意的是，zlentry 并非是 ziplist 在内存中的实际存储方式，它的作用仅仅是可以方便直观的描述压缩链表的节点信息
 *
 * 本质上，压缩链表并没有一个专门的数据结构用于描述它，所有的相关操作都是通过指针与解码出来的偏移进行的。
 */
typedef struct zlentry
{
    // 记录 <prevlen> 字段本身的字节长度
    unsigned int prevrawlensize; /* Bytes used to encode the previous entry len*/

    // 记录保存在 <prevlen> 字段中前序节点的长度
    unsigned int prevrawlen; /* Previous entry len. */

    // 记录 <encoding> 字段本身所占用的字节长度
    unsigned int lensize; /* Bytes used to encode this entry type/len.
                             For example strings have a 1, 2 or 5 bytes
                             header. Integers always use a single byte.*/

    // 记录当前 entry 中 entry-data 数据所占用的长度，可以为 0，表示存储在 <encoding> 中的小整数
    unsigned int len; /* Bytes used to represent the actual entry.
                         For strings this is just the string length
                         while for integers it is 1, 2, 3, 4, 8 or
                         0 (for 4 bit immediate) depending on the
                         number range. */

    // 记录当前 entry 中 header 的长度，等于 prevrawlensize + lensize
    unsigned int headersize; /* prevrawlensize + lensize. */

    // 记录当前 entry 中数据的编码方式
    unsigned char encoding; /* Set to ZIP_STR_* or ZIP_INT_* depending on
                               the entry encoding. However for 4 bits
                               immediate integers this can assume a range
                               of values and must be range-checked. */

    // 保存指向当前 entry 起始位置的指针，而这个指针指向的是当前节点的 <prevlen> 字段
    unsigned char *p; /* Pointer to the very start of the entry, that
                          is, this points to prev-entry-len field. */
} zlentry;

#define ZIPLIST_ENTRY_ZERO(zle)                              \
    {                                                        \
        (zle)->prevrawlensize = (zle)->prevrawlen = 0;       \
        (zle)->lensize = (zle)->len = (zle)->headersize = 0; \
        (zle)->encoding = 0;                                 \
        (zle)->p = NULL;                                     \
    }

/**
 * Extract the encoding from the byte pointed by 'ptr' and set it into
 * 'encoding' field of the zlentry structure.
 *
 * 从 ptr 指针指向的 <encoding> 字段中解析出对应的编码方式: 整型编码或者字符串编码，并将编码方式通过 encoding 返回
 *
 * @in_param: ptr 指向 entry 的 <encoding> 字段的指针
 * @out_param: encoding 解析出的编码方式
 */
#define ZIP_ENTRY_ENCODING(ptr, encoding) \
    do                                    \
    {                                     \
        (encoding) = (ptr[0]);            \
        if ((encoding) < ZIP_STR_MASK)    \
            (encoding) &= ZIP_STR_MASK;   \
    } while (0)

/* Return bytes needed to store integer encoded by 'encoding'. */
// 获取指定 encoding 对应的 <entry-data> 的内存大小
unsigned int zipIntSize(unsigned char encoding)
{
    switch (encoding)
    {
    case ZIP_INT_8B:
        return 1;
    case ZIP_INT_16B:
        return 2;
    case ZIP_INT_24B:
        return 3;
    case ZIP_INT_32B:
        return 4;
    case ZIP_INT_64B:
        return 8;
    }

    // 如果是小整数，不需要额外的存储空间来存储 <entry-data>，直接存储在 <encoding> 字段
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        return 0; /* 4 bit immediate */

    panic("Invalid integer encoding 0x%02X", encoding);
    return 0;
}

/* Write the encoding header of the entry in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. Arguments:
 *
 * 'encoding' is the encoding we are using for the entry. It could be
 * ZIP_INT_* or ZIP_STR_* or between ZIP_INT_IMM_MIN and ZIP_INT_IMM_MAX
 * for single-byte small immediate integers.
 *
 * 'rawlen' is only used for ZIP_STR_* encodings and is the length of the
 * string that this entry represents.
 *
 * The function returns the number of bytes used by the encoding/length
 * header stored in 'p'.
 *
 * p: 指向 entry 的 <encoding> 字段的指针
 * encoding: 编码方式
 * rawlen: 字符串的长度
 *
 * 根据 encoding 和 rawlen 计算 entry 的 <encoding> 字段的长度:
 * 1. 如果是整数编码，<encoding> 字段为 1 字节
 * 2. 如果是字符串编码
 *    2.1 字符串长度小于等于 63 字节：<encoding> 字段为 1 字节
 *    2.2 字符串长度在 64 ~ 16383 字节之间：<encoding> 字段为 2 字节
 *    2.3 字符串长度超过 16383：<encoding> 字段为 5 字节
 *
 * 1. p == null, 只返回 <encoding> 字段的长度
 * 2. p != null, 将 rawlen 按照 <encoding> 设置到 p 指向的内存中，并返回 <encoding> 字段的长度
 *
 **/
unsigned int zipStoreEntryEncoding(unsigned char *p, unsigned char encoding, unsigned int rawlen)
{
    unsigned char len = 1, buf[5];

    if (ZIP_IS_STR(encoding))
    {
        /* Although encoding is given it may not be set for strings,
         * so we determine it here using the raw length. */
        if (rawlen <= 0x3f)
        {
            if (!p)
                return len; // 字符串长度小于等于 63 字节：<encoding> 字段为 1 字节

            buf[0] = ZIP_STR_06B | rawlen;
        }
        else if (rawlen <= 0x3fff)
        {
            len += 1; // 字符串长度在 64 ~ 16383 字节之间：<encoding> 字段为 2 字节
            if (!p)
                return len;

            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;
        }
        else
        {
            len += 4; // 字符串长度超过 16383：<encoding> 字段为 5 字节
            if (!p)
                return len;

            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }
    }
    else
    {
        /* Implies integer encoding, so length is always 1. */
        if (!p)
            return len; // 整数编码，<encoding> 字段为 1 字节
        buf[0] = encoding;
    }

    /* Store this length at p. */
    memcpy(p, buf, len);
    return len;
}

/**
 * Decode the entry encoding type and data length (string length for strings,
 * number of bytes used for the integer for integer entries) encoded in 'ptr'.
 * The 'encoding' variable will hold the entry encoding, the 'lensize'
 * variable will hold the number of bytes required to encode the entry
 * length, and the 'len' variable will hold the entry length.
 *
 * 从 ptr 指针指向的 <encoding> 字段中解析出对应的编码方式: 整型编码还是字符串编码。将编码方式通过 encoding 返回
 *
 * @in_param:
 *      ptr 指向 entry 的 <encoding> 字段的指针
 *
 * @out_param:
 *      encoding: <encoding> 字段
 *      lensize: <encoding> 字段的大小：1，2 或者 5
 *      len: <entry-data> 字段占用的内存大小
 */
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len)               \
    do                                                               \
    {                                                                \
        ZIP_ENTRY_ENCODING((ptr), (encoding));                       \
        if ((encoding) < ZIP_STR_MASK)                               \
        {                                                            \
            if ((encoding) == ZIP_STR_06B)                           \
            {                                                        \
                (lensize) = 1;                                       \
                (len) = (ptr)[0] & 0x3f;                             \
            }                                                        \
            else if ((encoding) == ZIP_STR_14B)                      \
            {                                                        \
                (lensize) = 2;                                       \
                (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1];         \
            }                                                        \
            else if ((encoding) == ZIP_STR_32B)                      \
            {                                                        \
                (lensize) = 5;                                       \
                (len) = ((ptr)[1] << 24) |                           \
                        ((ptr)[2] << 16) |                           \
                        ((ptr)[3] << 8) |                            \
                        ((ptr)[4]);                                  \
            }                                                        \
            else                                                     \
            {                                                        \
                panic("Invalid string encoding 0x%02X", (encoding)); \
            }                                                        \
        }                                                            \
        else                                                         \
        {                                                            \
            (lensize) = 1;                                           \
            (len) = zipIntSize(encoding);                            \
        }                                                            \
    } while (0)

/* Encode the length of the previous entry and write it to "p". This only
 * uses the larger encoding (required in __ziplistCascadeUpdate).
 *
 * 如果 entry 的 <prevlen> 表示的数据超过 253 时，则 <prevlen> 需要用 5 个字节来表示。第一个字节为 0xFE, 后面四个字节为 len
 */
int zipStorePrevEntryLengthLarge(unsigned char *p, unsigned int len)
{
    if (p != NULL)
    {
        p[0] = ZIP_BIG_PREVLEN;
        memcpy(p + 1, &len, sizeof(len));
        memrev32ifbe(p + 1);
    }
    return 1 + sizeof(len);
}

/* Encode the length of the previous entry and write it to "p". Return the
 * number of bytes needed to encode this length if "p" is NULL.
 *
 * 计算存储长度为 len 的数据需要的 <prevlen> 字段的长度: 1 字节或者 5 字节。当 len 小于 254 时，<prevlen> 字段为 1 字节；否则 <prevlen> 为 5 字节
 *
 * 当 p != null 时: 将 len 写入到 p 指向的内存中, 并返回 <prevlen> 字段的长度
 * 当 p == null 时: 返回 <prevlen> 字段的长度
 *
 * p: 指向 entry 的 <prevlen> 字段的指针
 *
 */
unsigned int zipStorePrevEntryLength(unsigned char *p, unsigned int len)
{
    if (p == NULL)
    {
        return (len < ZIP_BIG_PREVLEN) ? 1 : sizeof(len) + 1;       // 1 字节 或者 5 字节
    }
    else
    {
        if (len < ZIP_BIG_PREVLEN)
        {
            p[0] = len;
            return 1;
        }
        else
        {
            return zipStorePrevEntryLengthLarge(p, len);
        }
    }
}

/**
 * Return the number of bytes used to encode the length of the previous
 * entry. The length is returned by setting the var 'prevlensize'.
 *
 * 从 ptr 指针指向的 <prevlen> 字段中解析出 <prevlen> 字段的长度: 1 字节还是 5 字节, 并将结果通过 prevlensize 返回
 *
 * @in_param: ptr 指向 entry 的 <prevlen> 字段的指针
 * @out_param: prevlensize <prevlen> 字段的大小：1 或者 5
 */
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) \
    do                                           \
    {                                            \
        if ((ptr)[0] < ZIP_BIG_PREVLEN)          \
        {                                        \
            (prevlensize) = 1;                   \
        }                                        \
        else                                     \
        {                                        \
            (prevlensize) = 5;                   \
        }                                        \
    } while (0)

/**
 * Return the length of the previous element, and the number of bytes that
 * are used in order to encode the previous element length.
 * 'ptr' must point to the prevlen prefix of an entry (that encodes the
 * length of the previous entry in order to navigate the elements backward).
 * The length of the previous entry is stored in 'prevlen', the number of
 * bytes needed to encode the previous entry length are stored in
 * 'prevlensize'.
 *
 * @in_param:
 *     ptr: 指向 entry 的 <prevlen> 字段的指针
 *
 * @out_param:
 *     prevlensize: <prevlen> 字段的大小。如果 <prevlen> 字段中存储的数据小于 254，则 <prevlen> 为 1 字节；否则为 5 字节
 *     prevlen: <prevlen> 字段存储的数据大小
 */
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen)   \
    do                                                  \
    {                                                   \
        ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);       \
        if ((prevlensize) == 1)                         \
        {                                               \
            (prevlen) = (ptr)[0];                       \
        }                                               \
        else if ((prevlensize) == 5)                    \
        {                                               \
            assert(sizeof((prevlen)) == 4);             \
            memcpy(&(prevlen), ((char *)(ptr)) + 1, 4); \
            memrev32ifbe(&prevlen);                     \
        }                                               \
    } while (0)

/* Given a pointer 'p' to the prevlen info that prefixes an entry, this
 * function returns the difference in number of bytes needed to encode
 * the prevlen if the previous entry changes of size.
 *
 * So if A is the number of bytes used right now to encode the 'prevlen'
 * field.
 *
 * And B is the number of bytes that are needed in order to encode the
 * 'prevlen' if the previous element will be updated to one of size 'len'.
 *
 * Then the function returns B - A
 *
 * So the function returns a positive number if more space is needed,
 * a negative number if less space is needed, or zero if the same space
 * is needed.
 *
 * 计算 p 指向的 <prevlen> 字段的大小与 len 需要的 <prevlen> 字段的大小的差值
 */
int zipPrevLenByteDiff(unsigned char *p, unsigned int len)
{
    unsigned int prevlensize;
    ZIP_DECODE_PREVLENSIZE(p, prevlensize); // prevlensize 为 1 或者 5
    return zipStorePrevEntryLength(NULL, len) - prevlensize;
}

/* Return the total number of bytes used by the entry pointed to by 'p'.
 *
 * 计算 p 指向的 entry 节点占用的内存大小
 * p: 指向 entry 的指针
 */
unsigned int zipRawEntryLength(unsigned char *p)
{
    unsigned int prevlensize, encoding, lensize, len;
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);                     // 计算得到 <prevlen> 字段的大小
    ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len); // 计算 <encoding> 字段和 <entry-data> 字段的大小
    return prevlensize + lensize + len;
}

/* Check if string pointed to by 'entry' can be encoded as an integer.
 * Stores the integer value in 'v' and its encoding in 'encoding'.
 *
 * 判断 entry 指针以及 entrylen 所描述的内存中保存的字符串是否可以转化成长整型
 * 1. 如果可以转化成长整型，那么会通过 v 返回转换后的长整型，并通过 encoding 返回长整型对应的编码，同时函数返回 1;
 * 2. 如果无法转化长整型，那么这个函数会返回 0，表示这个是一个字符串形式的数据
 */
int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding)
{
    long long value;

    // FIXME: 为什么要求 entrylen 在 0~32 之间?
    if (entrylen >= 32 || entrylen == 0)
        return 0;

    if (string2ll((char *)entry, entrylen, &value))     // 将字符串 entry 转换为长整数 value
    {
        /* Great, the string can be encoded. Check what's the smallest
         * of our encoding types that can hold this value. */
        if (value >= 0 && value <= 12)
        {
            *encoding = ZIP_INT_IMM_MIN + value;        // 直接使用 <encoding> 字段来存储小整数
        }
        else if (value >= INT8_MIN && value <= INT8_MAX)
        {
            *encoding = ZIP_INT_8B;
        }
        else if (value >= INT16_MIN && value <= INT16_MAX)
        {
            *encoding = ZIP_INT_16B;
        }
        else if (value >= INT24_MIN && value <= INT24_MAX)
        {
            *encoding = ZIP_INT_24B;
        }
        else if (value >= INT32_MIN && value <= INT32_MAX)
        {
            *encoding = ZIP_INT_32B;
        }
        else
        {
            *encoding = ZIP_INT_64B;
        }

        *v = value;
        return 1;
    }

    return 0;
}

/* Store integer 'value' at 'p', encoded as 'encoding' */
/**
 * 根据给定的编码方式 encoding，将 value 存储在 p 指向的位置。和 zipLoadInteger() 接口对应
 * p: 指向 <entry-data> 字段的指针
 */
void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding)
{
    int16_t i16;
    int32_t i32;
    int64_t i64;
    if (encoding == ZIP_INT_8B)
    {
        ((int8_t *)p)[0] = (int8_t)value;
    }
    else if (encoding == ZIP_INT_16B)
    {
        i16 = value;
        memcpy(p, &i16, sizeof(i16));
        memrev16ifbe(p);
    }
    else if (encoding == ZIP_INT_24B)
    {
        i32 = value << 8;
        memrev32ifbe(&i32);
        memcpy(p, ((uint8_t *)&i32) + 1, sizeof(i32) - sizeof(uint8_t));
    }
    else if (encoding == ZIP_INT_32B)
    {
        i32 = value;
        memcpy(p, &i32, sizeof(i32));
        memrev32ifbe(p);
    }
    else if (encoding == ZIP_INT_64B)
    {
        i64 = value;
        memcpy(p, &i64, sizeof(i64));
        memrev64ifbe(p);
    }
    else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
    {
        /* Nothing to do, the value is stored in the encoding itself. */
    }
    else
    {
        assert(NULL);
    }
}

/* Read integer encoded as 'encoding' from 'p' */
/**
 * 根据给定的编码方式 encoding，从 p 指向的内存中，解码出相应的整数数值。和 zipSaveInteger() 接口对应
 * p: 指向 <entry-data> 字段的指针
 */
int64_t zipLoadInteger(unsigned char *p, unsigned char encoding)
{
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    if (encoding == ZIP_INT_8B)
    {
        ret = ((int8_t *)p)[0];
    }
    else if (encoding == ZIP_INT_16B)
    {
        memcpy(&i16, p, sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    }
    else if (encoding == ZIP_INT_32B)
    {
        memcpy(&i32, p, sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    }
    else if (encoding == ZIP_INT_24B)
    {
        i32 = 0;
        memcpy(((uint8_t *)&i32) + 1, p, sizeof(i32) - sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32 >> 8;
    }
    else if (encoding == ZIP_INT_64B)
    {
        memcpy(&i64, p, sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    }
    else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
    {
        ret = (encoding & ZIP_INT_IMM_MASK) - 1;
    }
    else
    {
        assert(NULL);
    }
    return ret;
}

/* Return a struct with all information about an entry. */
/**
 * 从指针 p 指向的内存中解码出 zlentry 结构: 将 ziplist 的 entry 节点转化为 zlentry 结构
 *
 * @in_param:  p 指向 entry 节点的指针
 * @out_param: e 指向 zlentry 结构体的指针
 */
void zipEntry(unsigned char *p, zlentry *e)
{
    // 从指针 p 指向的内存中逐个解析出 zlentry 数据，然后赋值给 zlentry 指针 e
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    e->headersize = e->prevrawlensize + e->lensize;
    e->p = p;
}

/* Create a new empty ziplist. */
// 创建一个新的 ziplist, 只包含 <zlbytes>、<zltail>、<zllen>、<zlend> 四个字段
unsigned char *ziplistNew(void)
{
    unsigned int bytes = ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE;
    unsigned char *zl = zmalloc(bytes);
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
    ZIPLIST_LENGTH(zl) = 0;
    zl[bytes - 1] = ZIP_END;
    return zl;
}

/* Resize the ziplist. */
// 将 ziplist 的大小更新为 len：用于插入、删除、连锁更新中
unsigned char *ziplistResize(unsigned char *zl, size_t len)
{
    assert(len < UINT32_MAX);
    zl = zrealloc(zl, len);
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);      // 更新 <zlbytes> 字段
    zl[len - 1] = ZIP_END;
    return zl;
}

/* When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIG_PREVLEN, so we need to check that the prevlen can be encoded in
 * every consecutive entry.
 *
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update. */
/**
  * ziplist 的连锁更新：
  *     <entry> 中的 <prevlen> <encoding> <entry-data> 字段是变长的，所以当前一个节点的长度发生变化（比如长度从小于等于 253 字节变成大于 253 字节），当前节点 <prevlen> 字段的长度就会从 1 字节变成 5 字节，也就是当前节点的长度就会发生变化，进而可能导致下一个节点的 <prevlen> 字段发生长度变化....最坏的情况是，每一个节点的更新，都导致了下一个节点 <prevlen> 字段的长度发生变化。理想情况下，对 ziplist 的当前节点 A 的长度修改没有超过下一个节点 B 的 <prevlen> 字段能够表示的范围，那么就不会发生连锁更新。
  *
  * @in_param:
  *      zl: 指向 ziplist 头部的指针
  *      p:  指向 ziplist 的第一个不需要更新的 entry 节点
  */
unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p)
{
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), rawlen, rawlensize; // curlen: 当前 ziplist 的长度
    size_t offset, noffset, extra;
    unsigned char *np;
    zlentry cur, next;

    while (p[0] != ZIP_END)
    {
        zipEntry(p, &cur);
        rawlen = cur.headersize + cur.len;                  // 计算当前 entry 节点中 <prevlen> <encoding> <entry-data> 三个字段的总长度
        rawlensize = zipStorePrevEntryLength(NULL, rawlen); // 计算需要的 <prevlen> 字段的长度

        /* Abort if there is no next entry. */
        if (p[rawlen] == ZIP_END) // 只有一个 entry 节点
            break;
        zipEntry(p + rawlen, &next); // 获取下一个 entry 节点

        /* Abort when "prevlen" has not changed. */
        if (next.prevrawlen == rawlen) // 下一个节点的 <prevlen> 字段中存储的数据 恰好等于 上一个节点的长度
            break;

        // next.prevrawlensize: 下一个节点的 <prevlen> 字段的长度。1 或者 5
        // rawlensize: 当前节点的总长度需要的 <prevlen> 字段的长度。1 或者 5
        if (next.prevrawlensize < rawlensize)
        {
            // 扩容：当前节点的数据长度超过了 253，需要 5 个字节的 <prevlen>，但是下一个节点的 <prevlen> 只有 1 个字节
            /* The "prevlen" field of "next" needs more bytes to hold
             * the raw length of "cur". */
            offset = p - zl;                          // 计算当前节点到 ziplist 头部的偏移
            extra = rawlensize - next.prevrawlensize; // 5 - 1 == 4
            zl = ziplistResize(zl, curlen + extra);   // 扩容 ziplist，增加 4 个字节的 <prevlen>
            p = zl + offset;                          // 因为上面对 ziplist 进行了扩容，所以需要重新根据偏移量获取 entry 节点的地址

            /* Current pointer and offset for next element. */
            np = p + rawlen;   // 相当于 next entry
            noffset = np - zl; // 下一个 entry 到 ziplist 头部的偏移

            /* Update tail offset when next element is not the tail element. */
            if ((zl + intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))) != np)
            {
                // 更新 ziplist 的最后一个节点到头部的偏移量
                ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + extra);
            }

            /* Move the tail to the back. */
            /**
             * next entry 的 <prevlen> 字段本来只有 1 个字节，但是现在需要扩容为 5 个字节，
             * 所以需要将 next entry 的 <prevlen> 字段后面的所有数据向后移动 4 个字节
             *
             * 移动的起始位置为：np + 1。即从 next entry 的 <encoding> 字段开始
             * 移动的目标位置为：np + 5。空出 4 个字节，和原来的 <prevlen> 的 1 个字节用于存储新的 <prevlen> 字段
             * 移动的字节数为：4
             *
             * memmove(*dst, *src, n);
             *
             * 建议画图理解！
             */
            memmove(np + rawlensize,
                    np + next.prevrawlensize,
                    curlen - noffset - next.prevrawlensize - 1);
            zipStorePrevEntryLength(np, rawlen);

            /* Advance the cursor */
            p += rawlen;     // 移动 p 到下一个节点 entry
            curlen += extra; // 更新压缩链表的长度
        }
        else
        {
            // 缩容：当前节点的数据长度小于 253，需要 1 个字节的 <prevlen> 字段，但是下一个节点的 <prevlen> 有 5 个字节
            // 没有进行内存的移动：并没有将 <prevlen> 字段的长度从 5 字节缩小为 1 字节。只是将 rawlen 设置到了 <prevlen> 字段中
            if (next.prevrawlensize > rawlensize)
            {
                /* This would result in shrinking, which we want to avoid.
                 * So, set "rawlen" in the available bytes. */
                zipStorePrevEntryLengthLarge(p + rawlen, rawlen);
            }
            else
            {
                zipStorePrevEntryLength(p + rawlen, rawlen);
            }

            /* Stop here, as the raw length of "next" has not changed. */
            break;
        }
    }

    return zl;
}

/* Delete "num" entries, starting at "p". Returns pointer to the ziplist. */
/**
 * @param:
 *     zl: 指向 ziplist 的头部的指针
 *     p: 指向开始删除的 entry 的指针
 *     num: 要删除的 entry 的数量
 */
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num)
{
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;

    zipEntry(p, &first); // 从 p 指向的地址中解析出一个 zlentry 结构

    for (i = 0; p[0] != ZIP_END && i < num; i++)
    {
        // 移动 p，将中间的节点标记为删除
        p += zipRawEntryLength(p);
        deleted++;
    }

    totlen = p - first.p; /* Bytes taken by the element(s) to delete. */ // 被删除的所有节点的总长度
    if (totlen > 0)
    {
        if (p[0] != ZIP_END)
        {
            /* Storing `prevrawlen` in this entry may increase or decrease the
             * number of bytes required compare to the current `prevrawlen`.
             * There always is room to store this, because it was previously
             * stored by an entry that is now being deleted. */
            nextdiff = zipPrevLenByteDiff(p, first.prevrawlen);

            /* Note that there is always space when p jumps backward: if
             * the new previous entry is large, one of the deleted elements
             * had a 5 bytes prevlen header, so there is for sure at least
             * 5 bytes free and we need just 4. */
            p -= nextdiff;
            zipStorePrevEntryLength(p, first.prevrawlen);

            /* Update offset for tail */
            ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) - totlen);

            /* When the tail contains more than one entry, we need to take
             * "nextdiff" in account as well. Otherwise, a change in the
             * size of prevlen doesn't have an effect on the *tail* offset. */
            zipEntry(p, &tail);
            if (p[tail.headersize + tail.len] != ZIP_END)
            {
                ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + nextdiff);
            }

            /* Move tail to the front of the ziplist */
            memmove(first.p, p, intrev32ifbe(ZIPLIST_BYTES(zl)) - (p - zl) - 1);
        }
        else
        {
            /* The entire tail was deleted. No need to move memory. */
            ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe((first.p - zl) - first.prevrawlen);
        }

        /* Resize and update length */
        offset = first.p - zl;
        zl = ziplistResize(zl, intrev32ifbe(ZIPLIST_BYTES(zl)) - totlen + nextdiff); // 修改 ziplist 的大小
        ZIPLIST_INCR_LENGTH(zl, -deleted);
        p = zl + offset;

        /* When nextdiff != 0, the raw length of the next entry has changed, so
         * we need to cascade the update throughout the ziplist */
        if (nextdiff != 0)
            zl = __ziplistCascadeUpdate(zl, p); // 尝试进行连锁更新
    }
    return zl;
}

/* Insert item at "p". */
// 在压缩链表 zl 中指定节点 p 的前面插入长度为 slen 的字符串 s
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen)
{
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen;
    unsigned int prevlensize, prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. Using a value
                                    that is easy to see if for some reason
                                    we use it uninitialized. */
    zlentry tail;

    /* Find out prevlen for the entry that is inserted. */
    // 计算插入位置前一个节点的大小, 用来填充本节点的 <prevlen> 字段
    if (p[0] != ZIP_END)
    {
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
    }
    else
    {
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        if (ptail[0] != ZIP_END)
        {
            prevlen = zipRawEntryLength(ptail);
        }
    }

    /* See if the entry can be encoded */
    // 尝试将字符串 s 转换成长整型 value
    if (zipTryEncoding(s, slen, &value, &encoding))
    {
        /* 'encoding' is set to the appropriate integer encoding */
        // 可以转换成长整型 value, 编码方式为 encoding
        reqlen = zipIntSize(encoding);      // 计算 <entry-data> 字段的大小
    }
    else
    {
        /* 'encoding' is untouched, however zipStoreEntryEncoding will use the
         * string length to figure out how to encode it. */
        reqlen = slen;  // 无法转换成长整型, <entry-data> 字段的长度为 slen
    }
    /* We need space for both the length of the previous entry and
     * the length of the payload. */
    reqlen += zipStorePrevEntryLength(NULL, prevlen);       // 计算 <prevlen> 字段的长度
    reqlen += zipStoreEntryEncoding(NULL, encoding, slen);  // 计算 <encoding> 字段的长度

    // reqlen 等于 <prevlen><encoding><entry-data> 三个字段的长度总和

    /* When the insert position is not equal to the tail, we need to
     * make sure that the next entry can hold this entry's length in
     * its prevlen field. */
    // 如果 p 不是指向 ziplist 的尾部，需要计算 p 指向的节点的 <prevlen> 字段是否可以存储 reqlen 个字节
    // 因为要插入 p 指向节点的前面，所以需要在这里计算 p 指向节点的 <prevlen> 字段是否满足要求
    int forcelarge = 0;
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p, reqlen) : 0;
    if (nextdiff == -4 && reqlen < 4)
    {
        nextdiff = 0;
        forcelarge = 1;
    }

    /* Store offset because a realloc may change the address of zl. */
    offset = p - zl;    // 保存 p 相对于 ziplist 头部的偏移
    zl = ziplistResize(zl, curlen + reqlen + nextdiff);     // 重新进行内存分配, 可能导致 zl 指针失效, 所以会返回新的 zl 指针
    p = zl + offset;        // 计算新的 p 的地址

    /* Apply memory move when necessary and update tail offset. */
    if (p[0] != ZIP_END)
    {
        /* Subtract one because of the ZIP_END bytes */
        // 将 p 指向的位置后面的内存全部向后移动
        memmove(p + reqlen, p - nextdiff, curlen - offset - 1 + nextdiff);

        /* Encode this entry's raw length in the next entry. */
        // 更新 p 指向节点的 <prevlen> 字段
        if (forcelarge)
            zipStorePrevEntryLengthLarge(p + reqlen, reqlen);
        else
            zipStorePrevEntryLength(p + reqlen, reqlen);

        /* Update offset for tail */
        // 更新尾结点的偏移
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + reqlen);

        /* When the tail contains more than one entry, we need to take
         * "nextdiff" in account as well. Otherwise, a change in the
         * size of prevlen doesn't have an effect on the *tail* offset. */
        zipEntry(p + reqlen, &tail);
        if (p[reqlen + tail.headersize + tail.len] != ZIP_END)
        {
            ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + nextdiff);
        }
    }
    else
    {
        /* This element will be the new tail. */
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p - zl);     // 更新尾结点的偏移
    }

    /* When nextdiff != 0, the raw length of the next entry has changed, so
     * we need to cascade the update throughout the ziplist */
    if (nextdiff != 0)
    {
        offset = p - zl;
        zl = __ziplistCascadeUpdate(zl, p + reqlen);
        p = zl + offset;
    }

    /* Write the entry */
    p += zipStorePrevEntryLength(p, prevlen);
    p += zipStoreEntryEncoding(p, encoding, slen);
    if (ZIP_IS_STR(encoding))
    {
        memcpy(p, s, slen);
    }
    else
    {
        zipSaveInteger(p, value, encoding);
    }
    ZIPLIST_INCR_LENGTH(zl, 1);
    return zl;
}

/* Merge ziplists 'first' and 'second' by appending 'second' to 'first'.
 *
 * NOTE: The larger ziplist is reallocated to contain the new merged ziplist.
 * Either 'first' or 'second' can be used for the result.  The parameter not
 * used will be free'd and set to NULL.
 *
 * After calling this function, the input parameters are no longer valid since
 * they are changed and free'd in-place.
 *
 * The result ziplist is the contents of 'first' followed by 'second'.
 *
 * On failure: returns NULL if the merge is impossible.
 * On success: returns the merged ziplist (which is expanded version of either
 * 'first' or 'second', also frees the other unused input ziplist, and sets the
 * input ziplist argument equal to newly reallocated ziplist return value.
 *
 * 合并两个压缩链表 first 和 second. 将节点数少的合并到节点数多的压缩链表的后面, 然后释放节点数少的压缩链表
 *
 * 较长链表将被重新分配内存以存储两个链表中的所有节点，这个较长链表既可以是 first，也可以是 second。另一个链表的将在合并之后被释放
 */
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second)
{
    /* If any params are null, we can't merge, so NULL. */
    if (first == NULL || *first == NULL || second == NULL || *second == NULL)
        return NULL;

    /* Can't merge same list into itself. */
    if (*first == *second)
        return NULL;

    size_t first_bytes = intrev32ifbe(ZIPLIST_BYTES(*first));
    size_t first_len = intrev16ifbe(ZIPLIST_LENGTH(*first));

    size_t second_bytes = intrev32ifbe(ZIPLIST_BYTES(*second));
    size_t second_len = intrev16ifbe(ZIPLIST_LENGTH(*second));

    int append;
    unsigned char *source, *target;
    size_t target_bytes, source_bytes;
    /* Pick the largest ziplist so we can resize easily in-place.
     * We must also track if we are now appending or prepending to
     * the target ziplist. */
    if (first_len >= second_len)
    {
        /* retain first, append second to first. */
        target = *first;
        target_bytes = first_bytes;
        source = *second;
        source_bytes = second_bytes;
        append = 1;
    }
    else
    {
        /* else, retain second, prepend first to second. */
        target = *second;
        target_bytes = second_bytes;
        source = *first;
        source_bytes = first_bytes;
        append = 0;
    }

    /* Calculate final bytes (subtract one pair of metadata) */
    size_t zlbytes = first_bytes + second_bytes -
                     ZIPLIST_HEADER_SIZE - ZIPLIST_END_SIZE;
    size_t zllength = first_len + second_len;

    /* Combined zl length should be limited within UINT16_MAX */
    zllength = zllength < UINT16_MAX ? zllength : UINT16_MAX;

    /* larger values can't be stored into ZIPLIST_BYTES */
    assert(zlbytes < UINT32_MAX);

    /* Save offset positions before we start ripping memory apart. */
    size_t first_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*first));
    size_t second_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*second));

    /* Extend target to new zlbytes then append or prepend source. */
    target = zrealloc(target, zlbytes);
    if (append)
    {
        /* append == appending to target */
        /* Copy source after target (copying over original [END]):
         *   [TARGET - END, SOURCE - HEADER] */
        memcpy(target + target_bytes - ZIPLIST_END_SIZE,
               source + ZIPLIST_HEADER_SIZE,
               source_bytes - ZIPLIST_HEADER_SIZE);
    }
    else
    {
        /* !append == prepending to target */
        /* Move target *contents* exactly size of (source - [END]),
         * then copy source into vacated space (source - [END]):
         *   [SOURCE - END, TARGET - HEADER] */
        memmove(target + source_bytes - ZIPLIST_END_SIZE,
                target + ZIPLIST_HEADER_SIZE,
                target_bytes - ZIPLIST_HEADER_SIZE);
        memcpy(target, source, source_bytes - ZIPLIST_END_SIZE);
    }

    /* Update header metadata. */
    ZIPLIST_BYTES(target) = intrev32ifbe(zlbytes);
    ZIPLIST_LENGTH(target) = intrev16ifbe(zllength);
    /* New tail offset is:
     *   + N bytes of first ziplist
     *   - 1 byte for [END] of first ziplist
     *   + M bytes for the offset of the original tail of the second ziplist
     *   - J bytes for HEADER because second_offset keeps no header. */
    ZIPLIST_TAIL_OFFSET(target) = intrev32ifbe(
        (first_bytes - ZIPLIST_END_SIZE) +
        (second_offset - ZIPLIST_HEADER_SIZE));

    /* __ziplistCascadeUpdate just fixes the prev length values until it finds a
     * correct prev length value (then it assumes the rest of the list is okay).
     * We tell CascadeUpdate to start at the first ziplist's tail element to fix
     * the merge seam. */
    target = __ziplistCascadeUpdate(target, target + first_offset);

    /* Now free and NULL out what we didn't realloc */
    if (append)
    {
        zfree(*second);
        *second = NULL;
        *first = target;
    }
    else
    {
        zfree(*first);
        *first = NULL;
        *second = target;
    }
    return target;
}

// 向链表的两端执行插入操作，where 标记从头部开始插入还是尾部开始插入
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where)
{
    unsigned char *p;
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);
    return __ziplistInsert(zl, p, s, slen);
}

/* Returns an offset to use for iterating with ziplistNext. When the given
 * index is negative, the list is traversed back to front. When the list
 * doesn't contain an element at the provided index, NULL is returned.
 *
 * 从压缩链表 zl 中返回索引为 index 的数据节点的指针
 * index 为正数时：表示从前向后遍历
 * index 为负数时：表示从后往前遍历
 */
unsigned char *ziplistIndex(unsigned char *zl, int index)
{
    unsigned char *p;
    unsigned int prevlensize, prevlen = 0;
    if (index < 0)
    {
        index = (-index) - 1;
        p = ZIPLIST_ENTRY_TAIL(zl);
        if (p[0] != ZIP_END)
        {
            ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            while (prevlen > 0 && index--)
            {
                p -= prevlen;
                ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            }
        }
    }
    else
    {
        p = ZIPLIST_ENTRY_HEAD(zl);
        while (p[0] != ZIP_END && index--)
        {
            p += zipRawEntryLength(p);
        }
    }
    return (p[0] == ZIP_END || index > 0) ? NULL : p;
}

/* Return pointer to next entry in ziplist.
 *
 * zl is the pointer to the ziplist
 * p is the pointer to the current element
 *
 * The element after 'p' is returned, otherwise NULL if we are at the end.
 *
 * 返回压缩链表 zl 中 p 节点的下一个节点的指针
 */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p)
{
    ((void)zl);

    /* "p" could be equal to ZIP_END, caused by ziplistDelete,
     * and we should return NULL. Otherwise, we should return NULL
     * when the *next* element is ZIP_END (there is no next entry). */
    if (p[0] == ZIP_END)
    {
        return NULL;
    }

    p += zipRawEntryLength(p);  // 计算 p 指向节点的内存大小，然后将 p 向后移动
    if (p[0] == ZIP_END)
    {
        return NULL;
    }

    return p;
}

/* Return pointer to previous entry in ziplist. */
// 返回压缩链表 zl 中 p 节点的前一个节点的指针
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p)
{
    unsigned int prevlensize, prevlen = 0;

    /* Iterating backwards from ZIP_END should return the tail. When "p" is
     * equal to the first element of the list, we're already at the head,
     * and should return NULL. */
    if (p[0] == ZIP_END)
    {
        p = ZIPLIST_ENTRY_TAIL(zl);
        return (p[0] == ZIP_END) ? NULL : p;
    }
    else if (p == ZIPLIST_ENTRY_HEAD(zl))
    {
        return NULL;
    }
    else
    {
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
        assert(prevlen > 0);
        return p - prevlen;
    }
}

/* Get entry pointed to by 'p' and store in either '*sstr' or 'sval' depending
 * on the encoding of the entry. '*sstr' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Return 0 if 'p' points to the end of the ziplist, 1 otherwise.
 *
 * 从 p 指针所指向的数据节点 entry 中解析出其存储的数据:
 * 1. 如果该节点的数据是按照字符串进行编码的话，那么会通过 sval 以及 slen 字段进行返回；
 * 2. 如果是按照整数方式进行编码的话，那么会通过 lval 数值进行返回
 *
 * p: 指向 ziplist 的 entry 的指针
 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval)
{
    zlentry entry;
    if (p == NULL || p[0] == ZIP_END)
        return 0;
    if (sstr)
        *sstr = NULL;

    zipEntry(p, &entry);
    if (ZIP_IS_STR(entry.encoding))
    {
        if (sstr)
        {
            *slen = entry.len;
            *sstr = p + entry.headersize;
        }
    }
    else
    {
        if (sval)
        {
            *sval = zipLoadInteger(p + entry.headersize, entry.encoding);
        }
    }
    return 1;
}

/* Insert an entry at "p". */
/**
 * 在压缩链表 zl 中指定节点 p 的前面插入长度为 slen 的字符串 s
 *
 * zl: 指向 ziplist 头部的指针
 * p: 指向要开始插入的节点 entry 的指针
 * s: 指向要插入的数据的指针
 * slen: 要插入的数据长度
 *
 * NOTE: 只能插入到指定节点 p 的前面
 */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen)
{
    return __ziplistInsert(zl, p, s, slen);
}

/* Delete a single entry from the ziplist, pointed to by *p.
 * Also update *p in place, to be able to iterate over the
 * ziplist, while deleting entries.
 *
 * 从压缩链表 zl 中删除 p 指向的节点，并返回新的压缩链表的指针
 *
 * zl: 压缩链表
 * p: 指向压缩链表中的某一个数据节点 entry 的指针，删除该数据节点之后，将新的数据节点的指针赋给 p
 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p)
{
    size_t offset = *p - zl;         // 计算该数据节点 entry 在压缩链表 zl 中的偏移量
    zl = __ziplistDelete(zl, *p, 1); // 从压缩链表中删除数据节点 p

    /* Store pointer to current element in p, because ziplistDelete will
     * do a realloc which might result in a different "zl"-pointer.
     * When the delete direction is back to front, we might delete the last
     * entry and end up with "p" pointing to ZIP_END, so check this. */
    *p = zl + offset; // 计算偏移量为 offset 的新的数据节点 p
    return zl;
}

/* Delete a range of entries from the ziplist. */
// 从压缩链表 zl 给定的索引 index 开始删除 num 个节点
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num)
{
    unsigned char *p = ziplistIndex(zl, index);
    return (p == NULL) ? zl : __ziplistDelete(zl, p, num);
}

/* Compare entry pointer to by 'p' with 'sstr' of length 'slen'. */
/* Return 1 if equal. */
// 比较 p 指针对应的压缩链表节点中的数据是否等于 sstr 以及 slen 参数描述的数据
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen)
{
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;
    if (p[0] == ZIP_END)
        return 0;

    zipEntry(p, &entry);
    if (ZIP_IS_STR(entry.encoding))
    {
        /* Raw compare */
        if (entry.len == slen)
        {
            return memcmp(p + entry.headersize, sstr, slen) == 0;
        }
        else
        {
            return 0;
        }
    }
    else
    {
        /* Try to compare encoded values. Don't compare encoding because
         * different implementations may encoded integers differently. */
        if (zipTryEncoding(sstr, slen, &sval, &sencoding))
        {
            zval = zipLoadInteger(p + entry.headersize, entry.encoding);
            return zval == sval;
        }
    }
    return 0;
}

/* Find pointer to the entry equal to the specified entry. Skip 'skip' entries
 * between every comparison. Returns NULL when the field could not be found.
 * 从 p 指针对应的节点开始，以 skip 为步长，来查找是否存在所存储的数据等于 vstr 的节点。
 * 如果找到，那么会返回这个节点的指针，否则会返回 NULL。如果这个 skip 传入 0 的话，那么意味着按照顺序从 p 节点开始依次向后查找，不会跳过任何一个节点。
 */
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip)
{
    int skipcnt = 0;
    unsigned char vencoding = 0;
    long long vll = 0;

    while (p[0] != ZIP_END)
    {
        unsigned int prevlensize, encoding, lensize, len;
        unsigned char *q;

        ZIP_DECODE_PREVLENSIZE(p, prevlensize);
        ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
        q = p + prevlensize + lensize;

        if (skipcnt == 0)
        {
            /* Compare current entry with specified entry */
            if (ZIP_IS_STR(encoding))
            {
                if (len == vlen && memcmp(q, vstr, vlen) == 0)
                {
                    return p;
                }
            }
            else
            {
                /* Find out if the searched field can be encoded. Note that
                 * we do it only the first time, once done vencoding is set
                 * to non-zero and vll is set to the integer value. */
                if (vencoding == 0)
                {
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding))
                    {
                        /* If the entry can't be encoded we set it to
                         * UCHAR_MAX so that we don't retry again the next
                         * time. */
                        vencoding = UCHAR_MAX;
                    }
                    /* Must be non-zero by now */
                    assert(vencoding);
                }

                /* Compare current entry with specified entry, do it only
                 * if vencoding != UCHAR_MAX because if there is no encoding
                 * possible for the field it can't be a valid integer. */
                if (vencoding != UCHAR_MAX)
                {
                    long long ll = zipLoadInteger(q, encoding);
                    if (ll == vll)
                    {
                        return p;
                    }
                }
            }

            /* Reset skip count */
            skipcnt = skip;
        }
        else
        {
            /* Skip entry */
            skipcnt--;
        }

        /* Move to next entry */
        p = q + len;
    }

    return NULL;
}

/* Return length of ziplist. */
// 获取 ziplist 的节点数量：如果小于 UINT16_MAX，则可以通过 <zllen> 字段获取；否则需要遍历整个链表
unsigned int ziplistLen(unsigned char *zl)
{
    unsigned int len = 0;
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX)
    {
        len = intrev16ifbe(ZIPLIST_LENGTH(zl));
    }
    else
    {
        unsigned char *p = zl + ZIPLIST_HEADER_SIZE;
        while (*p != ZIP_END)
        {
            p += zipRawEntryLength(p);
            len++;
        }

        /* Re-store length if small enough */
        if (len < UINT16_MAX)
            ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }
    return len;
}

/* Return ziplist blob size in bytes. */
// 获取 ziplist 占用内存的大小：直接读取 <zlbytes> 字段
size_t ziplistBlobLen(unsigned char *zl)
{
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

void ziplistRepr(unsigned char *zl)
{
    unsigned char *p;
    int index = 0;
    zlentry entry;

    printf(
        "{total bytes %d} "
        "{num entries %u}\n"
        "{tail offset %u}\n",
        intrev32ifbe(ZIPLIST_BYTES(zl)),
        intrev16ifbe(ZIPLIST_LENGTH(zl)),
        intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)));
    p = ZIPLIST_ENTRY_HEAD(zl);
    while (*p != ZIP_END)
    {
        zipEntry(p, &entry);
        printf(
            "{\n"
            "\taddr 0x%08lx,\n"
            "\tindex %2d,\n"
            "\toffset %5ld,\n"
            "\thdr+entry len: %5u,\n"
            "\thdr len%2u,\n"
            "\tprevrawlen: %5u,\n"
            "\tprevrawlensize: %2u,\n"
            "\tpayload %5u\n",
            (long unsigned)p,
            index,
            (unsigned long)(p - zl),
            entry.headersize + entry.len,
            entry.headersize,
            entry.prevrawlen,
            entry.prevrawlensize,
            entry.len);
        printf("\tbytes: ");
        for (unsigned int i = 0; i < entry.headersize + entry.len; i++)
        {
            printf("%02x|", p[i]);
        }
        printf("\n");
        p += entry.headersize;
        if (ZIP_IS_STR(entry.encoding))
        {
            printf("\t[str]");
            if (entry.len > 40)
            {
                if (fwrite(p, 40, 1, stdout) == 0)
                    perror("fwrite");
                printf("...");
            }
            else
            {
                if (entry.len &&
                    fwrite(p, entry.len, 1, stdout) == 0)
                    perror("fwrite");
            }
        }
        else
        {
            printf("\t[int]%lld", (long long)zipLoadInteger(p, entry.encoding));
        }
        printf("\n}\n");
        p += entry.len;
        index++;
    }
    printf("{end}\n\n");
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include "adlist.h"
#include "sds.h"

#define debug(f, ...)               \
    {                               \
        if (DEBUG)                  \
            printf(f, __VA_ARGS__); \
    }

static unsigned char *createList()
{
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char *)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char *)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char *)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

static unsigned char *createIntList()
{
    unsigned char *zl = ziplistNew();
    char buf[32];

    sprintf(buf, "100");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "128000");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "-100");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "4294967296");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "non integer");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

static long long usec(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec) * 1000000) + tv.tv_usec;
}

static void stress(int pos, int num, int maxsize, int dnum)
{
    int i, j, k;
    unsigned char *zl;
    char posstr[2][5] = {"HEAD", "TAIL"};
    long long start;
    for (i = 0; i < maxsize; i += dnum)
    {
        zl = ziplistNew();
        for (j = 0; j < i; j++)
        {
            zl = ziplistPush(zl, (unsigned char *)"quux", 4, ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        start = usec();
        for (k = 0; k < num; k++)
        {
            zl = ziplistPush(zl, (unsigned char *)"quux", 4, pos);
            zl = ziplistDeleteRange(zl, 0, 1);
        }
        printf("List size: %8d, bytes: %8d, %dx push+pop (%s): %6lld usec\n",
               i, intrev32ifbe(ZIPLIST_BYTES(zl)), num, posstr[pos], usec() - start);
        zfree(zl);
    }
}

static unsigned char *pop(unsigned char *zl, int where)
{
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong;

    p = ziplistIndex(zl, where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p, &vstr, &vlen, &vlong))
    {
        if (where == ZIPLIST_HEAD)
            printf("Pop head: ");
        else
            printf("Pop tail: ");

        if (vstr)
        {
            if (vlen && fwrite(vstr, vlen, 1, stdout) == 0)
                perror("fwrite");
        }
        else
        {
            printf("%lld", vlong);
        }

        printf("\n");
        return ziplistDelete(zl, &p);
    }
    else
    {
        printf("ERROR: Could not pop\n");
        exit(1);
    }
}

static int randstring(char *target, unsigned int min, unsigned int max)
{
    int p = 0;
    int len = min + rand() % (max - min + 1);
    int minval, maxval;
    switch (rand() % 3)
    {
    case 0:
        minval = 0;
        maxval = 255;
        break;
    case 1:
        minval = 48;
        maxval = 122;
        break;
    case 2:
        minval = 48;
        maxval = 52;
        break;
    default:
        assert(NULL);
    }

    while (p < len)
        target[p++] = minval + rand() % (maxval - minval + 1);
    return len;
}

static void verify(unsigned char *zl, zlentry *e)
{
    int len = ziplistLen(zl);
    zlentry _e;

    ZIPLIST_ENTRY_ZERO(&_e);

    for (int i = 0; i < len; i++)
    {
        memset(&e[i], 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, i), &e[i]);

        memset(&_e, 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, -len + i), &_e);

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

int ziplistTest(int argc, char **argv)
{
    unsigned char *zl, *p;
    unsigned char *entry;
    unsigned int elen;
    long long value;

    /* If an argument is given, use it as the random seed. */
    if (argc == 2)
        srand(atoi(argv[1]));

    zl = createIntList();
    ziplistRepr(zl);

    zfree(zl);

    zl = createList();
    ziplistRepr(zl);

    zl = pop(zl, ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl, ZIPLIST_HEAD);
    ziplistRepr(zl);

    zl = pop(zl, ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl, ZIPLIST_TAIL);
    ziplistRepr(zl);

    zfree(zl);

    printf("Get element at index 3:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 3);
        if (!ziplistGet(p, &entry, &elen, &value))
        {
            printf("ERROR: Could not access index 3\n");
            return 1;
        }
        if (entry)
        {
            if (elen && fwrite(entry, elen, 1, stdout) == 0)
                perror("fwrite");
            printf("\n");
        }
        else
        {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index 4 (out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (p == NULL)
        {
            printf("No entry\n");
        }
        else
        {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p - zl);
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -1 (last element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        if (!ziplistGet(p, &entry, &elen, &value))
        {
            printf("ERROR: Could not access index -1\n");
            return 1;
        }
        if (entry)
        {
            if (elen && fwrite(entry, elen, 1, stdout) == 0)
                perror("fwrite");
            printf("\n");
        }
        else
        {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -4 (first element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -4);
        if (!ziplistGet(p, &entry, &elen, &value))
        {
            printf("ERROR: Could not access index -4\n");
            return 1;
        }
        if (entry)
        {
            if (elen && fwrite(entry, elen, 1, stdout) == 0)
                perror("fwrite");
            printf("\n");
        }
        else
        {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -5 (reverse out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -5);
        if (p == NULL)
        {
            printf("No entry\n");
        }
        else
        {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p - zl);
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 0 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value))
        {
            printf("Entry: ");
            if (entry)
            {
                if (elen && fwrite(entry, elen, 1, stdout) == 0)
                    perror("fwrite");
            }
            else
            {
                printf("%lld", value);
            }
            p = ziplistNext(zl, p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 1 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 1);
        while (ziplistGet(p, &entry, &elen, &value))
        {
            printf("Entry: ");
            if (entry)
            {
                if (elen && fwrite(entry, elen, 1, stdout) == 0)
                    perror("fwrite");
            }
            else
            {
                printf("%lld", value);
            }
            p = ziplistNext(zl, p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 2 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 2);
        while (ziplistGet(p, &entry, &elen, &value))
        {
            printf("Entry: ");
            if (entry)
            {
                if (elen && fwrite(entry, elen, 1, stdout) == 0)
                    perror("fwrite");
            }
            else
            {
                printf("%lld", value);
            }
            p = ziplistNext(zl, p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate starting out of range:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (!ziplistGet(p, &entry, &elen, &value))
        {
            printf("No entry\n");
        }
        else
        {
            printf("ERROR\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value))
        {
            printf("Entry: ");
            if (entry)
            {
                if (elen && fwrite(entry, elen, 1, stdout) == 0)
                    perror("fwrite");
            }
            else
            {
                printf("%lld", value);
            }
            p = ziplistPrev(zl, p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front, deleting all items:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value))
        {
            printf("Entry: ");
            if (entry)
            {
                if (elen && fwrite(entry, elen, 1, stdout) == 0)
                    perror("fwrite");
            }
            else
            {
                printf("%lld", value);
            }
            zl = ziplistDelete(zl, &p);
            p = ziplistPrev(zl, p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Delete inclusive range 0,0:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 0,1:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 1,2:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with start index out of range:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 5, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with num overflow:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 5);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete foo while iterating:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value))
        {
            if (entry && strncmp("foo", (char *)entry, elen) == 0)
            {
                printf("Delete foo\n");
                zl = ziplistDelete(zl, &p);
            }
            else
            {
                printf("Entry: ");
                if (entry)
                {
                    if (elen && fwrite(entry, elen, 1, stdout) == 0)
                        perror("fwrite");
                }
                else
                {
                    printf("%lld", value);
                }
                p = ziplistNext(zl, p);
                printf("\n");
            }
        }
        printf("\n");
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Regression test for >255 byte strings:\n");
    {
        char v1[257] = {0}, v2[257] = {0};
        memset(v1, 'x', 256);
        memset(v2, 'y', 256);
        zl = ziplistNew();
        zl = ziplistPush(zl, (unsigned char *)v1, strlen(v1), ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *)v2, strlen(v2), ZIPLIST_TAIL);

        /* Pop values again and compare their value. */
        p = ziplistIndex(zl, 0);
        assert(ziplistGet(p, &entry, &elen, &value));
        assert(strncmp(v1, (char *)entry, elen) == 0);
        p = ziplistIndex(zl, 1);
        assert(ziplistGet(p, &entry, &elen, &value));
        assert(strncmp(v2, (char *)entry, elen) == 0);
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Regression test deleting next to last entries:\n");
    {
        char v[3][257] = {{0}};
        zlentry e[3] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0, .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};
        size_t i;

        for (i = 0; i < (sizeof(v) / sizeof(v[0])); i++)
        {
            memset(v[i], 'a' + i, sizeof(v[0]));
        }

        v[0][256] = '\0';
        v[1][1] = '\0';
        v[2][256] = '\0';

        zl = ziplistNew();
        for (i = 0; i < (sizeof(v) / sizeof(v[0])); i++)
        {
            zl = ziplistPush(zl, (unsigned char *)v[i], strlen(v[i]), ZIPLIST_TAIL);
        }

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);
        assert(e[2].prevrawlensize == 1);

        /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
        unsigned char *p = e[1].p;
        zl = ziplistDelete(zl, &p);

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);

        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Create long list and check indices:\n");
    {
        zl = ziplistNew();
        char buf[32];
        int i, len;
        for (i = 0; i < 1000; i++)
        {
            len = sprintf(buf, "%d", i);
            zl = ziplistPush(zl, (unsigned char *)buf, len, ZIPLIST_TAIL);
        }
        for (i = 0; i < 1000; i++)
        {
            p = ziplistIndex(zl, i);
            assert(ziplistGet(p, NULL, NULL, &value));
            assert(i == value);

            p = ziplistIndex(zl, -i - 1);
            assert(ziplistGet(p, NULL, NULL, &value));
            assert(999 - i == value);
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Compare strings with ziplist entries:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        if (!ziplistCompare(p, (unsigned char *)"hello", 5))
        {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p, (unsigned char *)"hella", 5))
        {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl, 3);
        if (!ziplistCompare(p, (unsigned char *)"1024", 4))
        {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p, (unsigned char *)"1025", 4))
        {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Merge test:\n");
    {
        /* create list gives us: [hello, foo, quux, 1024] */
        zl = createList();
        unsigned char *zl2 = createList();

        unsigned char *zl3 = ziplistNew();
        unsigned char *zl4 = ziplistNew();

        if (ziplistMerge(&zl4, &zl4))
        {
            printf("ERROR: Allowed merging of one ziplist into itself.\n");
            return 1;
        }

        /* Merge two empty ziplists, get empty result back. */
        zl4 = ziplistMerge(&zl3, &zl4);
        ziplistRepr(zl4);
        if (ziplistLen(zl4))
        {
            printf("ERROR: Merging two empty ziplists created entries.\n");
            return 1;
        }
        zfree(zl4);

        zl2 = ziplistMerge(&zl, &zl2);
        /* merge gives us: [hello, foo, quux, 1024, hello, foo, quux, 1024] */
        ziplistRepr(zl2);

        if (ziplistLen(zl2) != 8)
        {
            printf("ERROR: Merged length not 8, but: %u\n", ziplistLen(zl2));
            return 1;
        }

        p = ziplistIndex(zl2, 0);
        if (!ziplistCompare(p, (unsigned char *)"hello", 5))
        {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p, (unsigned char *)"hella", 5))
        {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2, 3);
        if (!ziplistCompare(p, (unsigned char *)"1024", 4))
        {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p, (unsigned char *)"1025", 4))
        {
            printf("ERROR: \"1025\"\n");
            return 1;
        }

        p = ziplistIndex(zl2, 4);
        if (!ziplistCompare(p, (unsigned char *)"hello", 5))
        {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p, (unsigned char *)"hella", 5))
        {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2, 7);
        if (!ziplistCompare(p, (unsigned char *)"1024", 4))
        {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p, (unsigned char *)"1025", 4))
        {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Stress with random payloads of different encoding:\n");
    {
        int i, j, len, where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        /* Hold temp vars from ziplist */
        unsigned char *sstr;
        unsigned int slen;
        long long sval;

        for (i = 0; i < 20000; i++)
        {
            zl = ziplistNew();
            ref = listCreate();
            listSetFreeMethod(ref, (void (*)(void *))sdsfree);
            len = rand() % 256;

            /* Create lists */
            for (j = 0; j < len; j++)
            {
                where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
                if (rand() % 2)
                {
                    buflen = randstring(buf, 1, sizeof(buf) - 1);
                }
                else
                {
                    switch (rand() % 3)
                    {
                    case 0:
                        buflen = sprintf(buf, "%lld", (0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = sprintf(buf, "%lld", (0LL + rand()));
                        break;
                    case 2:
                        buflen = sprintf(buf, "%lld", (0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* Add to ziplist */
                zl = ziplistPush(zl, (unsigned char *)buf, buflen, where);

                /* Add to reference list */
                if (where == ZIPLIST_HEAD)
                {
                    listAddNodeHead(ref, sdsnewlen(buf, buflen));
                }
                else if (where == ZIPLIST_TAIL)
                {
                    listAddNodeTail(ref, sdsnewlen(buf, buflen));
                }
                else
                {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == ziplistLen(zl));
            for (j = 0; j < len; j++)
            {
                /* Naive way to get elements, but similar to the stresser
                 * executed from the Tcl test suite. */
                p = ziplistIndex(zl, j);
                refnode = listIndex(ref, j);

                assert(ziplistGet(p, &sstr, &slen, &sval));
                if (sstr == NULL)
                {
                    buflen = sprintf(buf, "%lld", sval);
                }
                else
                {
                    buflen = slen;
                    memcpy(buf, sstr, buflen);
                    buf[buflen] = '\0';
                }
                assert(memcmp(buf, listNodeValue(refnode), buflen) == 0);
            }
            zfree(zl);
            listRelease(ref);
        }
        printf("SUCCESS\n\n");
    }

    printf("Stress with variable ziplist size:\n");
    {
        stress(ZIPLIST_HEAD, 100000, 16384, 256);
        stress(ZIPLIST_TAIL, 100000, 16384, 256);
    }

    return 0;
}
#endif
