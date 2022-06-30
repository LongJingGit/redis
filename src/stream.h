#ifndef STREAM_H
#define STREAM_H

#include "rax.h"
#include "listpack.h"

/* Stream item ID: a 128 bit number composed of a milliseconds time and
 * a sequence counter. IDs generated in the same millisecond (or in a past
 * millisecond if the clock jumped backward) will use the millisecond time
 * of the latest generated ID and an incremented sequence. */
// 表示消息 ID 的结构体
typedef struct streamID
{
    uint64_t ms;  // Unix time in milliseconds. 消息产生的时间
    uint64_t seq; // Sequence number. 如果同一毫秒 ms 内产生了多条消息, 会用 seq 进行区分
} streamID;

// Redis 中消息队列
typedef struct stream
{
    rax *rax;         // The radix tree holding the stream. 存储消息队列中的消息, key 为消息 id, value 为紧凑列表 listpack
    uint64_t length;  // Number of elements inside this stream. 存储在消息队列中的消息数量
    streamID last_id; // Zero if there are yet no items. 上一次生成消息的消息 id, 当插入新消息时, 会根据这个 id 来生成新的消息 id
    rax *cgroups;     // Consumer groups dictionary: name -> streamCG. 关联该消息队列的消费者组. 消费者组的名字作为基数树的 key
} stream;

/* We define an iterator to iterate stream items in an abstract way, without
 * caring about the radix tree + listpack representation. Technically speaking
 * the iterator is only used inside streamReplyWithRange(), so could just
 * be implemented inside the function, but practically there is the AOF
 * rewriting code that also needs to iterate the stream to emit the XADD
 * commands. */
typedef struct streamIterator
{
    stream *stream;                     // The stream we are iterating. 该迭代器对应的消息队列的指针
    streamID master_id;                 // ID of the master entry at listpack head. 该迭代器当前所在的紧凑列表 listpack 对应的 master_id
    uint64_t master_fields_count;       // Master entries # of fields. */
    unsigned char *master_fields_start; // Master entries start in listpack. */
    unsigned char *master_fields_ptr;   // Master field to emit next. */
    int entry_flags;                    // Flags of entry we are emitting. */
    int rev;                            // True if iterating end to start (reverse). */
    uint64_t start_key[2];              // Start key as 128 bit big endian. */
    uint64_t end_key[2];                // End key as 128 bit big endian. */
    raxIterator ri;                     // Rax iterator. */
    unsigned char *lp;                  // Current listpack. */
    unsigned char *lp_ele;              // Current listpack cursor. */
    unsigned char *lp_flags;            // Current entry flags pointer. */
    /* Buffers used to hold the string of lpGet() when the element is
     * integer encoded, so that there is no string representation of the
     * element inside the listpack itself. */
    unsigned char field_buf[LP_INTBUF_SIZE];
    unsigned char value_buf[LP_INTBUF_SIZE];
} streamIterator;

// Consumer group. 消费者组
typedef struct streamCG
{
    // 消息分发游标,记录下一条需要发送给消费者的消息 id, 每次消费者请求消息时，消费者组都会将 last_id 对应的消息发送给消费者，然后将 last_id 向后移动一位
    streamID last_id; /* Last delivered (not acknowledged) ID for this
                         group. Consumers that will just ask for more
                         messages will served with IDs > than this. */

    // 存储已经发送但是还没有被确认的消息的列表
    rax *pel;         /* Pending entries list. This is a radix tree that
                         has every message delivered to consumers (without
                         the NOACK option) that was yet not acknowledged
                         as processed. The key of the radix tree is the
                         ID as a 64 bit big endian number, while the
                         associated value is a streamNACK structure.*/

    // 存储消费者组里面的消费者. key 为消费者名字, value 为 streamConsumer
    rax *consumers;   /* A radix tree representing the consumers by name
                         and their associated representation in the form
                         of streamConsumer structures. */
} streamCG;

/* A specific consumer in a consumer group.  消费者组里面的一个消费者 */
typedef struct streamConsumer
{
    // 消费者上次活动的时间戳
    mstime_t seen_time; /* Last time this consumer was active. */

    // 消费者的名字
    sds name; /* Consumer name. This is how the consumer
                 will be identified in the consumer group
                 protocol. Case sensitive. */

    // 该消费者还没有被确认的消息的列表
    rax *pel; /* Consumer specific pending entries list: all
                 the pending messages delivered to this
                 consumer not yet acknowledged. Keys are
                 big endian message IDs, while values are
                 the same streamNACK structure referenced
                 in the "pel" of the consumer group structure
                 itself, so the value is shared. */
} streamConsumer;

/* Pending (yet not acknowledged) message in a consumer group. 已经分发但是还没有被确认的消息 */
typedef struct streamNACK
{
    // 该消息上次被分发的时间戳
    mstime_t delivery_time;   /* Last time this message was delivered. */

    // 该条消息被分发的次数
    uint64_t delivery_count;  /* Number of times this message was delivered.*/

    // 该消息上次被分发的消费者
    streamConsumer *consumer; /* The consumer this message was delivered to
                                   in the last delivery. */
} streamNACK;

/* Stream propagation informations, passed to functions in order to propagate
 * XCLAIM commands to AOF and slaves. */
typedef struct streamPropInfo
{
    robj *keyname;
    robj *groupname;
} streamPropInfo;

/* Prototypes of exported APIs. */
struct client;

/* Flags for streamLookupConsumer */
#define SLC_NONE 0
#define SLC_NOCREAT (1 << 0)   /* Do not create the consumer if it doesn't exist */
#define SLC_NOREFRESH (1 << 1) /* Do not update consumer's seen-time */

stream *streamNew(void);
void freeStream(stream *s);
unsigned long streamLength(const robj *subject);
size_t streamReplyWithRange(client *c, stream *s, streamID *start, streamID *end, size_t count, int rev, streamCG *group, streamConsumer *consumer, int flags, streamPropInfo *spi);
void streamIteratorStart(streamIterator *si, stream *s, streamID *start, streamID *end, int rev);
int streamIteratorGetID(streamIterator *si, streamID *id, int64_t *numfields);
void streamIteratorGetField(streamIterator *si, unsigned char **fieldptr, unsigned char **valueptr, int64_t *fieldlen, int64_t *valuelen);
void streamIteratorStop(streamIterator *si);
streamCG *streamLookupCG(stream *s, sds groupname);
streamConsumer *streamLookupConsumer(streamCG *cg, sds name, int flags);
streamCG *streamCreateCG(stream *s, char *name, size_t namelen, streamID *id);
streamNACK *streamCreateNACK(streamConsumer *consumer);
void streamDecodeID(void *buf, streamID *id);
int streamCompareID(streamID *a, streamID *b);
void streamFreeNACK(streamNACK *na);
void streamIncrID(streamID *id);

#endif
