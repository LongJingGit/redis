#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static size_t lazyfree_objects = 0;
pthread_mutex_t lazyfree_objects_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Return the number of currently pending objects to free. */
size_t lazyfreeGetPendingObjectsCount(void)
{
    size_t aux;
    atomicGet(lazyfree_objects, aux);
    return aux;
}

/* Return the amount of work needed in order to free an object.
 * The return value is not always the actual number of allocations the
 * object is composed of, but a number proportional to it.
 *
 * For strings the function always returns 1.
 *
 * For aggregated objects represented by hash tables or other data structures
 * the function just returns the number of elements the object is composed of.
 *
 * Objects composed of single allocations are always reported as having a
 * single item even if they are actually logical composed of multiple
 * elements.
 *
 * For lists the function returns the number of elements in the quicklist
 * representing the list.
 *
 * 获取对象 obj 中存储节点的数量，以此判断该对象是否需要异步释放。
 */
size_t lazyfreeGetFreeEffort(robj *obj)
{
    if (obj->type == OBJ_LIST)
    {
        quicklist *ql = obj->ptr;
        return ql->len;
    }
    else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT)
    {
        dict *ht = obj->ptr;
        return dictSize(ht);
    }
    else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zset *zs = obj->ptr;
        return zs->zsl->length;
    }
    else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT)
    {
        dict *ht = obj->ptr;
        return dictSize(ht);
    }
    else if (obj->type == OBJ_STREAM)
    {
        size_t effort = 0;
        stream *s = obj->ptr;

        /* Make a best effort estimate to maintain constant runtime. Every macro
         * node in the Stream is one allocation. */
        effort += s->rax->numnodes;

        /* Every consumer group is an allocation and so are the entries in its
         * PEL. We use size of the first group's PEL as an estimate for all
         * others. */
        if (s->cgroups && raxSize(s->cgroups))
        {
            raxIterator ri;
            streamCG *cg;
            raxStart(&ri, s->cgroups);
            raxSeek(&ri, "^", NULL, 0);
            /* There must be at least one group so the following should always
             * work. */
            serverAssert(raxNext(&ri));
            cg = ri.data;
            effort += raxSize(s->cgroups) * (1 + raxSize(cg->pel));
            raxStop(&ri);
        }
        return effort;
    }
    else
    {
        return 1; /* Everything else is a single allocation. */
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB.
 * If there are enough allocations to free the value object may be put into
 * a lazy free list instead of being freed synchronously. The lazy free list
 * will be reclaimed in a different bio.c thread. */
#define LAZYFREE_THRESHOLD 64
int dbAsyncDelete(redisDb *db, robj *key)
{
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    // 过期字典 expires 中存放的是 key-timeout, 不存在释放压力，可以同步释放
    if (dictSize(db->expires) > 0)
        dictDelete(db->expires, key->ptr);

    /* If the value is composed of a few allocations, to free in a lazy way
     * is actually just slower... So under a certain limit we just free
     * the object synchronously. */
    dictEntry *de = dictUnlink(db->dict, key->ptr);
    if (de)
    {
        robj *val = dictGetVal(de);
        size_t free_effort = lazyfreeGetFreeEffort(val); // 获取 val 中存储的元素个数

        /* If releasing the object is too much work, do it in the background
         * by adding the object to the lazy free list.
         * Note that if the object is shared, to reclaim it now it is not
         * possible. This rarely happens, however sometimes the implementation
         * of parts of the Redis core may call incrRefCount() to protect
         * objects, and then call dbDelete(). In this case we'll fall
         * through and reach the dictFreeUnlinkedEntry() call, that will be
         * equivalent to just calling decrRefCount().
         *
         * 如果 val 中存储的元素个数大于 64 并且引用计数为 1 的时候，才会采用异步释放：将 val 递交给 BIO 线程，由 BIO 线程回收
         * NOTE: 这里只将 value 递交给 BIO 线程去异步释放，对于 key 和 entry 本身的释放则由下面进行同步释放
         */
        if (free_effort > LAZYFREE_THRESHOLD && val->refcount == 1)
        {
            atomicIncr(lazyfree_objects, 1);
            bioCreateBackgroundJob(BIO_LAZY_FREE, val, NULL, NULL);
            dictSetVal(db->dict, de, NULL);
        }
    }

    /* Release the key-val pair, or just the key if we set the val
     * field to NULL in order to lazy free it later. */
    // 采用同步释放策略，释放 entry
    if (de)
    {
        dictFreeUnlinkedEntry(db->dict, de);
        if (server.cluster_enabled)
            slotToKeyDel(key->ptr);
        return 1;
    }
    else
    {
        return 0;
    }
}

/* Free an object, if the object is huge enough, free it in async way. */
// 异步释放一个 redisObject 数据，如果该 redisObject 内存储的元素个数大于 64 且引用计数为 1，则递交给后台 BIO 线程去释放，否则只减少其引用计数
void freeObjAsync(robj *o)
{
    size_t free_effort = lazyfreeGetFreeEffort(o);
    if (free_effort > LAZYFREE_THRESHOLD && o->refcount == 1)
    {
        atomicIncr(lazyfree_objects, 1);
        bioCreateBackgroundJob(BIO_LAZY_FREE, o, NULL, NULL);
    }
    else
    {
        decrRefCount(o);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing.
 *
 * 异步清空 db 的主字典和过期字典:
 *    为 db 分配了新的主字典 dict 和过期字典 expires。将旧的主字典和过期字典递交给后台 BIO 线程, 由后台线程去释放
 */
void emptyDbAsync(redisDb *db)
{
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    db->dict = dictCreate(&dbDictType, NULL);
    db->expires = dictCreate(&keyptrDictType, NULL);
    atomicIncr(lazyfree_objects, dictSize(oldht1));
    bioCreateBackgroundJob(BIO_LAZY_FREE, NULL, oldht1, oldht2);
}

/* Release the radix tree mapping Redis Cluster keys to slots asynchronously. */
void freeSlotsToKeysMapAsync(rax *rt)
{
    atomicIncr(lazyfree_objects, rt->numele);
    bioCreateBackgroundJob(BIO_LAZY_FREE, NULL, NULL, rt);
}

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
void lazyfreeFreeObjectFromBioThread(robj *o)
{
    decrRefCount(o);
    atomicDecr(lazyfree_objects, 1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substituted with a fresh one in the main thread
 * when the database was logically deleted. */
void lazyfreeFreeDatabaseFromBioThread(dict *ht1, dict *ht2)
{
    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    dictRelease(ht2);
    atomicDecr(lazyfree_objects, numkeys);
}

/* Release the radix tree mapping Redis Cluster keys to slots in the
 * lazyfree thread.
 * rax 是一个基数树的数据结构
 */
void lazyfreeFreeSlotsMapFromBioThread(rax *rt)
{
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects, len);
}
