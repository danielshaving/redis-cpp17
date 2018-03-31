#pragma once

#include "all.h"
#include "xZmalloc.h"
#include "xSds.h"
#include "xBuffer.h"
#include "xLog.h"
#include "xUtil.h"

#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1<<LRU_BITS)-1) /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000 /* LRU clock resolution in ms */


typedef struct redisObject
{		
	void calHash()
	{
		hash = dictGenHashFunction(ptr,sdslen(ptr));
	}

	bool operator <(const redisObject &r) const
	{
		int32_t cmp = memcmp(ptr,r.ptr,sdslen(ptr));
		if( cmp < 0)
		{
			return true;
		}
		else if(cmp == 0)
		{
			return memcmp(ptr,r.ptr,sdslen(ptr)) < 0;
		}
		else
		{
			return false;
		}
	}

	unsigned lru:LRU_BITS;    
   	int32_t refcount;
	unsigned type;
	unsigned encoding;
	size_t hash;
	char  *ptr;
} rObj;


struct Hash
{
	size_t operator()(const rObj *x) const
	{
		return x->hash;
	}
};

struct Equal
{
	bool operator()(const rObj *x,const rObj *y) const
	{
		 return ((sdslen(x->ptr) == sdslen(y->ptr)) &&
            (memcmp(x->ptr, y->ptr, sdslen(y->ptr))== 0));
	}
};


class xRedis;
class xObjects : noncopyable
{
public:
	xObjects(xRedis *redis);
	~xObjects();

	void createSharedObjects();
	void destorySharedObjects();
	void freeStringObject(rObj *o);
	void freeListObject(rObj *o);
	void freeSetObject(rObj *o);
	void freeHashObject(rObj *o);
	void freeZsetObject(rObj *o);
	void decrRefCount(rObj *o);

	void addReplyBulkSds(xBuffer &buffer,sds s);
	void addReplyMultiBulkLen(xBuffer &buffer,int32_t length);
	void addReply(xBuffer &buffer,rObj *obj);
	void addReplyString(xBuffer &buffer,const char *s,size_t len);
	void addReplyError(xBuffer &buffer,const char *str);
	void addReplyErrorLength(xBuffer &buffer,const char *s,size_t len);
	void addReplyLongLongWithPrefix(xBuffer &buffer,int64_t ll, char prefix);
	void addReplyBulkLen(xBuffer &buffer,rObj *obj);
	void addReplyBulk(xBuffer &buffer,rObj *obj);
	void addReplyErrorFormat(xBuffer &buffer,const char *fmt, ...);
	void addReplyBulkCBuffer(xBuffer &buffer,const char *p,size_t len);
	void addReplyLongLong(xBuffer &buffer,size_t len);
	void addReplySds(xBuffer &buffer,sds s);
	void addReplyStatus(xBuffer &buffer,char *status);
	void addReplyStatusLength(xBuffer &buffer,char *s,size_t len);
	void addReplyBulkCString(xBuffer &buffer,const char *s);
	void addReplyDouble(xBuffer &buffer,double d);
	void prePendReplyLongLongWithPrefix(xBuffer &buffer,int32_t length);

	rObj *createRawStringObject(char *ptr,size_t len);
	rObj *createObject(int32_t type,void *ptr);
	rObj *createStringObject(char *ptr,size_t len);
	rObj *createEmbeddedStringObject(char *ptr,size_t len);
	rObj *createStringObjectFromLongLong(int64_t value);


	int32_t getLongLongFromObject(rObj *o,int64_t *target);
	int32_t getLongFromObjectOrReply(xBuffer &buffer,rObj *o,int32_t *target,const char *msg);
	int32_t getLongLongFromObjectOrReply(xBuffer &buffer,rObj *o,int64_t *target,const char *msg);
	int32_t getDoubleFromObject(const rObj *o,double *target);
	int32_t getDoubleFromObjectOrReply(xBuffer &buffer,rObj *o,double *target,const char *msg);

	rObj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pping,*ping,*pong,*ppong, *space,
	*colon, *nullbulk, *nullmultibulk, *queued,
	*emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
	*outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
	*masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
	*busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
	*unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *rpop, *lpop,
  	*lpush, *rpush,*emptyscan, *minstring, *maxstring,*sync,*set,*get,*flushdb,
	*dbsize,*asking,
	*hset,*hget,*hgetall,*save,*slaveof,*command,*config,*auth,
	*info,*echo,*client,*hkeys,*hlen,*keys,*bgsave,*memory,*cluster,*migrate,*debug,
	*ttl,*lrange,*llen,*sadd,*scard,*psync,*addsync,*setslot,*node,*connect,*delsync,
	*zadd,*zrange,*zrevrange,*zcard,*dump,*restore,
	*PING,*DEL, *RPOP, *LPOP,
	*LPUSH, *RPUSH,*SYNC,*SET,*GET,*FLUSHDB,*DBSIZE,*ASKING,
	*HSET,*HGET,*HGETALL,*SAVE,*SLAVEOF,*COMMAND,*CONFIG,*AUTH,
	*INFO,*ECHO,*CLIENT,*HKEYS,*HLEN,*KEYS,*BGSAVE,*MEMORY,*CLUSTER,*MIGRATE,*DEBUG,
	*TTL,*LRANGE,*LLEN,*SADD,*SCARD,*PSYNC,*ADDSYNC,*SETSLOT,*NODE,*CONNECT,*DELSYNC,
	*ZADD,*ZRANGE,*ZREVRANGE,*ZCARD,*DUMP,*RESTORE,
	*integers[REDIS_SHARED_INTEGERS],
	*mbulkhdr[REDIS_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
	*bulkhdr[REDIS_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */

	rObj *rIp;
	rObj *rPort;

private:
	xRedis *redis;

};



