#pragma once

#include "all.h"
#include "xZmalloc.h"
#include "xSds.h"
#include "xBuffer.h"
#include "xLog.h"
#include "xUtil.h"

unsigned int dictGenHashFunction(const void *key, int len) ;
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len);
typedef struct redisObject
{		
	void calHash()
	{
		hash = dictGenHashFunction(ptr,sdslen(ptr));
	}

	bool operator <(const redisObject & r) const
	{
		int cmp = memcmp(ptr,r.ptr,sdslen(ptr));
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
	
	unsigned type;
	unsigned encoding;
	size_t hash;
	char *ptr;
} rObj;


struct Hash
{
	size_t operator()(const rObj * x) const
	{
		return x->hash;
	}
};

struct Equal
{
	bool operator()(const rObj * x, const rObj * y) const
	{
		 return ((sdslen(x->ptr) == sdslen(y->ptr)) &&
            (memcmp(x->ptr, y->ptr, sdslen(y->ptr))== 0));
	}

};


class xRedis;
class xObjects
{
public:
	xObjects(xRedis * redis);
	~xObjects();

	void createSharedObjects();
	void destorySharedObjects();
	void freeStringObject(rObj *o) ;
	void decrRefCount(rObj *o) ;

	void addReplyBulkSds(xBuffer &sendBuf, sds s);
	void addReplyMultiBulkLen(xBuffer &sendBuf,long length);
	void addReply(xBuffer &sendBuf,rObj *obj);
	void addReplyString(xBuffer & sendBuf,const char *s, size_t len);
	void addReplyError(xBuffer &sendBuf,const char *str);
	void addReplyErrorLength(xBuffer &sendBuf,const char *s,size_t len);
	void addReplyLongLongWithPrefix(xBuffer &sendBuf, long long ll, char prefix);
	void addReplyBulkLen(xBuffer &sendBuf,rObj *obj);
	void addReplyBulk(xBuffer &sendBuf,rObj *obj);
	void addReplyErrorFormat(xBuffer &sendBuf,const char *fmt, ...);
	void addReplyBulkCBuffer(xBuffer &sendBuf, const char  *p, size_t len);
	void addReplyLongLong(xBuffer &sendBuf,size_t len);
	void addReplySds(xBuffer &sendBuf,sds s);
	void addReplyStatus(xBuffer &sendBuf, char *status);
	void addReplyStatusLength(xBuffer &sendBuf, char *s, size_t len);
	void addReplyBulkCString(xBuffer & sendBuf, const char *s);
	void addReplyDouble(xBuffer & sendBuf, double d);
	void prePendReplyLongLongWithPrefix(xBuffer & sendBuf,long length);

	rObj *createRawStringObject(char *ptr, size_t len);
	rObj *createObject(int type, void *ptr);
	rObj *createStringObject(char *ptr, size_t len);
	rObj *createEmbeddedStringObject(char *ptr, size_t len);
	rObj *createStringObjectFromLongLong(long long value);


	int getLongLongFromObject(rObj *o, long long   *target);
	int getLongFromObjectOrReply(xBuffer &sendBuf, rObj *o, long  *target, const char *msg);
	int getLongLongFromObjectOrReply(xBuffer &sendBuf,rObj *o, long long *target, const char *msg);
	int getDoubleFromObject(const rObj *o, double *target);
	int getDoubleFromObjectOrReply(xBuffer  &sendBuf, rObj *o, double *target, const char *msg);

	rObj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pping,*ping,*pong,*ppong, *space,
	*colon, *nullbulk, *nullmultibulk, *queued,
	*emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
	*outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
	*masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
	*busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
	*unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *rpop, *lpop,
  	*lpush, *rpush,*emptyscan, *minstring, *maxstring,*sync,*set,*get,*flushdb,*dbsize,
	*hset,*hget,*hgetall,*save,*slaveof,*command,*config,*auth,
	*info,*echo,*client,*hkeys,*hlen,*keys,*bgsave,*memory,*cluster,*migrate,*debug,
	*ttl,*lrange,*llen,*sadd,*scard,*psync,*addsync,*setslot,*node,*connect,*delsync,
	*zadd,*zrange,*zrevrange,*zcard,
	*PING,*DEL, *RPOP, *LPOP,
	*LPUSH, *RPUSH,*SYNC,*SET,*GET,*FLUSHDB,*DBSIZE,
	*HSET,*HGET,*HGETALL,*SAVE,*SLAVEOF,*COMMAND,*CONFIG,*AUTH,
	*INFO,*ECHO,*CLIENT,*HKEYS,*HLEN,*KEYS,*BGSAVE,*MEMORY,*CLUSTER,*MIGRATE,*DEBUG,
	*TTL,*LRANGE,*LLEN,*SADD,*SCARD,*PSYNC,*ADDSYNC,*SETSLOT,*NODE,*CONNECT,*DELSYNC,
	*ZADD,*ZRANGE,*ZREVRANGE,*ZCARD,
	*integers[REDIS_SHARED_INTEGERS],
	*mbulkhdr[REDIS_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
	*bulkhdr[REDIS_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */

	rObj * rIp;
	rObj * rPort;

private:
	xRedis * redis;

};



