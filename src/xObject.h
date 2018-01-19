#pragma once

#include "all.h"
#include "xZmalloc.h"
#include "xSds.h"
#include "xBuffer.h"
#include "xLog.h"


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
	unsigned type:4;
	unsigned encoding:4;
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


struct EEqual
{
	bool operator()(const rObj * x, const rObj * y) const
	{
		 return  (strcasecmp(x->ptr, y->ptr) == 0);
	}

};

struct sharedObjectsStruct
{
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
	*zadd,*zrange,
	*PING,*DEL, *RPOP, *LPOP,
	*LPUSH, *RPUSH,*SYNC,*SET,*GET,*FLUSHDB,*DBSIZE,
	*HSET,*HGET,*HGETALL,*SAVE,*SLAVEOF,*COMMAND,*CONFIG,*AUTH,
	*INFO,*ECHO,*CLIENT,*HKEYS,*HLEN,*KEYS,*BGSAVE,*MEMORY,*CLUSTER,*MIGRATE,*DEBUG,
	*TTL,*LRANGE,*LLEN,*SADD,*SCARD,*PSYNC,*ADDSYNC,*SETSLOT,*NODE,*CONNECT,*DELSYNC,
	*ZADD,*ZRANGE,
	*integers[REDIS_SHARED_INTEGERS],
	*mbulkhdr[REDIS_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
	*bulkhdr[REDIS_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
};




extern sharedObjectsStruct shared;
int ll2string(char *s, size_t len, long long value);
int string2ll(const char * s,size_t slen, long long * value);
rObj *createRawStringObject(char *ptr, size_t len);
rObj *createObject(int type, void *ptr);
rObj *createStringObject(char *ptr, size_t len);
rObj *createEmbeddedStringObject(char *ptr, size_t len);
void createSharedObjects();
void destorySharedObjects();
void freeStringObject(rObj *o) ;
void decrRefCount(rObj *o) ;

rObj *createStringObjectFromLongLong(long long value);
int getLongLongFromObject(rObj *o, long long   *target);
int getLongFromObjectOrReply(xBuffer &sendBuf, rObj *o, long  *target, const char *msg);
int getLongLongFromObjectOrReply(xBuffer &sendBuf,rObj *o, long long *target, const char *msg);
int getDoubleFromObject(const rObj *o, double *target);
int getDoubleFromObjectOrReply(xBuffer  &sendBuf, rObj *o, double *target, const char *msg);


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

long long ustime(void);
long long mstime(void);
long long setime(void) ;


void memrev64(void *p);

#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)

void bytesToHuman(char *s, unsigned long long n);



