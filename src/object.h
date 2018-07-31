#pragma once
#include "all.h"
#include "zmalloc.h"
#include "sds.h"
#include "buffer.h"
#include "log.h"
#include "util.h"
#include "callback.h"

class RedisObject
{
public:
	RedisObject();
	~RedisObject();

	void calHash();
	bool operator <(const RedisObjectPtr &r) const;
	unsigned type :4;
	unsigned encoding :4;
	size_t hash;
	sds ptr;
};

struct Hash
{
	size_t operator()(const RedisObjectPtr &x) const
	{
		return x->hash;
	}
};

struct Equal
{
	bool operator()(const RedisObjectPtr &x,const RedisObjectPtr &y) const
	{
		return ((sdslen(x->ptr) == sdslen(y->ptr)) &&
				(memcmp(x->ptr,y->ptr,sdslen(y->ptr))== 0));
	}
};

struct SharedObjectsStruct
{
	RedisObjectPtr crlf,ok,err,emptybulk,czero,
	cone,cnegone,pping,ping,pong,ppong,space,
	colon,nullbulk,nullmultibulk,queued,rIp,rPort,
	emptymultibulk,wrongtypeerr,nokeyerr,syntaxerr,sameobjecterr,
	outofrangeerr,noscripterr,loadingerr,slowscripterr,bgsaveerr,
	masterdownerr,roslaveerr,execaborterr,noautherr,noreplicaserr,
	busykeyerr,oomerr,plus,messagebulk,pmessagebulk,subscribebulk,
	unsubscribebulk,psubscribebulk,punsubscribebulk,del,rpop,lpop,
	lpush,rpush,emptyscan,minstring,maxstring,sync,psync,set,get,flushdb,
	dbsize,asking,hset,hget,hgetall,save,slaveof,command,config,auth,
	info,echo,client,hkeys,hlen,keys,bgsave,memory,cluster,migrate,debug,
	ttl,lrange,llen,sadd,scard,addsync,setslot,node,clusterconnect,delsync,
	zadd,zrange,zrevrange,zcard,dump,restore,incr,decr,monitor,
	integers[REDIS_SHARED_INTEGERS],
	mbulkhdr[REDIS_SHARED_BULKHDR_LEN],
	bulkhdr[REDIS_SHARED_BULKHDR_LEN];
};

extern struct SharedObjectsStruct shared;
void createSharedObjects();

RedisObjectPtr createRawStringObject(char *ptr,size_t len);
RedisObjectPtr createRawStringObject(int32_t type,char *ptr,size_t len);
RedisObjectPtr createObject(int32_t type,char *ptr);
RedisObjectPtr createStringObject(char *ptr,size_t len);
RedisObjectPtr createStringObjectFromLongLong(int64_t value);

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */
void addReply(Buffer *buffer,const RedisObjectPtr &obj);
void addReplyBulkSds(Buffer *buffer,sds s);
void addReplyMultiBulkLen(Buffer *buffer,int32_t length);
void addReply(Buffer *buffer,const RedisObjectPtr &obj);
void addReplyString(Buffer *buffer,const char *s,size_t len);
void addReplyError(Buffer *buffer,const char *str);
void addReplyErrorLength(Buffer *buffer,const char *s,size_t len);
void addReplyLongLongWithPrefix(Buffer *buffer,int64_t ll,char prefix);
void addReplyBulkLen(Buffer *buffer,const RedisObjectPtr &obj);
void addReplyBulk(Buffer *buffer,const RedisObjectPtr &obj);
void addReplyErrorFormat(Buffer *buffer,const char *fmt, ...);
void addReplyBulkCBuffer(Buffer *buffer,const char *p,size_t len);
void addReplyLongLong(Buffer *buffer,size_t len);
void addReplySds(Buffer *buffer,sds s);
void addReplyStatus(Buffer *buffer,char *status);
void addReplyStatusLength(Buffer *buffer,char *s,size_t len);
void addReplyBulkCString(Buffer *buffer,const char *s);
void addReplyDouble(Buffer *buffer,double d);
void prePendReplyLongLongWithPrefix(Buffer *buffer,int32_t length);

int32_t getLongLongFromObject(const RedisObjectPtr &o,int64_t *target);
int32_t getLongFromObjectOrReply(Buffer *buffer,
			const RedisObjectPtr &o,int32_t *target,const char *msg);
int32_t getLongLongFromObjectOrReply(Buffer *buffer,
			const RedisObjectPtr &o,int64_t *target,const char *msg);
int32_t getDoubleFromObject(const RedisObjectPtr &o,double *target);
int32_t getDoubleFromObjectOrReply(Buffer *buffer,
			const RedisObjectPtr &o,double *target,const char *msg);






