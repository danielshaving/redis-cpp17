#pragma once

#include "all.h"
#include "xZmalloc.h"
#include "xSds.h"
#include "xBuffer.h"
#include "xLog.h"

unsigned int dictGenHashFunction(const void *key, int len) ;

#define sdsEncodedObject(objptr) (objptr->encoding == REDIS_ENCODING_RAW || objptr->encoding == REDIS_ENCODING_EMBSTR)
typedef struct redisObject 
{		
	void calHash()
	{
		hash =boost::hash_range(ptr,sdsllen(ptr) + ptr);
	}
	unsigned type:4;
	unsigned encoding:4;
	size_t hash;
	const char *ptr;
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
		 return ((sdsllen(x->ptr) == sdsllen(y->ptr)) &&
            (memcmp(x->ptr, y->ptr, sdsllen(y->ptr))== 0));
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
    rObj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk, *queued,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
    *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *rpop, *lpop,
    *lpush, *emptyscan, *minstring, *maxstring,*sync,
    *select[REDIS_SHARED_SELECT_CMDS],
    *integers[REDIS_SHARED_INTEGERS],
    *mbulkhdr[REDIS_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[REDIS_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
};

extern sharedObjectsStruct shared;

int ll2string(char *s, size_t len, long long value);
int string2ll(const char * s,size_t slen, long long * value);
rObj *createRawStringObject(const char *ptr, size_t len);
rObj *createObject(int type, const void *ptr);
rObj *createStringObject(const char *ptr, size_t len);
rObj *createEmbeddedStringObject(const char *ptr, size_t len);
void createSharedObjects();
void destorySharedObjects();
rObj *createStringObjectFromLongLong(long long value);
int getLongLongFromObject(rObj *o, long long   *target);
int getLongFromObjectOrReply(xBuffer &sendBuf, rObj *o, long  *target, const char *msg);

void addReplyMultiBulkLen(xBuffer &sendBuf,long length);
void addReply(xBuffer &sendBuf,rObj *obj);
void addReplyString(xBuffer & sendBuf,const char *s, size_t len);
void addReplyError(xBuffer &sendBuf,const char *str);
void addReplyErrorLength(xBuffer &sendBuf,const char *s,size_t len);
void addReplyLongLongWithPrefix(xBuffer &sendBuf, long long ll, char prefix);
void addReplyBulkLen(xBuffer &sendBuf,rObj *obj);
void addReplyBulk(xBuffer &sendBuf,rObj *obj);
void addReplyErrorFormat(xBuffer &sendBuf,const char *fmt, ...);
void addReplyBulkCBuffer(xBuffer &sendBuf,const char *p, size_t len);
void addReplyLongLong(xBuffer &sendBuf,size_t len);
void addReplySds(xBuffer &sendBuf,sds s);

long long ustime(void);
long long mstime(void);

#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)




