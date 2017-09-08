#pragma once
#include "all.h"
#include "xSds.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xLog.h"
#include "xThreadPool.h"

class xRedisAsyncContext;
typedef void (redisCallbackFn)(const xRedisAsyncContextPtr &ac, void*, void*);


typedef struct redisReply 
{
    int type;
    long long integer;
    int len;
    char *str;
    size_t elements;
    struct redisReply **element;
} redisReply;


typedef struct redisReadTask {
    int type;
    int elements;
    int idx;
    void *obj;
    struct redisReadTask *parent;
    void *privdata;
} redisReadTask;


redisReply * createReplyObject(int type);
void * createString(const redisReadTask * task, const char * str, size_t len);
void * createArray(const redisReadTask * task, int elements);
void * createInteger(const redisReadTask * task, long long value);
void * createNil(const redisReadTask * task);
void freeReply(void *reply);


typedef struct redisReplyObjectFunctions
{
	redisReplyObjectFunctions()
	{
		createStringFuc = createString;
	   	createArrayFuc = createArray;
		createIntegerFuc = createInteger;
		createNilFuc = createNil;
		freeObjectFuc = freeReply;
	}
	
	std::function<void *(const redisReadTask *,const char *,size_t)> createStringFuc;
	std::function<void *(const redisReadTask*, int)> createArrayFuc;
	std::function<void *(const redisReadTask*, long long)> createIntegerFuc;
	std::function<void *(const redisReadTask*)> createNilFuc;
	std::function<void (void*)> freeObjectFuc;
} redisReplyObjectFunctions;



class xRedisReader
{
public:
	xRedisReader()
	{
		pos = 0;
		err = 0;
		errstr[0] = '\0';
		fn = std::shared_ptr<redisReplyObjectFunctions>(new redisReplyObjectFunctions());
		ridx 	= -1;
	}
	
	int err;
	char errstr[128];
	size_t pos;
	xBuffer *buf;
	redisReadTask rstack[9];
	int ridx;
	void *reply;
	std::shared_ptr<redisReplyObjectFunctions> fn;
    	void *privdata;
};


typedef struct redisCallback
{
    redisCallbackFn *fn;
    void *privdata;
} redisCallback;

typedef std::list<redisCallback> RedisCallbackList;

class xRedisContext: noncopyable
{
public:
	xRedisContext():reader(new xRedisReader())
	{
		flags	&= ~REDIS_BLOCK;
		err = 0;
		errstr[0] = '\0';
	}
	int err;
	char errstr[128];
	int fd;
	int flags;
	xBuffer sender;
	xRedisReaderPtr reader;
};

class xRedisAsyncContext: noncopyable
{
public:
	xRedisAsyncContext():c(new (xRedisContext))
	{
		err = 0;
		errstr = nullptr;
		data = nullptr;
		c->flags &= ~REDIS_CONNECTED;
	}
	int err;
	char *errstr;
	void *data;
	xRedisContextPtr c;
	xTcpconnectionPtr conn;
	RedisCallbackList replies;

	struct
	{
		RedisCallbackList invalid;
		std::map<rObj *,redisCallback> channels;
		std::map<rObj *,redisCallback> patterns;
	}sub;
};



int __redisAsyncCommand(const xRedisAsyncContextPtr &ac,redisCallbackFn *fn, void *privdata, char *cmd, size_t len);
int redisvFormatCommand(char * *target, const char * format, va_list ap);
int redisvAsyncCommand(const xRedisAsyncContextPtr &ac, redisCallbackFn *fn, void *privdata, const char *format, va_list ap);
int redisAsyncCommand(const xRedisAsyncContextPtr &ac, redisCallbackFn *fn, void *privdata, const char *format, ...);


int redisFormatCommand(char **target, const char *format, ...);
int redisFormatCommandArgv(char * *target, int argc, const char * *argv, const size_t * argvlen);
int redisAppendCommandArgv(const xRedisContextPtr  & c, int argc, const char * *argv, const size_t * argvlen);
int redisFormatSdsCommandArgv(sds *target, int argc, const char ** argv, const size_t *argvlen);

int redisvAppendCommand(const xRedisContextPtr  & c, const char * format, va_list ap);
int __redisAppendCommand(const xRedisContextPtr & c, const char * cmd, size_t len);


void * redisCommand(const xRedisContextPtr  & c, const char * format, ...);
void * redisvCommand(const xRedisContextPtr  & c, const char * format, va_list ap);
void * redisCommandArgv(const xRedisContextPtr  & c, int argc, const char * *argv, const size_t * argvlen);

int redisBufferRead(const xRedisContextPtr & c);
int redisBufferWrite(const xRedisContextPtr & c, int * done);
int redisGetReply(const xRedisContextPtr & c, void * *reply);
int redisGetReplyFromReader(const xRedisContextPtr & c, void * *reply);
int redisReaderGetReply(const xRedisReaderPtr & r,void * *reply);
void __redisSetError(const xRedisContextPtr & c, int type, const char * str);






