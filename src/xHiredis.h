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


static redisReply * createReplyObject(int type);
static void * createString(const redisReadTask * task, const char * str, size_t len);
static void * createArray(const redisReadTask * task, int elements);
static void * createInteger(const redisReadTask * task, long long value);
static void * createNil(const redisReadTask * task);
static void freeReply(void *reply);


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
	xRedisReader();
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
		flags			&= ~REDIS_BLOCK;
		err				= 0;
		errstr[0]		= '\0';
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
	}
	int err;
	char *errstr;
	void *data;
	xRedisContextPtr c;
	xTcpconnectionPtr conn;
	RedisCallbackList replies;
};

class xClient;

class xHiredis:noncopyable
{
public:
	xHiredis(xEventLoop *loop,xClient * owner);
	xHiredis(xClient * owner);
	~xHiredis(){}
	void start();

	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void connSyncCallBack(const xTcpconnectionPtr& conn,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	void connErrorCallBack();

private:
	xEventLoop *loop;
	xEventLoop sloop;
	std::map<int32_t,xRedisAsyncContextPtr> redisAsyncs;
	std::map<int32_t,xRedisContextPtr> redisSyncs;
	xClient * owner;
	xTcpClient client;
	mutable std::mutex mutex;
};


class xClient:noncopyable
{
public:
	xClient(xEventLoop *loop,const char *ip,uint16_t port,int blockSize,int sessionCount,int threadCount)
	:loop(loop),
	ip(ip),
	port(port),
	sessionCount(sessionCount),
	threadPool(loop)
{

	if (threadCount > 1)
	{
	  	threadPool.setThreadNum(threadCount);
	}

	threadPool.start();
	for (int i = 0; i < blockSize; ++i)
	{
		message.push_back(static_cast<char>(i % 128));
	}

	for(int i = 0; i < sessionCount ; i ++)
	{
		std::shared_ptr<xHiredis>   redis = std::shared_ptr<xHiredis>(new xHiredis(threadPool.getNextLoop(),this));
		redis->start();
		redisVectors.push_back(redis);
	}
}

	xClient(const char *ip,uint16_t port)
	:ip(ip),
	port(port),
	threadPool(loop)
{
	std::shared_ptr<xHiredis>   redis = std::shared_ptr<xHiredis>(new xHiredis(this));
	redis->start();
	redisVectors.push_back(redis);
}



~xClient()
{
	redisVectors.clear();
}

public:
	xEventLoop *loop;
	const char *ip;
	uint16_t port;
	int sessionCount;
	xThreadPool threadPool;
	std::string message;
	std::vector<std::shared_ptr<xHiredis>>   redisVectors;
};


int __redisAsyncCommand(const xRedisAsyncContextPtr &ac,redisCallbackFn *fn, void *privdata, char *cmd, size_t len);
int redisvFormatCommand(char * *target, const char * format, va_list ap);
int redisvAsyncCommand(const xRedisAsyncContextPtr &ac, redisCallbackFn *fn, void *privdata, const char *format, va_list ap);
int redisAsyncCommand(const xRedisAsyncContextPtr &ac, redisCallbackFn *fn, void *privdata, const char *format, ...);


int redisFormatCommandArgv(char * *target, int argc, const char * *argv, const size_t * argvlen);
int redisAppendCommandArgv(const xRedisContextPtr  & c, int argc, const char * *argv, const size_t * argvlen);

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






