#pragma once
#include "all.h"
#include "xSds.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xLog.h"
#include "xThreadPool.h"

class xRedisAsyncContext; /* need forward declaration of redisAsyncContext */
typedef void (redisCallbackFn)(const xRedisAsyncContextPtr &ac, void*, void*);

/* This is the reply object returned by redisCommand() */
typedef struct redisReply {
    int type; /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    int len; /* Length of string */
    char *str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    struct redisReply **element; /* elements vector for REDIS_REPLY_ARRAY */
} redisReply;


typedef struct redisReadTask {
    int type;
    int elements; /* number of elements in multibulk container */
    int idx; /* index in parent (array) object */
    void *obj; /* holds user-generated value for a read task */
    struct redisReadTask *parent; /* parent task */
    void *privdata; /* user-settable arbitrary field */
} redisReadTask;



typedef struct redisReplyObjectFunctions
{
	void *(*createString)(const redisReadTask*, const char*, size_t);
	void *(*createArray)(const redisReadTask*, int);
	void *(*createInteger)(const redisReadTask*, long long);
	void *(*createNil)(const redisReadTask*);
	void (*freeObject)(void*);
} redisReplyObjectFunctions;


/* State for the protocol parser */
class xRedisReader
{
public:
	xRedisReader();
    int err; /* Error flags, 0 when there is no error */
    char errstr[128]; /* String representation of error when applicable */
    size_t pos; /* Buffer cursor */
    xBuffer *buf;
    redisReadTask rstack[9];
    int ridx; /* Index of current read task */
    void *reply; /* Temporary reply pointer */
    redisReplyObjectFunctions *fn;
    void *privdata;
};


typedef struct redisCallback
{
    redisCallbackFn *fn;
    void *privdata;
} redisCallback;

typedef std::list<redisCallback> RedisCallbackList;

class xRedisContext
{
public:
	xRedisContext():reader(new xRedisReader())
	{
		flags			&= ~REDIS_BLOCK;
		err				= 0;
		errstr[0]		= '\0';
	}
	int err; /* Error flags, 0 when there is no error */
	char errstr[128]; /* String representation of error when applicable */
	int fd;
	int flags;
	xBuffer sender;
	xRedisReaderPtr reader; /* Protocol reader */
};

class xRedisAsyncContext
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

class xHiredis: boost::noncopyable
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


class xClient
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






