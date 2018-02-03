#pragma once
#include "xSds.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xLog.h"
#include "xThreadPool.h"

class xRedisAsyncContext;
typedef void (redisCallbackFn)(const xRedisAsyncContextPtr &ac, void*, void*);
typedef struct redisReply :noncopyable
{
    int32_t type;
    long long integer;
    int32_t len;
    char *str;
    size_t elements;
    struct redisReply **element;
} redisReply;


typedef struct redisReadTask:noncopyable
{
    int32_t type;
    int32_t elements;
    int32_t idx;
    void *obj;
    struct redisReadTask *parent;
    void *privdata;
} redisReadTask;


redisReply * createReplyObject(int32_t type);
void * createString(const redisReadTask * task, const char * str, size_t len);
void * createArray(const redisReadTask * task, int32_t elements);
void * createInteger(const redisReadTask * task, long long value);
void * createNil(const redisReadTask * task);
void freeReply(void *reply);


typedef struct redisReplyObjectFunctions:noncopyable
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
	std::function<void *(const redisReadTask*, int32_t)> createArrayFuc;
	std::function<void *(const redisReadTask*, long long)> createIntegerFuc;
	std::function<void *(const redisReadTask*)> createNilFuc;
	std::function<void (void*)> freeObjectFuc;
} redisFunc;


class xRedisReader:noncopyable
{
public:
	xRedisReader();

	int32_t redisReaderGetReply(void * *reply);
	void redisReaderSetError(int32_t type, const char *str);
	void redisReaderSetErrorProtocolByte(char byte);
	void redisReaderSetErrorOOM();
	void moveToNextTask();
	int32_t processLineItem();
	int32_t processBulkItem();
	int32_t processMultiBulkItem();
	int32_t processItem();

	long long readLongLong(const char * s);
	const char * readBytes(uint32_t bytes);
	const char * readLine(int32_t * _len);

	int32_t ridx;
	void *reply;
	void *privdata;
	int32_t err;
	char errstr[128];
	size_t pos;
	xBuffer  *buf;
	redisReadTask rstack[9];
	std::shared_ptr<redisFunc> fn;
};


typedef struct redisCallback
{
    redisCallbackFn *fn;
    void *privdata;
} redisCallback;

typedef std::list<redisCallback> RedisCallbackList;
typedef struct redisClusterCallback
{
	redisClusterCallback()
	{
		data = nullptr;
	}
	char * data;
	int32_t  len;
	redisCallback cb;
};

typedef std::list<redisClusterCallback> RedisClusterCallbackList;
class xRedisContext: noncopyable
{
public:
	xRedisContext();
	~xRedisContext();
	void clear();
	int32_t redisvAppendCommand(const char * format, va_list ap);
	int32_t __redisAppendCommand(const char * cmd, size_t len);
	void * redisCommand(const char * format, ...);
	void * redisvCommand(const char * format, va_list ap);
	void * redisCommandArgv(int32_t argc, const char * *argv, const size_t * argvlen);
	int32_t redisAppendFormattedCommand(const char *cmd, size_t len);
	int32_t redisAppendCommandArgv(int32_t argc, const char * *argv, const size_t * argvlen);
	void redisSetError(int32_t type, const char * str);
	void *redisBlockForReply();
	int32_t redisContextWaitReady(long msec);
	int32_t redisCheckSocketError();
	int32_t redisBufferRead();
	int32_t redisBufferWrite(int32_t * done);
	int32_t redisGetReply(void * *reply);
	int32_t redisGetReplyFromReader(void * *reply);
	int32_t redisContextConnectTcp(const char *addr, int32_t port, const struct timeval *timeout);
	int32_t redisAppendCommand(const char *format, ...);

	int32_t err;
	char errstr[128];
	int32_t fd;
	int32_t flags;
	const char *addr;
	int32_t port;
	xRedisReaderPtr reader;
	xBufferPtr sender;
};


class xRedisAsyncContext: noncopyable
{
public:
	xRedisAsyncContext();
	~xRedisAsyncContext();
	int32_t __redisAsyncCommand(redisCallbackFn *fn, void *privdata, char *cmd, size_t len);
	int32_t redisvAsyncCommand(redisCallbackFn *fn, void *privdata, const char *format, va_list ap);
	int32_t redisAsyncCommand(redisCallbackFn *fn, void *privdata, const char *format, ...);
	int32_t redisFormatSdsCommandArgv(sds *target, int32_t argc, const char ** argv, const size_t *argvlen);

	int32_t err;
	char *errstr;
	void *data;
	xRedisContextPtr c;
	xTcpconnectionPtr conn;
	RedisClusterCallbackList clus;
	std::mutex hiMutex;

	struct
	{
		RedisCallbackList invalid;
		std::unordered_map<rObj *,redisCallback> channels;
		std::unordered_map<rObj *,redisCallback> patterns;
	}sub;
};

class xHiredis : noncopyable
{
public:
	xHiredis(xEventLoop *loop);
	xHiredis(xEventLoop *loop,bool clusterMode);
	void clusterAskConnCallBack(const xTcpconnectionPtr& conn,void *data);
	void clusterMoveConnCallBack(const xTcpconnectionPtr& conn,void *data);
	void clusterErrorConnCallBack(void *data);
	void redisReadCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void eraseTcpMap(int32_t data);
	void eraseRedisMap(int32_t sockfd);
	void insertRedisMap(int32_t sockfd, xRedisAsyncContextPtr ac);
	void insertTcpMap(int32_t data,xTcpClientPtr tc);

	xThreadPool & getPool() { return pool; }
	void setCount(){ count ++; }
	int32_t getCount(){ return count; }
	std::mutex &getMutex(){ return rtx; }
	std::unordered_map<int32_t,xRedisAsyncContextPtr> & getRedisMap() { return redisMaps; }

private:
	std::unordered_map<int32_t,xTcpClientPtr> tcpClientMaps;
	std::unordered_map<int32_t,xRedisAsyncContextPtr> redisMaps;
	std::unordered_map<int32_t,redisClusterCallback> clusterMaps;

	xThreadPool pool;
	bool clusterMode;
	int32_t count;
	std::mutex rtx;
};

static int32_t redisFormatCommand(char **target, const char *format, ...);
static int32_t redisFormatCommandArgv(char * *target, int32_t argc, const char * *argv, const size_t * argvlen);
static int32_t redisvFormatCommand(char * *target, const char * format, va_list ap);

xRedisContextPtr redisConnectWithTimeout(const char *ip, int32_t port, const struct timeval tv);
xRedisContextPtr redisConnect(const char *ip, int32_t port);

