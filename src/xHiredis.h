#pragma once
#include "xSds.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xLog.h"
#include "xThreadPool.h"

class xRedisAsyncContext;
typedef struct redisReply :noncopyable
{
    int32_t type;
    long long integer;
    int32_t len;
    char *str;
    size_t elements;
    struct redisReply **element;
} redisReply;


typedef void (redisCallbackFn)(const xRedisAsyncContextPtr &ac, redisReply*, const std::any& );

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


typedef struct redisReplyObjectFunctions : noncopyable
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


class xRedisReader : noncopyable
{
public:
	xRedisReader();
	xRedisReader(xBuffer & recvBuff);
	
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

	char errstr[128];
	void *reply;
	void *privdata;

	int32_t ridx;
	int32_t err;
	size_t pos;
	xBuffer  *buf;
	redisReadTask rstack[9];
	redisFunc fn;
};


typedef struct redisCallback
{
    redisCallbackFn *fn;
    std::any privdata;
} redisCallback;

typedef std::list<redisCallback> RedisCallbackList;
typedef struct redisAsyncCallback
{
	redisAsyncCallback()
	{
		data = nullptr;
	}
	char * data;
	int32_t  len;
	redisCallback cb;
};

typedef std::list<redisAsyncCallback> RedisAsyncCallbackList;
class xRedisContext : noncopyable
{
public:
	xRedisContext();
	xRedisContext(xBuffer & recvBuff,int32_t sockfd);
	~xRedisContext();

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
	int32_t redisContextConnectTcp(const char *addr, int16_t port, const struct timeval *timeout);
	int32_t redisAppendCommand(const char *format, ...);

	void setBlock();
	void setConnected();
	void clear();

	char errstr[128];
	const char *ip;
	int32_t err;
	int32_t fd;
	int8_t flags;
	int16_t port;
	xRedisReaderPtr reader;
	xBuffer sender;
};


class xRedisAsyncContext : noncopyable
{
public:
	xRedisAsyncContext(xBuffer & recvBuff,xTcpconnectionPtr conn,int32_t sockfd);
	~xRedisAsyncContext();

	int32_t __redisAsyncCommand(redisCallbackFn *fn, const std::any& privdata, char *cmd, size_t len);
	int32_t redisvAsyncCommand(redisCallbackFn *fn, const std::any& privdata, const char *format, va_list ap);
	int32_t redisAsyncCommand(redisCallbackFn *fn, const std::any& privdata, const char *format, ...);
	int32_t redisFormatSdsCommandArgv(sds *target, int32_t argc, const char ** argv, const size_t *argvlen);

	int32_t err;
	char *errstr;
	void *data;
	xRedisContextPtr c;
	xTcpconnectionPtr conn;
	RedisAsyncCallbackList asynCb;
	std::mutex mtx;

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

	void clusterAskConnCallBack(const xTcpconnectionPtr& conn);
	void clusterMoveConnCallBack(const xTcpconnectionPtr& conn);
	void clusterErrorConnCallBack(const std::any &context);
	void redisReadCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf);
	void eraseTcpMap(int32_t context);
	void eraseRedisMap(int32_t sockfd);
	void insertRedisMap(int32_t sockfd, xRedisAsyncContextPtr ac);
	void insertTcpMap(int32_t data,xTcpClientPtr tc);

	xThreadPool & getPool() { return pool; }
	void setCount() { count ++; }
	int32_t getCount() { return count; }
	std::mutex &getMutex() { return rtx; }
	std::unordered_map<int32_t,xRedisAsyncContextPtr> &getRedisMap() { return redisMaps; }
	std::unordered_map<int32_t,xTcpClientPtr> &getClientMap() { return tcpClientMaps; }

private:
	std::unordered_map<int32_t,xTcpClientPtr> tcpClientMaps;
	std::unordered_map<int32_t,xRedisAsyncContextPtr> redisMaps;
	std::unordered_map<int32_t,redisAsyncCallback> clusterMaps;

	xThreadPool pool;
	bool clusterMode;
	int32_t count;
	std::mutex rtx;
};

int32_t redisFormatCommand(char **target, const char *format, ...);
int32_t redisFormatCommandArgv(char * *target, int32_t argc, const char * *argv, const size_t * argvlen);
int32_t redisvFormatCommand(char * *target, const char * format, va_list ap);

xRedisContextPtr redisConnectWithTimeout(const char *ip, int16_t port, const struct timeval tv);
xRedisContextPtr redisConnect(const char *ip, int16_t port);

