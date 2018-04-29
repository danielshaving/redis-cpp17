#pragma once
#include "xSds.h"
#include "xTcpConnection.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xLog.h"
#include "xThreadPool.h"

class xRedisAsyncContext;
struct redisReply : boost::noncopyable
{
    int32_t type;
    int64_t integer;
    int32_t len;
    size_t elements;
    char *str;
    struct redisReply **element;
};

typedef std::function<void(const RedisAsyncContextPtr &context,redisReply*,const std::any &)> RedisCallbackFn;
struct redisReadTask : boost::noncopyable
{
    int32_t type;
    int32_t elements;
    int32_t idx;
    std::any privdata;
    redisReply *obj;
    struct redisReadTask *parent;
};

redisReply *createReplyObject(int32_t type);
redisReply *createString(const redisReadTask *task,const char *str,size_t len);
redisReply *createArray(const redisReadTask *task,int32_t elements);
redisReply *createInteger(const redisReadTask *task,int64_t value);
redisReply *createNil(const redisReadTask *task);
void freeReply(redisReply *reply);

struct redisFunc : boost::noncopyable
{
	redisFunc()
	{
		createStringFuc = createString;
		createArrayFuc = createArray;
		createIntegerFuc = createInteger;
		createNilFuc = createNil;
		freeObjectFuc = freeReply;
	}
	
	std::function<redisReply*(const redisReadTask*,const char*,size_t)> createStringFuc;
	std::function<redisReply*(const redisReadTask*,int32_t)> createArrayFuc;
	std::function<redisReply*(const redisReadTask*,int64_t)> createIntegerFuc;
	std::function<redisReply*(const redisReadTask*)> createNilFuc;
	std::function<void (redisReply*)> freeObjectFuc;
};

class xRedisReader : boost::noncopyable
{
public:
	xRedisReader();
	xRedisReader(xBuffer *buffer);
	~xRedisReader();

	int32_t redisReaderGetReply(redisReply **reply);
	void redisReaderSetError(int32_t type,const char *str);
	void redisReaderSetErrorProtocolByte(char byte);
	void redisReaderSetErrorOOM();
	void moveToNextTask();
	int32_t processLineItem();
	int32_t processBulkItem();
	int32_t processMultiBulkItem();
	int32_t processItem();

	int64_t readLongLong(const char *s);
	const char *readBytes(uint32_t bytes);
	const char *readLine(int32_t *_len);

	char errstr[128];
	redisReadTask rstack[9];
	int32_t ridx;
	int32_t err;
	size_t pos;
	redisFunc fn;
	xBuffer buf;
	xBuffer *buffer;
	redisReply *reply;
	std::any privdata;
};

struct redisCallback
{
	RedisCallbackFn fn;
    std::any privdata;
};

typedef std::list<redisCallback> RedisCallbackList;
struct redisAsyncCallback
{
	redisAsyncCallback()
	:data(nullptr),
	 len(0)
	{

	}
	char *data;
	int32_t len;
	redisCallback cb;
};

typedef std::list<redisAsyncCallback> RedisAsyncCallbackList;
class xRedisContext : boost::noncopyable
{
public:
	xRedisContext();
	xRedisContext(xBuffer *buffer,int32_t sockfd);
	~xRedisContext();

	int32_t redisvAppendCommand(const char *format,va_list ap);
	void redisAppendCommand(const char *cmd,size_t len);
	redisReply *redisCommand(const char *format,...);
	redisReply *redisvCommand(const char *format,va_list ap);
	redisReply *redisCommandArgv(int32_t argc,const char **argv,const size_t *argvlen);
	void redisAppendFormattedCommand(const char *cmd,size_t len);
	int32_t redisAppendCommandArgv(int32_t argc,const char **argv,const size_t *argvlen);
	void redisSetError(int32_t type,const char *str);
	redisReply *redisBlockForReply();
	int32_t redisContextWaitReady(int32_t msec);
	int32_t redisCheckSocketError();
	int32_t redisBufferRead();
	int32_t redisBufferWrite(int32_t *done);
	int32_t redisGetReply(redisReply **reply);
	int32_t redisGetReplyFromReader(redisReply **reply);
	int32_t redisContextConnectTcp(const char *addr,int16_t port,const struct timeval *timeout);
	int32_t redisAppendCommand(const char *format, ...);

public:
	void clear();
	void setBlock();
	void setConnected();
	void setDisConnected();

	char errstr[128];
	const char *ip;
	int32_t err;
	int32_t fd;
	int8_t flags;
	int16_t p;

	xBuffer sender;
	RedisReaderPtr reader;
};

class xRedisAsyncContext : boost::noncopyable
{
public:
	xRedisAsyncContext(xBuffer *buffer,const TcpConnectionPtr &conn);
	~xRedisAsyncContext();

	void  __redisAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,char *cmd,size_t len);
	int redisvAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,const char *format,va_list ap);
	int redisAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,const char *format, ...);

	int32_t redisGetReply(redisReply **reply) { return context->redisGetReply(reply); }
	RedisContextPtr getRedisContext() { return context; }
	TcpConnectionPtr getServerConn() { return serverConn; }
	std::mutex &getMutex() { return mtx;}
	RedisAsyncCallbackList &getCb() { return asyncCb; }

private:
	int32_t err;
	char *errstr;
	std::any data;
	RedisContextPtr context;
	TcpConnectionPtr serverConn;
	RedisAsyncCallbackList asyncCb;
	std::mutex mtx;

	struct
	{
		RedisCallbackList invalid;
		std::unordered_map<rObj*,redisCallback> channels;
		std::unordered_map<rObj*,redisCallback> patterns;
	}sub;
};

class xHiredis : boost::noncopyable
{
public:
	xHiredis(xEventLoop *loop,bool clusterMode = false);
	~xHiredis();

	void clusterAskConnCallBack(const TcpConnectionPtr &conn);
	void clusterMoveConnCallBack(const TcpConnectionPtr &conn);
	void redisReadCallBack(const TcpConnectionPtr &conn,xBuffer *buffer);
	void redisConnCallBack(const TcpConnectionPtr &conn);

	void eraseRedisMap(int32_t sockfd);
	void insertRedisMap(int32_t sockfd,const RedisAsyncContextPtr &context);

	void pushTcpClient(const TcpClientPtr &client);
	void clearTcpClient();

	void start() { pool.start(); }
	void setThreadNum(int16_t threadNum) { pool.setThreadNum(threadNum); }

	auto &getPool() { return pool; }
	auto &getMutex() { return rtx; }
	auto &getAsyncContext() { return redisAsyncContexts; }
	auto &getTcpClient() { return tcpClients; }

	RedisAsyncContextPtr getIteratorNode();

private:
	typedef std::unordered_map<int32_t,RedisAsyncContextPtr> RedisAsyncContext;
	xThreadPool pool;
	std::vector<TcpClientPtr> tcpClients;
	RedisAsyncContext redisAsyncContexts;
	bool clusterMode;
	std::mutex rtx;
	RedisAsyncContext::iterator node;
};

int32_t redisFormatCommand(char **target,const char *format,...);
int32_t redisFormatCommandArgv(char **target,int32_t argc,const char **argv,const size_t *argvlen);
int32_t redisvFormatCommand(char **target,const char *format,va_list ap);

RedisContextPtr redisConnectWithTimeout(const char *ip,int16_t port,const struct timeval tv);
RedisContextPtr redisConnect(const char *ip,int16_t port);

