#pragma once
#include "sds.h"
#include "tcpconnection.h"
#include "object.h"
#include "tcpclient.h"
#include "socket.h"
#include "log.h"
#include "threadpool.h"

class RedisAsyncContext;
struct RedisReply
{
    int32_t type;
    int64_t integer;
    int32_t len;
    size_t elements;
    char *str;
    struct RedisReply **element;
};

struct RedisReadTask
{
    int32_t type;
    int32_t elements;
    int32_t idx;
    std::any privdata;
    RedisReply *obj;
    struct RedisReadTask *parent;
};

void freeReply(RedisReply *reply);
RedisReply *createReplyObject(int32_t type);
RedisReply *createString(const RedisReadTask *task,const char *str,size_t len);
RedisReply *createArray(const RedisReadTask *task,int32_t elements);
RedisReply *createInteger(const RedisReadTask *task,int64_t value);
RedisReply *createNil(const RedisReadTask *task);

struct RedisFunc
{
	RedisFunc()
	{
		createStringFuc = createString;
		createArrayFuc = createArray;
		createIntegerFuc = createInteger;
		createNilFuc = createNil;
		freeObjectFuc = freeReply;
	}
	
	std::function<RedisReply*(const RedisReadTask*,const char*,size_t)> createStringFuc;
	std::function<RedisReply*(const RedisReadTask*,int32_t)> createArrayFuc;
	std::function<RedisReply*(const RedisReadTask*,int64_t)> createIntegerFuc;
	std::function<RedisReply*(const RedisReadTask*)> createNilFuc;
	std::function<void (RedisReply*)> freeObjectFuc;
};

class RedisReader
{
public:
	RedisReader();
	RedisReader(Buffer *buffer);
	~RedisReader();

	int32_t redisReaderGetReply(RedisReply **reply);
	void redisReaderSetError(int32_t type,const char *str);
	void redisReaderSetErrorProtocolByte(char byte);
	void redisReaderSetErrorOOM() { redisReaderSetError(REDIS_ERR_OOM,"Out of memory"); }
	void moveToNextTask();
	int32_t processLineItem();
	int32_t processBulkItem();
	int32_t processMultiBulkItem();
	int32_t processItem();

	int64_t readLongLong(const char *s);
	const char *readBytes(uint32_t bytes);
	const char *readLine(int32_t *_len);

private:
	RedisReader(const EventLoop&);
	void operator=(const RedisReader&);

public:
	char errstr[128];
	RedisReadTask rstack[9];
	int32_t ridx;
	int32_t err;
	size_t pos;
	RedisFunc fn;
	Buffer buf;
	Buffer *buffer;
	RedisReply *reply;
	std::any privdata;
};

typedef std::function<void(const RedisAsyncContextPtr &context,RedisReply*,const std::any &)> RedisCallbackFn;
struct RedisCallback
{
	RedisCallbackFn fn;
    std::any privdata;
};

struct RedisAsyncCallback
{
	RedisAsyncCallback()
	:data(nullptr),
	 len(0)
	{

	}
	char *data;
	int32_t len;
	RedisCallback cb;
};

class RedisContext
{
public:
	RedisContext();
	RedisContext(Buffer *buffer,int32_t sockfd);
	~RedisContext();

	int32_t redisvAppendCommand(const char *format,va_list ap);
	void redisAppendCommand(const char *cmd,size_t len) { sender.append(cmd,len); }
	RedisReply *redisCommand(const char *format,...);
	RedisReply *redisvCommand(const char *format,va_list ap);
	RedisReply *redisCommandArgv(int32_t argc,const char **argv,const size_t *argvlen);
	void redisAppendFormattedCommand(const char *cmd,size_t len) { redisAppendCommand(cmd,len); }
	int32_t redisAppendCommandArgv(int32_t argc,const char **argv,const size_t *argvlen);
	void redisSetError(int32_t type,const char *str);
	RedisReply *redisBlockForReply();
	int32_t redisContextWaitReady(int32_t msec);
	int32_t redisCheckSocketError();
	int32_t redisBufferRead();
	int32_t redisBufferWrite(int32_t *done);
	int32_t redisGetReply(RedisReply **reply);
	int32_t redisGetReplyFromReader(RedisReply **reply);
	int32_t redisContextConnectTcp(const char *ip,int16_t port,const struct timeval *timeout);
	int32_t redisAppendCommand(const char *format, ...);
	int32_t redisContextConnectUnix(const char *path,const struct timeval *timeout);

private:
	RedisContext(const RedisContext&);
	void operator=(const RedisContext&);

public:
	void clear();
	void setBlock();
	void setConnected();
	void setDisConnected();

	char errstr[128];
	const char *ip;
	int16_t port;
	const char *path;
	int32_t err;
	int32_t fd;
	int8_t flags;

	Buffer sender;
	RedisReaderPtr reader;
};

class RedisAsyncContext
{
public:
	typedef std::list<RedisAsyncCallback> RedisAsyncCallbackList;
	RedisAsyncContext(Buffer *buffer,const TcpConnectionPtr &conn);
	~RedisAsyncContext();

	void  __redisAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,char *cmd,size_t len);
	int redisvAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,const char *format,va_list ap);
	int redisAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,const char *format, ...);

	int32_t redisGetReply(RedisReply **reply) { return context->redisGetReply(reply); }
	RedisContextPtr getRedisContext() { return context; }
	TcpConnectionPtr getServerConn() { return serverConn; }
	std::mutex &getMutex() { return mtx;}
	RedisAsyncCallbackList &getCb() { return asyncCb; }

private:
	RedisAsyncContext(const RedisAsyncContext&);
	void operator=(const RedisAsyncContext&);

	int32_t err;
	char *errstr;
	std::any data;
	RedisContextPtr context;
	TcpConnectionPtr serverConn;
	RedisAsyncCallbackList asyncCb;
	std::mutex mtx;
};

class Hiredis
{
public:
	Hiredis(EventLoop *loop,bool clusterMode = false);
	~Hiredis();

	void clusterAskConnCallBack(const TcpConnectionPtr &conn);
	void clusterMoveConnCallBack(const TcpConnectionPtr &conn);
	void redisReadCallBack(const TcpConnectionPtr &conn,Buffer *buffer);
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
	Hiredis(const Hiredis&);
	void operator=(const Hiredis&);

	typedef std::unordered_map<int32_t,RedisAsyncContextPtr> RedisAsyncContextMap;
	ThreadPool pool;
	std::vector<TcpClientPtr> tcpClients;
	RedisAsyncContextMap redisAsyncContexts;
	bool clusterMode;
	std::mutex rtx;
	RedisAsyncContextMap::iterator node;
};

int32_t redisFormatCommand(char **target,const char *format,...);
int32_t redisFormatCommandArgv(char **target,int32_t argc,const char **argv,const size_t *argvlen);
int32_t redisvFormatCommand(char **target,const char *format,va_list ap);

RedisContextPtr redisConnectWithTimeout(const char *ip,int16_t port,const struct timeval tv);
RedisContextPtr redisConnect(const char *ip,int16_t port);
RedisContextPtr redisConnectUnix(const char *path);

