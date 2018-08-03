#pragma once
#include "all.h"
#include "tcpconnection.h"
#include "object.h"
#include "tcpclient.h"
#include "socket.h"
#include "log.h"
#include "threadpool.h"
#include "sds.h"

/* This is the reply object returned by redisCommand() */

struct RedisReply
{
	RedisReply();
	~RedisReply();

    char type;			/* REDIS_REPLY_* */
    int64_t integer;	/* The integer when type is REDIS_REPLY_INTEGER */
    int32_t len;	 	/* Length of string */
    size_t elements;	/* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    sds str;	 		/* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    std::vector<RedisReplyPtr> element;	/* elements vector for REDIS_REPLY_ARRAY */
};

struct RedisReadTask
{
    int32_t type;	
    int32_t elements;
    int32_t idx;
    std::any privdata;
    RedisReplyWeakPtr obj;
    struct RedisReadTask *parent;
};

RedisReplyPtr createReplyObject(int32_t type);
RedisReplyPtr createString(const RedisReadTask *task,const char *str,size_t len);
RedisReplyPtr createArray(const RedisReadTask *task,int32_t elements);
RedisReplyPtr createInteger(const RedisReadTask *task,int64_t value);
RedisReplyPtr createNil(const RedisReadTask *task);

/* Default set of functions to build the reply. Keep in mind that such a
 * function returning NULL is interpreted as OOM. */

struct RedisFunc
{
	RedisFunc()
	{
		createStringFuc = createString;
		createArrayFuc = createArray;
		createIntegerFuc = createInteger;
		createNilFuc = createNil;
	}
	
	std::function<RedisReplyPtr(const RedisReadTask*,const char*,size_t)> createStringFuc;
	std::function<RedisReplyPtr(const RedisReadTask*,int32_t)> createArrayFuc;
	std::function<RedisReplyPtr(const RedisReadTask*,int64_t)> createIntegerFuc;
	std::function<RedisReplyPtr(const RedisReadTask*)> createNilFuc;
};

class RedisReader
{
public:
	RedisReader();
	RedisReader(Buffer *buffer);

	void redisReaderSetError(int32_t type,const char *str);
	void redisReaderSetErrorProtocolByte(char byte);
	void redisReaderSetErrorOOM();
	void moveToNextTask();

	int32_t redisReaderGetReply(RedisReplyPtr &reply);
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
	RedisReadTask rstack[9];
	char errstr[128];
	int32_t ridx;
	int32_t err;
	size_t pos;
	RedisFunc fn;
	Buffer *buffer;
	Buffer buf;
	RedisReplyPtr reply;
	std::any privdata;
};

class RedisContext : public std::enable_shared_from_this<RedisContext>
{
public:
	RedisContext();
	RedisContext(Buffer *buffer,int32_t sockfd);
	~RedisContext();

	RedisReplyPtr redisCommand(const char *format,...);
	RedisReplyPtr redisvCommand(const char *format,va_list ap);
	RedisReplyPtr redisCommandArgv(int32_t argc,
			const char **argv,const size_t *argvlen);
	RedisReplyPtr redisBlockForReply();

	void redisAppendFormattedCommand(const char *cmd,size_t len);
	void redisAppendCommand(const char *cmd,size_t len);
	void redisSetError(int32_t type,const char *str);
	void clear();

	int32_t redisvAppendCommand(const char *format,va_list ap);
	int32_t redisAppendCommandArgv(int32_t argc,
			const char **argv,const size_t *argvlen);
	int32_t redisContextWaitReady(int32_t msec);
	int32_t redisCheckSocketError();
	int32_t redisBufferRead();
	int32_t redisBufferWrite(int32_t *done);
	int32_t redisGetReply(RedisReplyPtr &reply);
	int32_t redisContextConnectTcp(const char *ip,
			int16_t port,const struct timeval *timeout);
	int32_t redisAppendCommand(const char *format, ...);
	int32_t redisContextConnectUnix(const char *path,
			const struct timeval *timeout);

private:
	RedisContext(const RedisContext&);
	void operator=(const RedisContext&);

public:
	char errstr[128];	/* String representation of error when applicable */
	const char *ip;
	int16_t port;
	const char *path;
	int32_t err;	/* Error flags, 0 when there is no error */
	int32_t fd;
	int8_t flags;
	Buffer sender;  /* Write buffer */
	RedisReaderPtr reader;	/* Protocol reader */
};

typedef std::function<void(const RedisAsyncContextPtr &context,
			const RedisReplyPtr &,const std::any &)> RedisCallbackFn;
struct RedisCallback
{
	RedisCallbackFn fn;
	std::any privdata;
};

struct RedisAsyncCallback
{
	RedisAsyncCallback();
	~RedisAsyncCallback();

	RedisCallback cb;
	size_t len;
	char *data;
};

typedef std::shared_ptr<RedisAsyncCallback> RedisAsyncCallbackPtr;
typedef std::list<RedisAsyncCallbackPtr> RedisAsyncCallbackList;

/* Subscription callbacks */
struct SubCallback
{
	SubCallback();
	~SubCallback();
	RedisAsyncCallbackList invalidCb;
	std::unordered_map<RedisObjectPtr,RedisAsyncCallbackPtr,Hash,Equal> channelCb;
	std::unordered_map<RedisObjectPtr,RedisAsyncCallbackPtr,Hash,Equal> patternCb;
};

class RedisAsyncContext : public std::enable_shared_from_this<RedisAsyncContext>
{
public:
	RedisAsyncContext(Buffer *buffer,const TcpConnectionPtr &conn);
	~RedisAsyncContext();

	int32_t __redisAsyncCommand(const RedisAsyncCallbackPtr &asyncCallback);
	int32_t redisvAsyncCommand(const RedisCallbackFn &fn,
			const std::any &privdata,const char *format,va_list ap);
	int32_t redisAsyncCommand(const RedisCallbackFn &fn,
			const std::any &privdata,const char *format, ...);

	int32_t err;
	char *errstr;
	std::any data;
	RedisContextPtr redisContext;
	TcpConnectionPtr redisConn;
	RedisAsyncCallbackList repliesCb;
	SubCallback	subCb;
private:
	RedisAsyncContext(const RedisAsyncContext&);
	void operator=(const RedisAsyncContext&);
};

class Hiredis
{
public:
	Hiredis(EventLoop *loop,int16_t sessionCount,bool clusterMode = false);
	~Hiredis();

	void redisAsyncDisconnect(const RedisAsyncContextPtr &ac);
	void redisGetSubscribeCallback(const RedisAsyncContextPtr &ac,
		const RedisReplyPtr &reply,RedisAsyncCallbackPtr &callback);
	void clusterAskConnCallback(const TcpConnectionPtr &conn);
	void clusterMoveConnCallback(const TcpConnectionPtr &conn);
	void redisReadCallback(const TcpConnectionPtr &conn,Buffer *buffer);
	void redisConnCallback(const TcpConnectionPtr &conn);

	void setDisconnectionCallback(const DisConnectionCallback &&cb)
	{ disConnectionCallback = std::move(cb); }

	void setConnectionCallback(const ConnectionCallback &&cb)
	{ connectionCallback = std::move(cb); }

	void pushTcpClient(const TcpClientPtr &client);
	void clearTcpClient();
	void diconnectTcpClient();
	void start(const char *ip,int16_t port);
	bool redirectySlot(const char *ip,int16_t port,
			const RedisAsyncContextPtr &ac,const RedisReplyPtr &reply,
			const RedisAsyncCallbackPtr &repliesCb);

	void setThreadNum(int16_t threadNum)
	{ pool.setThreadNum(threadNum); }

	auto &getPool() { return pool; }
	auto &getTcpClient() { return tcpClients; }

private:
	Hiredis(const Hiredis&);
	void operator=(const Hiredis&);

	ThreadPool pool;
	bool clusterMode;
	std::mutex mutex;
	ConnectionCallback connectionCallback;
	DisConnectionCallback disConnectionCallback;
	std::vector<TcpClientPtr> tcpClients;
	int32_t sessionCount;
};

int redisFormatSdsCommandArgv(sds *target,int argc,
		const char **argv,const size_t *argvlen);
int32_t redisFormatCommand(char **target,const char *format,...);
int32_t redisFormatCommandArgv(char **target,int32_t argc,
			const char **argv,const size_t *argvlen);
int32_t redisvFormatCommand(char **target,const char *format,va_list ap);

RedisContextPtr redisConnectWithTimeout(const char *ip,
		int16_t port,const struct timeval tv);
RedisContextPtr redisConnect(const char *ip,int16_t port);
RedisContextPtr redisConnectUnix(const char *path);


