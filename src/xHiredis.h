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
} redisFunc;



class xRedisReader
{
public:
	xRedisReader()
	{
		fn = std::shared_ptr<redisFunc>(new redisFunc());
		pos = 0;
		err = 0;
		errstr[0] = '\0';
		ridx 	= -1;
		buf = nullptr;
	}
	
	int err;
	char errstr[128];
	size_t pos;
	xBuffer  *buf;
	redisReadTask rstack[9];
	int ridx;
	void *reply;
	std::shared_ptr<redisFunc> fn;
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
		init();
		sender =  std::shared_ptr<xBuffer>(new xBuffer());
		reader = std::shared_ptr<xRedisReader>(new xRedisReader());
	}
	~xRedisContext()
	{
		if(fd > 0)
		{
			::close(fd);
		}
		init();
	}

	void init()
	{
		flags	&= ~REDIS_BLOCK;
		err = 0;
		errstr[0] = '\0';
		fd = 0 ;
	}
	
	int err;
	char errstr[128];
	int fd;
	int flags;
	xBufferPtr sender;
	xRedisReaderPtr reader;
};

class xRedisAsyncContext: noncopyable
{
public:
	xRedisAsyncContext():c(new (xRedisContext))
	{
		conn = nullptr;
		err = 0;
		errstr = nullptr;
		data = nullptr;
		c->flags &= ~REDIS_CONNECTED;
	}
	~xRedisAsyncContext()
	{
		if(conn != nullptr)
		{
			conn->forceClose();
		}
	}
	int err;
	char *errstr;
	void *data;
	xRedisContextPtr c;
	xTcpconnectionPtr conn;
	RedisCallbackList replies;
	std::mutex hiMutex;
	struct
	{
		RedisCallbackList invalid;
		std::map<rObj *,redisCallback> channels;
		std::map<rObj *,redisCallback> patterns;
	}sub;
};


int redisContextConnectTcp(const xRedisContextPtr &c, const char *addr, int port, const struct timeval *timeout);
xRedisContextPtr redisConnectWithTimeout(const char *ip, int port, const struct timeval tv);
xRedisContextPtr redisConnect(const char *ip, int port);

int redisAppendCommand(xRedisContextPtr c, const char *format, ...);

int __redisAsyncCommand(const xRedisAsyncContextPtr &ac,redisCallbackFn *fn, void *privdata, char *cmd, size_t len);
int redisvFormatCommand(char * *target, const char * format, va_list ap);
int redisvAsyncCommand(const xRedisAsyncContextPtr &ac, redisCallbackFn *fn, void *privdata, const char *format, va_list ap);
int redisAsyncCommand(const xRedisAsyncContextPtr &ac, redisCallbackFn *fn, void *privdata, const char *format, ...);

int redisAppendFormattedCommand(xRedisContextPtr c, const char *cmd, size_t len);
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


class  xHiredisAsync
{
public:
	xHiredisAsync(xEventLoop * loop,int threadCount,int sessionCount,const char *ip,int32_t port)
	:connectCount(0),
	loop(loop),
	pool(loop)
	{
		if(threadCount > 1)
		{
			pool.setThreadNum(threadCount);
		}

		pool.start();

 		for(int i = 0; i < sessionCount; i++)
 		{
 			std::shared_ptr<xTcpClient> client(new xTcpClient(pool.getNextLoop(),nullptr));
 			client->setConnectionCallback(std::bind(&xHiredisAsync::redisConnCallBack,this,std::placeholders::_1,std::placeholders::_2));
 			client->setMessageCallback(std::bind(&xHiredisAsync::redisReadCallBack,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
 			client->connect(ip,port);
 			vectors.push_back(client);
 		}

 		{
 			std::unique_lock <std::mutex> lck(rtx);
			while(connectCount  < sessionCount)
			{
				condition.wait(lck);
			}
 		}
	}

	void redisReadCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
	{
		xRedisAsyncContextPtr redis;
		{
			std::unique_lock <std::mutex> lck(rmutex);
			auto it = redisMaps.find(conn->getSockfd());
			assert(it != redisMaps.end());
			redis = it->second;
		}

		 redisCallback cb;
		 void  *reply = nullptr;
		 int status;
		 while((status = redisGetReply(redis->c,&reply)) == REDIS_OK)
		 {
			 if(reply == nullptr)
			 {
				 break;
			 }

			 {
				 std::unique_lock<std::mutex> lk(redis->hiMutex);
				 cb = std::move(redis->replies.front());
				 redis->replies.pop_front();
			 }

			 if(cb.fn)
			 {
				 cb.fn(redis,reply,cb.privdata);
			 }

			 redis->c->reader->fn->freeObjectFuc(reply);

		 }

	}

	void redisConnCallBack(const xTcpconnectionPtr& conn,void *data)
	{
		if(conn->connected())
		{
			xRedisAsyncContextPtr ac (new xRedisAsyncContext());
			ac->c->reader->buf = &(conn->recvBuff);
			ac->conn = conn;
			ac->c->fd = conn->getSockfd();

			{
				std::unique_lock<std::mutex> lk(rmutex);
				redisMaps.insert(std::make_pair(conn->getSockfd(),ac));
			}

			{
				std::unique_lock <std::mutex> lck(rtx);
				connectCount++;
				condition.notify_one();
			}

		}
		else
		{
			{
				std::unique_lock<std::mutex> lk(rmutex);
				redisMaps.erase(conn->getSockfd());
			}
		}

	}


public:
	int connectCount;
	xEventLoop *loop;
	xThreadPool pool;
	std::vector<std::shared_ptr<xTcpClient>> vectors;
	std::map<int32_t,xRedisAsyncContextPtr> redisMaps;
	std::mutex rmutex;
	std::mutex rtx;
	std::condition_variable condition;
};
