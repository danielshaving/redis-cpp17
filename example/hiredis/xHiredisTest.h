#pragma once
#include "xHiredis.h"

class xHiredisTest : boost::noncopyable
{
public:
	xHiredisTest(xEventLoop *loop,int8_t threadCount,
			int16_t sessionCount,int32_t messageCount,const char *ip,int16_t port);
	~xHiredisTest();

	void redisConnCallBack(const TcpConnectionPtr& conn);
	xHiredis *getHiredis() { return &hiredis; }

	void string();
	void hash();
	void list();

	void setCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void getCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void hgetCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void hsetCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void hgetallCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);

	void lpushCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void rpushCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void rpopCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void lpopCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);

private:
	xHiredis hiredis;
	std::atomic<int32_t> connectCount;
	int32_t count;
	int16_t sessionCount;
	std::condition_variable condition;
	xEventLoop *loop;
	int32_t messageCount;
};
