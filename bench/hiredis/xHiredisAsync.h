#pragma once
#include "xHiredis.h"

class xHiredisAsync : noncopyable
{
public:
	xHiredisAsync(xEventLoop *loop,int8_t threadCount,const char *ip,int16_t port);
	~xHiredisAsync();

	void redisConnCallBack(const TcpConnectionPtr& conn);
	xHiredis *getHiredis() { return &hiredis; }
	void serverCron(const std::any &context);
	void getCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void setCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);

private:
	xHiredis hiredis;
	std::atomic<int> connectCount;
	xEventLoop *loop;
	std::condition_variable condition;
	bool cron;
};
