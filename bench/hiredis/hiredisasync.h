#pragma once
#include "hiredis.h"
#include "util.h"

int64_t startTime = 0;
int64_t endTime = 0;

class HiredisAsync : noncopyable
{
public:
	HiredisAsync(EventLoop *loop,int8_t threadCount,const char *ip,int16_t port);
	~HiredisAsync();

	void redisConnCallBack(const TcpConnectionPtr& conn);
	xHiredis *getHiredis() { return &hiredis; }
	void serverCron();
	void getCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);
	void setCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata);

private:
	Hiredis hiredis;
	std::atomic<int> connectCount;
	EventLoop *loop;
	std::condition_variable condition;
	bool cron;
};
