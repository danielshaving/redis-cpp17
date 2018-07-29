#pragma once
#include "hiredis.h"
#include "util.h"

class HiredisAsync
{
public:
	HiredisAsync(EventLoop *loop,int8_t threadCount,const char *ip,int16_t port);
	~HiredisAsync();

	void redisConnCallBack(const TcpConnectionPtr &conn);
	Hiredis *getHiredis() { return &hiredis; }
	void serverCron();
	void getCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void setCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);

private:
	Hiredis hiredis;
	std::atomic<int32_t> connectCount;
	EventLoop *loop;
	std::condition_variable condition;
	bool cron;
};
