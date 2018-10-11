#pragma once
#include "all.h"
#include "hiredis.h"

class EventBase;
class HiredisLogic
{
public:
	HiredisLogic(EventBase *base, EventLoop *loop, int16_t threadCount, int16_t sessionCount,
			const char *ip, int16_t port);
	~HiredisLogic();
	Hiredis *getHiredis() { return &hiredis; }

	void start();
	void connectionCallback(const TcpConnectionPtr &conn);
	void disConnectionCallback(const TcpConnectionPtr &conn);
		
	void setCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata);
	void loginCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata);

	void getCommand(const std::string &userID, int16_t cmd, const RedisCallbackFn &fn, const TcpConnectionPtr &conn);

private:
	EventBase *base;
	EventLoop *loop;
	Hiredis hiredis;
	std::atomic<int32_t> connectCount;
	int32_t count;
	int16_t sessionCount;
	std::mutex mutex;
	std::condition_variable condition;
	int32_t messageCount;
	sds unlockScript;
};
