#pragma once
#include "hiredis.h"

class HiredisTest
{
public:
	HiredisTest(EventLoop *loop,int8_t threadCount,
			int16_t sessionCount,int32_t messageCount,
			const char *ip,int16_t port);
	~HiredisTest();
	Hiredis *getHiredis() { return &hiredis; }

	void string();
	void hash();
	void list();
	void subscribe();
	void publish();
	void monitor();
	void redlock(const char *resource,const char *val,const int32_t ttl);

	void connectionCallback(const TcpConnectionPtr &conn);
	void disConnectionCallback(const TcpConnectionPtr &conn);

	void setCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void getCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void hgetCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void hsetCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void hgetallCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void lpushCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void rpushCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void rpopCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void lpopCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void subscribeCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void monitorCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void publishCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void redLockCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void redUnlcokCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);
	void setRedLockCallback(const RedisAsyncContextPtr &c,
			const RedisReplyPtr &reply,const std::any &privdata);

private:
	Hiredis hiredis;
	std::atomic<int32_t> connectCount;
	int32_t count;
	int16_t sessionCount;
	std::mutex mutex;
	std::condition_variable condition;
	EventLoop *loop;
	int32_t messageCount;
	sds unlockScript;
};
