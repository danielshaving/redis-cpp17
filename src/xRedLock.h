#pragma once
#include "xAll.h"
#include "xHiredis.h"
#include "xSds.h"

class xLock : noncopyable
{
public:
	xLock();
	~xLock();

	int32_t validityTime;
	sds resource;
	sds val;
};

class xRedLock : noncopyable
{
public:
	xRedLock();
	~xRedLock();

	void syncAddServerUrl(const char *ip,const int16_t port);
	void asyncAddServerUrl(const char *ip,const int16_t port);
	redisReply *commandArgv(const RedisContextPtr &c,int32_t argc,char **inargv);
	sds getUniqueLockId();
	bool lock(const char *resource,const int32_t ttl,xLock &lock);
	bool unlock(const xLock &lock);
	void unlockInstance(const RedisContextPtr &c,const char *resource,const char *val);
	int32_t lockInstance(const RedisContextPtr &c,const char *resource,const char *val,const int32_t ttl);

private:
	static int32_t defaultRetryCount;
	static int32_t defaultRetryDelay;
	static float clockDriftFactor;

	sds unlockScript;
	int32_t retryCount;
	int32_t retryDelay;
	int32_t quoRum;
	int32_t fd;
	xLock continueLock;
	sds continueLockScript;

	struct timeval timeout = { 1, 500000 }; 
	std::vector<RedisContextPtr> disconnectServers;
	std::unordered_map<int32_t,RedisContextPtr> syncServers;
	HiredisAsyncPtr	asyncServers;

};
