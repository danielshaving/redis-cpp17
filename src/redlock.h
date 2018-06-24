//
// Created by zhanghao on 2018/6/17.
//

#pragma once
#include "all.h"
#include "hiredis.h"
#include "sds.h"

class Lock
{
public:
	Lock();
	~Lock();

	int32_t validityTime;
	sds resource;
	sds val;
private:
	Lock(const Lock&);
	void operator=(const Lock&);
};

class RedLock
{
public:
	RedLock();
	~RedLock();

	void syncAddServerUrl(const char *ip,const int16_t port);
	void asyncAddServerUrl(const char *ip,const int16_t port);
	RedisReply *commandArgv(const RedisContextPtr &c,int32_t argc,char **inargv);
	sds getUniqueLockId();
	bool lock(const char *resource,const int32_t ttl,Lock &lock);
	bool unlock(const Lock &lock);
	void unlockInstance(const RedisContextPtr &c,const char *resource,const char *val);
	int32_t lockInstance(const RedisContextPtr &c,const char *resource,const char *val,const int32_t ttl);

private:
	RedLock(const RedLock&);
	void operator=(const RedLock&);

	static int32_t defaultRetryCount;
	static int32_t defaultRetryDelay;
	static float clockDriftFactor;

	sds unlockScript;
	int32_t retryCount;
	int32_t retryDelay;
	int32_t quoRum;
	int32_t fd;
	Lock continueLock;
	sds continueLockScript;

	struct timeval timeout = { 1,500000 };
	std::vector<RedisContextPtr> disconnectServers;
	std::unordered_map<int32_t,RedisContextPtr> syncServers;
	HiredisAsyncPtr	asyncServers;

};
