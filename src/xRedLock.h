#pragma once
#include "all.h"
#include "xHiredis.h"
#include "xSds.h"


class xLock : noncopyable
{
public:
	xLock();
	~xLock();

	int validityTime;
	sds  resource;
	sds  val;
};


class xRedLock : noncopyable
{
public:
	xRedLock();
	~xRedLock();

	void syncAddServerUrl(const char *ip,const int port);
	void asyncAddServerUrl(const char *ip,const int port);
	redisReply * commandArgv(const xRedisContextPtr &c, int argc, char **inargv);
	sds	 getUniqueLockId();
	bool lock(const char *resource, const int ttl, xLock &lock);
	bool unlock(const xLock &lock);
	void unlockInstance(const xRedisContextPtr &c,const char * resource,const  char *val);
	int  lockInstance(const xRedisContextPtr &c,const char * resource,const  char *val,const int ttl);

private:

	static int defaultRetryCount;
	static int defaultRetryDelay;
	static float clockDriftFactor;

	sds 	unlockScript;
	int 	retryCount;
	int 	retryDelay;
	int		quoRum;
	int		fd;
	xLock	continueLock;
	sds		continueLockScript;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	std::vector<xRedisContextPtr> disconnectServers;
	std::unordered_map<int32_t,xRedisContextPtr> syncServerMaps;
	xHiredisAsyncPtr	asyncServerMaps;

};
