#pragma once

#include "all.h"
#include "xLog.h"
#include "xHiredis.h"

class xClient;

class xHiredis:noncopyable
{
public:
	xHiredis(xEventLoop *loop,xClient * owner);
	~xHiredis();
	void start();
	void connSyncCallBack(const xTcpconnectionPtr& conn,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	void connErrorCallBack();
	void testCommand(xRedisContextPtr c);
	void testFormatCommand();
private:
	xTcpClient client;
	xClient * owner;
	std::map<int32_t,xRedisContextPtr> redisSyncs;
	mutable std::mutex mutex;
};


class xClient:noncopyable
{
public:
	xClient(xEventLoop *loop,const char *ip,uint16_t port)
	:loop(loop),
	ip(ip),
	port(port)
{
	std::shared_ptr<xHiredis>   redis = std::shared_ptr<xHiredis>(new xHiredis(loop,this));
	redis->start();
	redisVectors.push_back(redis);
}

~xClient()
{
	redisVectors.clear();
}

public:
	xEventLoop *loop;
	const char *ip;
	uint16_t port;
	int sessionCount;
	std::string message;
	std::vector<std::shared_ptr<xHiredis>>   redisVectors;
};

