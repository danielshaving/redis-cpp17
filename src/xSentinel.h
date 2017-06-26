#pragma once
#include "all.h"

#include "xEventLoop.h"
#include "xTcpServer.h"
#include "xPosix.h"

class xRedis;
class xSentinel
{
public:
	xSentinel(const char *ip,uint16_t port,bool clusterEnabled,int32_t threadCount);
	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	void run();

private:

	std::shared_ptr<xRedis> redis;
	xEventLoop loop;
	xTcpServer server;
	mutable MutexLock mutex;

	const char *ip;
	uint16_t port;
	bool clusterEnabled;
};
