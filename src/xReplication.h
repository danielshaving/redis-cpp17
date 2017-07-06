#pragma once

#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"

class xRedis;
class xReplication
{
public:
	xReplication();
	~xReplication();

	void connectMaster();
	void replicationSetMaster(xRedis * redis,rObj * obj,int32_t port);

	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);

	void reconnectTimer(void * data);
	void syncWithMaster(const xTcpconnectionPtr& conn);
	void replicationCron();
	void syncWrite(const xTcpconnectionPtr& conn);

public:

	bool start;
	bool isreconnect;
	pid_t pid;
	xEventLoop *loop;
	xTcpClient *client;
	mutable std::mutex mutex;
	std::condition_variable condition;
	xRedis *redis;
	std::string ip;
	int32_t port;
	int replLen;
	int replState;
	xBuffer sendBuf;
	int connectCount;
	xSocket socket;
	static const int maxConnectCount = 3;
};

void replicationFeedSlaves(xBuffer &  sendBuf,rObj * commond  ,std::deque<rObj*> &robjs);

