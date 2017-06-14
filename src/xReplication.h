#pragma once

#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"

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
	void reconnectTimer();
	void syncWithMaster(const xTcpconnectionPtr& conn);
	void replicationCron();
	void syncWrite(const xTcpconnectionPtr& conn);
public:
	bool start = false;
	bool isreconnect = true;
	pid_t pid;
	std::thread  *threads = nullptr;
	xEventLoop *loop;
	xTcpClient *client;
	mutable std::mutex mutex;
	std::condition_variable condition;
	xRedis *redis;
	std::string ip;
	int32_t port = 0 ;
	int replLen;
	int replState;          /* Replication status if the instance is a slave */
	xBuffer sendBuf;
	int connectCount = 0;
	static const int maxConnectCount = 3;
};

void replicationFeedSlaves(xBuffer &  sendBuf,rObj * commond  ,std::deque<rObj*> &robjs);

