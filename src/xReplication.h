#pragma once

#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"

class xRedis;
class xReplication: noncopyable
{
public:
	xReplication();
	~xReplication();

	void handleTimer(void * data);
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
	xEventLoop *loop;
	xTcpClient *client;
	xRedis *redis;
	std::string ip;
	int32_t port;
	int replLen;
	int replState;
	xBuffer sendBuf;
	int connectCount;
	xSocket socket;
	FILE * fp ;
	int32_t salveLen;
	int32_t salveReadLen;
	std::atomic<bool>  slaveSyncEnabled;
	xTcpconnectionPtr conn;
	xTimer *timer;
};

