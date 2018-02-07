#pragma once

#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"

class xRedis;
class xReplication : noncopyable
{
public:
	xReplication(xRedis * redis);
	~xReplication();
	
	void connectMaster();
	void replicationSetMaster(rObj * obj,int16_t port);

	void connErrorCallBack();
	void slaveCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf);
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf);
	void connCallBack(const xTcpconnectionPtr& conn);

	void reconnectTimer(const std::any &context);
	void syncWithMaster(const xTcpconnectionPtr& conn);
	void replicationCron();
	void syncWrite(const xTcpconnectionPtr& conn);
	void disconnect();

public:
	xRedis *redis;
	bool start;
	bool isreconnect;
	xEventLoop *loop;
	xTcpClient *client;
	std::string ip;
	int32_t port;
	int32_t replLen;
	int32_t replState;
	xBuffer sendBuf;
	int32_t connectCount;
	xSocket socket;
	FILE * fp ;
	int32_t salveLen;
	int32_t salveReadLen;
	xTcpconnectionPtr conn;
	xTimer *timer;
	std::atomic<bool>  slaveSyncEnabled;
};

