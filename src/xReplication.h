#pragma once

#include "xAll.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"

class xRedis;
class xReplication : noncopyable
{
public:
	xReplication(xRedis *redis);
	~xReplication();
	
	void connectMaster();
	void replicationSetMaster(rObj *obj,int16_t port);

	void slaveCallBack(const TcpConnectionPtr &conn,xBuffer *buffer);
	void readCallBack(const TcpConnectionPtr &conn,xBuffer *buffer);
	void connCallBack(const TcpConnectionPtr &conn);

	void reconnectTimer(const std::any &context);
	void syncWithMaster(const TcpConnectionPtr &conn);
	void replicationCron();
	void syncWrite(const TcpConnectionPtr &conn);
	void disConnect();

public:
	xRedis *redis;
	xEventLoop *loop;
	TcpClientPtr client;
	std::string ip;
	int32_t port;
	int32_t replLen;
	int32_t replState;
	xBuffer sendBuf;
	int32_t connectCount;
	xSocket socket;
	FILE *fp ;
	int32_t salveLen;
	int32_t salveReadLen;
	TcpConnectionPtr repliConn;
	xTimer *timer;
	std::atomic<bool> slaveSyncEnabled;
};

