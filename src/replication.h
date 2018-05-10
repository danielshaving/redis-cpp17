#pragma once
#include "all.h"
#include "object.h"
#include "tcpclient.h"
#include "socket.h"

class Redis;
class Replication
{
public:
	Replication(Redis *redis);
	~Replication();
	
	void connectMaster();
	void replicationSetMaster(rObj *obj,int16_t port);

	void slaveCallBack(const TcpConnectionPtr &conn,Buffer *buffer);
	void readCallBack(const TcpConnectionPtr &conn,Buffer *buffer);
	void connCallBack(const TcpConnectionPtr &conn);

	void reconnectTimer(const std::any &context);
	void syncWithMaster(const TcpConnectionPtr &conn);
	void replicationCron();
	void syncWrite(const TcpConnectionPtr &conn);
	void disConnect();

private:
	Replication(const Replication&);
	void operator=(const Replication&);

	Redis *redis;
	EventLoop *loop;
	TcpClientPtr client;
	std::string ip;
	int32_t port;
	int32_t replLen;
	int32_t replState;
	Buffer sendBuf;
	int32_t connectCount;
	Socket socket;
	FILE *fp;
	int32_t salveLen;
	int32_t salveReadLen;
	TcpConnectionPtr repliConn;
	Timer *timer;
	std::atomic<bool> slaveSyncEnabled;
};

