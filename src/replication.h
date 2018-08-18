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
	void replicationSetMaster(const RedisObjectPtr &obj,int16_t port);

	void slaveCallback(const TcpConnectionPtr &conn,Buffer *buffer);
	void readCallback(const TcpConnectionPtr &conn,Buffer *buffer);
	void connCallback(const TcpConnectionPtr &conn);

	void reconnectTimer(const std::any &context);
	void syncWithMaster(const TcpConnectionPtr &conn);
	void replicationCron();
	void syncWrite(const TcpConnectionPtr &conn);
	void disConnect();
	void close();

private:
	Replication(const Replication&);
	void operator=(const Replication&);

	Redis *redis;
	EventLoop *loop;
	TcpClientPtr client;
	std::string ip;
	Buffer sendBuf;
	FILE *fp;
	TcpConnectionPtr repliConn;
	std::atomic<int32_t> port;
	std::atomic<int32_t> replLen;
	std::atomic<int32_t> replState;
	std::atomic<int32_t> connectCount;
	std::atomic<int32_t> salveLen;
	std::atomic<int32_t> salveReadLen;
	std::atomic<bool> slaveSyncEnabled;
};

