#pragma once
#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"

class xRedis;
class xSentinel : noncopyable
{
public:
	xSentinel(xRedis * redis);
	~xSentinel();

	void connectSentinel();
	void connErrorCallBack();
	void readCallBack(const TcpConnectionPtr &conn,xBuffer *buffer);
	void connCallBack(const TcpConnectionPtr &conn);
	void reconnectTimer(const std::any &context);

private:
	xRedis *redis;
	bool start;
	bool isreconnect;
	xEventLoop *loop;
	xTcpClient *client;
	std::string ip;
	int32_t port;
	xBuffer sendBuf;
	int connectCount;
	xSocket socket;
	TcpConnectionPtr conn;
};
