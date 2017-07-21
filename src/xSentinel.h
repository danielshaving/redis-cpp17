#pragma once
#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"

class xRedis;
class xSentinel: noncopyable
{
public:
	xSentinel();
	~xSentinel();

	void connectSentinel();
	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	void reconnectTimer(void * data);
private:
	bool start;
	bool isreconnect;
	xEventLoop *loop;
	xTcpClient *client;
	xRedis *redis;
	std::string ip;
	int32_t port;
	xBuffer sendBuf;
	int connectCount;
	xSocket socket;
};
