#pragma once
#include "all.h"
#include "xTcpServer.h"

class xEventLoop;
class xHttpRequest;
class xHttpResponse;

class xHttpServer:noncopyable
{
public:
	typedef std::function<void(const xHttpRequest &,xHttpResponse*)> HttpCallBack;

	xHttpServer(xEventLoop *loop,const char *ip,uint16_t  port);
	~xHttpServer();
	void disPlayer(const char *begin);
	void setThreadNum(int numThreads)
	{
		server.setThreadNum(numThreads);
	}

	void setMessageCallback(HttpCallBack callback);
	void start();
	void onConnection(const TcpConnectionPtr & conn);
	void onMessage(const TcpConnectionPtr &conn,xBuffer *recvBuf);
	void onRequest(const TcpConnectionPtr &conn,const xHttpRequest &req);

private:
	xEventLoop *loop;
	xTcpServer server;
	HttpCallBack httpCallback;
};
