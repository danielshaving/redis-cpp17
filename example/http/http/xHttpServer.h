#pragma once
#include "xTcpServer.h"
#include "xUtil.h"
#include "xHttpContext.h"
#include "xHttpResponse.h"

class xEventLoop;
class xHttpServer : noncopyable
{
public:
	typedef std::function<void(xHttpRequest &,xHttpResponse *)> HttpCallBack;
	xHttpServer(xEventLoop *loop,const char *ip,uint16_t  port);
	~xHttpServer();

	void setThreadNum(int numThreads)
	{
		server.setThreadNum(numThreads);
	}

	void setMessageCallback(HttpCallBack callback);
	void start();
	void onConnection(const TcpConnectionPtr &conn);
	void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	void webMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	void onRequest(const TcpConnectionPtr &conn,const xHttpRequest &req);

private:
	xEventLoop *loop;
	xTcpServer server;
	HttpCallBack webCallback;
	std::map<int32_t,std::shared_ptr<xHttpContext>> webSockets;
};
