#pragma once
#include "xTcpServer.h"
#include "xUtil.h"
#include "xHttpContext.h"
#include "xHttpResponse.h"

class xEventLoop;
class xHttpServer : noncopyable
{
public:
	typedef std::function<bool(xHttpRequest *,xHttpResponse *)> HttpReadCallBack;
	typedef std::function<void(const TcpConnectionPtr &)> HttpConnCallBack;
	xHttpServer(xEventLoop *loop,const char *ip,uint16_t  port);
	~xHttpServer();

	void setThreadNum(int numThreads)
	{
		server.setThreadNum(numThreads);
	}

	void setMessageCallback(HttpReadCallBack callback);
	void setConnCallback(HttpConnCallBack callback);
	void start();
	void onConnection(const TcpConnectionPtr &conn);
	void onHandeShake(const TcpConnectionPtr &conn,xBuffer *buffer);
	void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	void onRequest(const TcpConnectionPtr &conn,const xHttpRequest &req);

private:
	xEventLoop *loop;
	xTcpServer server;
	HttpReadCallBack httpReadCallback;
	HttpConnCallBack httpConnCallback;
	std::map<int32_t,std::shared_ptr<xHttpContext>> webSockets;
};
