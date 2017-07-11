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

	xHttpServer(xEventLoop *loop,const std::string &ip,const int32_t port);
	~xHttpServer();
	//xEventLoop *getLoop() const { server.getLoop(); }
	void disPlayer(const char *begin);
	void setThreadNum(int numThreads)
	{
		server.setThreadNum(numThreads);
	}
	void start();

	void onConnection(const xTcpconnectionPtr & conn);
	void onMessage(const xTcpconnectionPtr &conn,xBuffer *recvBuf);
	void onRequest(const xTcpconnectionPtr &conn,const xHttpRequest &req);
private:
	xTcpServer server;
	HttpCallBack httpCallback;
};
