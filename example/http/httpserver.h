#pragma once
#include "all.h"
#include "tcpserver.h"

class EventLoop;
class HttpRequest;
class HttpResponse;

class HttpServer : boost::noncopyable
{
public:
	typedef std::function<void(const HttpRequest &,HttpResponse*)> HttpCallBack;

	HttpServer(EventLoop *loop,const char *ip,uint16_t port);
	~HttpServer();
	void disPlayer(const char *begin);
	void setThreadNum(int numThreads)
	{
		server.setThreadNum(numThreads);
	}

	void setMessageCallback(HttpCallBack callback);
	void start();
	void onConnection(const TcpConnectionPtr &conn);
	void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	void onRequest(const TcpConnectionPtr &conn,const HttpRequest &req);

private:
	EventLoop *loop;
	TcpServer server;
	HttpCallBack httpCallback;
};
