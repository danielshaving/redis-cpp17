#pragma once
#include "httpcontext.h"
#include "httpresponse.h"
#include "tcpserver.h"
#include "util.h"

class EventLoop;
class HttpServer : boost::noncopyable
{
public:
	typedef std::function<bool(HttpRequest *,const TcpConnectionPtr &)> HttpReadCallBack;
	typedef std::function<void(const TcpConnectionPtr &)> HttpConnCallBack;

	HttpServer(EventLoop *loop,const char *ip,uint16_t  port);
	~HttpServer();

	void setThreadNum(int numThreads) { server.setThreadNum(numThreads); }
	void setMessageCallback(HttpReadCallBack callback);
	void setConnCallback(HttpConnCallBack callback);
	void start();
	void onConnection(const TcpConnectionPtr &conn);
	void onHandeShake(const TcpConnectionPtr &conn,Buffer *buffer);
	void onMessage(const TcpConnectionPtr &conn,Buffer *buffer);
	void onRequest(const TcpConnectionPtr &conn,const HttpRequest &req);

private:
	EventLoop *loop;
	TcpServer server;
	HttpReadCallBack httpReadCallback;
	HttpConnCallBack httpConnCallback;
	std::map<int32_t,std::shared_ptr<HttpContext>> webSockets;
};
