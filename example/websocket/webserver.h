#pragma once
#include "webcontext.h"
#include "tcpserver.h"
#include "util.h"

class EventLoop;
class WebServer
{
public:
	typedef std::function<bool(HttpRequest *, const TcpConnectionPtr &)> WebReadCallBack;
	typedef std::function<void(const TcpConnectionPtr &)> WebConnCallBack;

	WebServer(EventLoop *loop, const char *ip, uint16_t  port);
	~WebServer();

	void setThreadNum(int numThreads) { server.setThreadNum(numThreads); }
	void setMessageCallback(WebReadCallBack callback);
	void setConnCallback(WebConnCallBack callback);
	void start();
	void onConnection(const TcpConnectionPtr &conn);
	void onHandeShake(const TcpConnectionPtr &conn, Buffer *buffer);
	void onMessage(const TcpConnectionPtr &conn, Buffer *buffer);
	void onRequest(const TcpConnectionPtr &conn, const HttpRequest &req);

private:
	EventLoop * loop;
	TcpServer server;
	WebReadCallBack httpReadCallback;
	WebConnCallBack httpConnCallback;
	std::map<int32_t, std::shared_ptr<HttpContext>> webSockets;
};
