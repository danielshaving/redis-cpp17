#pragma once
#include "acceptor.h"
#include "threadpool.h"
#include "callback.h"

class EventLoop;
class TcpServer
{
public:
	typedef std::function<void(EventLoop*)> ThreadInitCallback;

	TcpServer(EventLoop *loop, const char *ip, int16_t port, const std::any &context);
	~TcpServer();

	void newConnection(int32_t sockfd);
	void start();

	void removeConnection(const TcpConnectionPtr &conn);
	void removeConnectionInLoop(const TcpConnectionPtr &conn);
	void setThreadInitCallback(const ThreadInitCallback &&cb) { threadInitCallback = std::move(cb); }
	void setConnectionCallback(const ConnectionCallback &&cb) { connectionCallback = std::move(cb); }
	void setMessageCallback(const MessageCallback &&cb) { messageCallback = std::move(cb); }
	void setWriteCompleteCallback(const WriteCompleteCallback &&cb) { writeCompleteCallback = std::move(cb); }
	void setThreadNum(int16_t numThreads);

	EventLoop *getLoop() const { return loop; }
	ThreadPoolPtr getThreadPool() { return threadPool; }

	std::any *getMutableContext() { return &context; }
	const std::any &getContext() const { return context; }
	void setContext(const std::any &context) { this->context = context; }

private:
	TcpServer(const TcpServer&);
	void operator=(const TcpServer&);

	EventLoop *loop;
	AcceptorPtr acceptor;
	ThreadPoolPtr threadPool;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;
	ThreadInitCallback threadInitCallback;

	typedef std::unordered_map<int32_t, TcpConnectionPtr> ConnectionMap;
	ConnectionMap connections;
	std::any context;

};
