#pragma once

#include "xAcceptor.h"
#include "xThreadPool.h"
#include "xCallback.h"

class xEventLoop;

class xTcpServer : noncopyable
{
public:
	typedef std::function<void(xEventLoop*)> ThreadInitCallback;
	xTcpServer(xEventLoop *loop,std::string ip,int16_t port,const std::any &context);
	~xTcpServer();
	void newConnection(int32_t sockfd);
	void start();

	void removeConnection(const TcpConnectionPtr &conn);
	void removeConnectionInLoop(const TcpConnectionPtr &conn);
	void setThreadInitCallback(const ThreadInitCallback &&cb) { threadInitCallback = std::move(cb); }
	void setConnectionCallback(const ConnectionCallback &&cb) { connectionCallback = std::move(cb); }
	void setMessageCallback(const MessageCallback &&cb) { messageCallback = std::move(cb); }
	void setWriteCompleteCallback(const WriteCompleteCallback &&cb) { writeCompleteCallback = std::move(cb); }
	void setThreadNum(int16_t numThreads);

	xEventLoop *getLoop() const { return loop; }
	std::shared_ptr<xThreadPool> getThreadPool() { return threadPool; }

	std::any *getContext() { return &context; }
	const std::any &getContext() const { return context; }
	void setContext(const std::any &context) { this->context = context; }

private:
	xEventLoop *loop;
	std::unique_ptr<xAcceptor> 	acceptor;
	std::shared_ptr<xThreadPool> 	threadPool;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;
	ThreadInitCallback threadInitCallback;

	typedef std::unordered_map<int32_t, TcpConnectionPtr> ConnectionMap;
	ConnectionMap connections;
	std::any context;

};
