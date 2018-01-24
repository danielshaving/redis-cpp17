#pragma once

#include "xAcceptor.h"
#include "xThreadPool.h"
#include "xCallback.h"

class xEventLoop;

class xTcpServer: noncopyable
{
public:
	typedef std::function<void(xEventLoop*)> ThreadInitCallback;
	xTcpServer(xEventLoop *loop,std::string ip,int16_t port,void *data);
	~xTcpServer();
	void newConnection(int sockfd);
	void start();

	void removeConnection(const xTcpconnectionPtr& conn);
	void removeConnectionInLoop(const xTcpconnectionPtr& conn);
	void setThreadInitCallback(const ThreadInitCallback& cb){ threadInitCallback= cb; }
	void setConnectionCallback(const ConnectionCallback& cb){ connectionCallback = cb; }
	void setMessageCallback(const MessageCallback& cb){ messageCallback = cb; }
	void setWriteCompleteCallback(const WriteCompleteCallback& cb){ writeCompleteCallback = cb; }
	void setThreadNum(int numThreads);
	void setData(void *data);
	xEventLoop *getLoop() const { return loop; }
	std::shared_ptr<xThreadPool> getThreadPool() { return threadPool;}

private:
	xEventLoop* loop;
	std::unique_ptr<xAcceptor> 	acceptor;
	std::shared_ptr<xThreadPool> 	threadPool;


	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;
	ThreadInitCallback threadInitCallback;


	typedef std::unordered_map<int, xTcpconnectionPtr> ConnectionMap;
	ConnectionMap connections;
	void *data;

};
