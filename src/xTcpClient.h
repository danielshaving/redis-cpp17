	#pragma once

#include "all.h"
#include "xEventLoop.h"
#include "xCallback.h"


class xTcpClient: noncopyable
{
public:
	xTcpClient();
	xTcpClient(xEventLoop *loop,const std::any &context);
	~xTcpClient();

	void syncConnect(const char *ip,int16_t port);
	void asyncConnect(const char *ip,int16_t port);
	void disconnect();
	void stop();

	void setConnectionErrorCallBack(const ConnectionErrorCallback &&cb) { connectionErrorCallBack = std::move(cb); }
	void setConnectionCallback(const ConnectionCallback &&cb) { connectionCallback = std::move(cb); }
	void setMessageCallback(const MessageCallback &&cb){ messageCallback = std::move(cb); }
	void setWriteCompleteCallback(const WriteCompleteCallback &&cb) { writeCompleteCallback = std::move(cb); }

	xEventLoop *getLoop() { return loop; }
	std::any *getContext() { return &context; }
	const std::any &getContext() const { return context; }
	void setContext(const std::any &context) { this->context = context; }
	TcpConnectionPtr getConnection() { return connection; }

private:
	void errorConnection();
	void newConnection(int32_t sockfd);
	void removeConnection(const TcpConnectionPtr &conn);

	ConnectorPtr connector;
	xEventLoop *loop;

	bool isconnect;
	int nextConnId;
	mutable std::mutex mutex;
	ConnectionErrorCallback connectionErrorCallBack;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;

	TcpConnectionPtr connection;
	std::any context;


};
