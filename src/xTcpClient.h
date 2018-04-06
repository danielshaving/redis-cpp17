#pragma once

#include "xAll.h"
#include "xEventLoop.h"
#include "xCallback.h"


class xTcpClient: noncopyable
{
public:
	xTcpClient(xEventLoop *loop,const char *ip,int16_t port,const std::any &context);
	~xTcpClient();

	bool syncConnect();
	void asyncConnect();
	void disConnect();
	void stop();

	bool getRetry() { return retry; }
	void enableRetry() { retry = true; }
	void closeRetry() { retry = false; }

	void setConnectionErrorCallBack(const ConnectionErrorCallback &&cb) { connectionErrorCallBack = std::move(cb); }
	void setConnectionCallback(const ConnectionCallback &&cb) { connectionCallback = std::move(cb); }
	void setMessageCallback(const MessageCallback &&cb) { messageCallback = std::move(cb); }
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

	int32_t nextConnId;
	mutable std::mutex mutex;
	ConnectionErrorCallback connectionErrorCallBack;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;

	TcpConnectionPtr connection;
	std::any context;
	bool retry;
	bool connect;
};
