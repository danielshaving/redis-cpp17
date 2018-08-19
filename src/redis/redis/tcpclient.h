#pragma once
#include "all.h"
#include "eventloop.h"
#include "callback.h"

class TcpClient
{
public:
	TcpClient(EventLoop *loop, const char *ip,
		int16_t port, const std::any &context);
	~TcpClient();

	void connect(bool sync = false);
	void disConnect();
	void stop();

	bool getRetry() { return retry; }
	void enableRetry() { retry = true; }
	void closeRetry() { retry = false; }

	void setConnectionErrorCallBack(const ConnectionErrorCallback &&cb)
	{
		connectionErrorCallBack = std::move(cb);
	}

	void setConnectionCallback(const ConnectionCallback &&cb)
	{
		connectionCallback = std::move(cb);
	}

	void setMessageCallback(const MessageCallback &&cb)
	{
		messageCallback = std::move(cb);
	}

	void setWriteCompleteCallback(const WriteCompleteCallback &&cb)
	{
		writeCompleteCallback = std::move(cb);
	}

	EventLoop *getLoop() { return loop; }
	std::any *getContext() { return &context; }
	const std::any &getContext() const { return context; }

	void setContext(const std::any &context) { this->context = context; }
	const char *getIp() { return ip; }
	int16_t getPort() { return port; }

	TcpConnectionPtr getConnection();

private:
	TcpClient(const TcpClient&);
	void operator=(const TcpClient&);

	void errorConnection();
	void newConnection(int32_t sockfd);
	void removeConnection(const TcpConnectionPtr &conn);

	ConnectorPtr connector;
	EventLoop *loop;

	std::mutex mutex;
	ConnectionErrorCallback connectionErrorCallBack;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;

	TcpConnectionPtr connection;
	std::any context;
	const char *ip;
	int16_t port;
	bool retry;
	bool connecting;
};
