	#pragma once

#include "all.h"
#include "xEventLoop.h"
#include "xCallback.h"


class xTcpClient: noncopyable
{
public:
	xTcpClient();
	xTcpClient(xEventLoop *loop,const std::any & context);
	~xTcpClient();

	void connect(const char * ip,int16_t port);
	void disconnect();
	void stop();

	void setConnectionErrorCallBack(ConnectionErrorCallback &&cb) { connectionErrorCallBack = std::move(cb);}
	void setConnectionCallback(ConnectionCallback&& cb) { connectionCallback = std::move(cb); }
	void setMessageCallback(MessageCallback&&  cb){ messageCallback = std::move(cb); }
	void setWriteCompleteCallback(WriteCompleteCallback&& cb) { writeCompleteCallback = std::move(cb); }

	xEventLoop * getLoop(){ return loop; }

	std::any* getContext() { return &context; }
	const std::any& getContext() const { return context; }
	void setContext(const std::any& context) { this->context = context; }

public:
	void errorConnection();
	void newConnection(int32_t sockfd);
	void removeConnection(const xTcpconnectionPtr& conn);

	xConnectorPtr connector;
	xEventLoop *loop;

	std::string ip;
	int16_t port;
	bool isconnect;
	int nextConnId;
	mutable std::mutex mutex;
	ConnectionErrorCallback connectionErrorCallBack;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;

	xTcpconnectionPtr connection;
	std::any context;


};
