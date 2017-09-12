#pragma once

#include "all.h"
#include "xEventLoop.h"
#include "xCallback.h"


class xTcpClient
{
public:
	xTcpClient();
	xTcpClient(xEventLoop *loop,void *data);
	~xTcpClient();


	void connect(const char * ip,int32_t port);
	void disconnect();
	void stop();

	void setConnectionErrorCallBack(ConnectionErrorCallback &&cb) { connectionErrorCallBack = std::move(cb);}
	void setConnectionCallback(ConnectionCallback&& cb) { connectionCallback = std::move(cb); }
	void setMessageCallback(MessageCallback&&  cb){ messageCallback = std::move(cb); }
	void setWriteCompleteCallback(WriteCompleteCallback&& cb) { writeCompleteCallback = std::move(cb); }


public:

	void errorConnection();
	void newConnection(int sockfd);
	void removeConnection(const xTcpconnectionPtr& conn);

	xConnectorPtr connector;
	xEventLoop *loop;


	bool isconnect;
	int nextConnId;
	mutable std::mutex mutex;
	ConnectionErrorCallback connectionErrorCallBack;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;

	xTcpconnectionPtr connection;
	void *data;


};
