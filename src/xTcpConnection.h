#pragma once

#include "all.h"
#include "xBuffer.h"
#include "xCallback.h"
#include "xChannel.h"

class xEventLoop;
class xTcpConnection : noncopyable,public std::enable_shared_from_this<xTcpConnection>
{
public:
	enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };
	xTcpConnection(xEventLoop *loop,int32_t sockfd,const std::any &context);
	~xTcpConnection();

	xEventLoop	 *getLoop();
	int  getSockfd();
	void setState(StateE s) { state  = s; }

	void setConnectionCallback(const ConnectionCallback &cb)
	{ connectionCallback = cb; }

	void setMessageCallback(const MessageCallback &cb)
	{ messageCallback = cb; }

	void setWriteCompleteCallback(const WriteCompleteCallback &cb)
	{ writeCompleteCallback = cb; }

	void setHighWaterMarkCallback(const HighWaterMarkCallback &cb, size_t highWaterMark)
	{ highWaterMarkCallback = cb; this->highWaterMark  = highWaterMark; }

	void setCloseCallback(const CloseCallback &cb)
	{ closeCallback = cb; }

	void handleRead();
	void handleWrite();
	void handleClose();
	void handleError();

	void sendInLoop(const void *message, size_t len);
	void sendInLoop(const xStringPiece &message);
	void sendPipeInLoop(const void *message, size_t len);
	void sendPipeInLoop(const xStringPiece &message);

	static void bindSendInLoop(xTcpConnection *conn, const xStringPiece &message);
	static void bindSendPipeInLoop(xTcpConnection *conn, const xStringPiece &message);
	
	void sendPipe(const xStringPiece &message);
	void sendPipe(xBuffer *message);
	void sendPipe(const void *message, int len);

  	void send(const void *message, int len);
	void send(xBuffer *message);
	void send(const xStringPiece &message);

	bool disconnected() const { return state == kDisconnected; }
	bool connected();
	void forceCloseInLoop();
	void connectEstablished();

	void connectDestroyed();
	void shutdown();
	void shutdownInLoop();
	void forceClose();
	
	std::any* getContext() { return &context; }
	const std::any& getContext() const { return context; }
	void setContext(const std::any& context) { this->context = context; }

	xBuffer *outputBuffer() { return &sendBuff; }

public:
	xEventLoop *loop;
	int sockfd;
	xBuffer recvBuff;
	xBuffer sendBuff;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;
	HighWaterMarkCallback highWaterMarkCallback;
	CloseCallback closeCallback;

	size_t highWaterMark;
	StateE state;
	std::shared_ptr<xChannel> channel;
	std::any  context;
	std::string ip;
	int16_t  port;

};
