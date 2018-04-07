#pragma once

#include "xAll.h"
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

	xEventLoop *getLoop() { return loop; }
	int32_t getSockfd() { return sockfd; }
	void setState(StateE s) { state  = s; }

	void setConnectionCallback(const ConnectionCallback &&cb)
	{ connectionCallback = std::move(cb); }

	void setMessageCallback(const MessageCallback &&cb)
	{ messageCallback = std::move(cb); }

	void setWriteCompleteCallback(const WriteCompleteCallback &&cb)
	{ writeCompleteCallback = std::move(cb); }

	void setHighWaterMarkCallback(const HighWaterMarkCallback &&cb,size_t highWaterMark)
	{ highWaterMarkCallback = std::move(cb); this->highWaterMark  = highWaterMark; }

	void setCloseCallback(const CloseCallback &cb)
	{ closeCallback = cb; }

	void sendInLoop(const void *message,size_t len);
	void sendInLoop(const xStringPiece &message);
	void sendPipeInLoop(const void *message,size_t len);
	void sendPipeInLoop(const xStringPiece &message);

	static void bindSendInLoop(xTcpConnection *conn,const xStringPiece &message);
	static void bindSendPipeInLoop(xTcpConnection *conn,const xStringPiece &message);
	
	void sendPipe(const xStringPiece &message);
	void sendPipe(xBuffer *message);
	void sendPipe(const void *message,int32_t len);

  	void send(const void *message,int32_t len);
	void send(xBuffer *message);
	void send(const xStringPiece &message);

	bool disconnected() const { return state == kDisconnected; }
	bool connected() { return state == kConnected; }
	void forceCloseInLoop();
	void connectEstablished();
	void forceCloseWithDelay(double seconds);
	void forceCloseDelay();

	void connectDestroyed();
	void shutdown();
	void shutdownInLoop();
	void forceClose();
	
	void handleRead();
	void handleWrite();
	void handleClose();
	void handleError();

	void startReadInLoop();
	void stopReadInLoop();

	void startRead();
	void stopRead();

	std::any *getMutableContext() { return &context; }
	const std::any &getContext() const { return context; }
	void setContext(const std::any &context) { this->context = context; }

	xBuffer *outputBuffer() { return &sendBuff; }
	xBuffer *intputBuffer() { return &recvBuff; }

	const std::string &getip() const { return ip; }
	int16_t &getport() { return port; }

private:
	xEventLoop *loop;
	int32_t sockfd;
	bool reading;

	xBuffer recvBuff;
	xBuffer sendBuff;
	ConnectionCallback connectionCallback;
	MessageCallback messageCallback;
	WriteCompleteCallback writeCompleteCallback;
	HighWaterMarkCallback highWaterMarkCallback;
	CloseCallback closeCallback;

	size_t highWaterMark;
	StateE state;
	ChannelPtr channel;
	std::any context;
	std::string ip;
	int16_t  port;
};
