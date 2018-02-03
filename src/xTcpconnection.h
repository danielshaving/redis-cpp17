#pragma once

#include "all.h"
#include "xBuffer.h"
#include "xCallback.h"
#include "xChannel.h"

class xEventLoop;
class xTcpconnection:noncopyable,public std::enable_shared_from_this<xTcpconnection>
{
public:
	enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };
	xTcpconnection(xEventLoop *loop,int sockfd,void *data);
	~xTcpconnection();

	xEventLoop	 *getLoop();
	int  getSockfd();


	void setState(StateE s) { state  = s; }

	void setConnectionCallback(const ConnectionCallback& cb)
	{ connectionCallback = cb; }

	void setMessageCallback(const MessageCallback& cb)
	{ messageCallback = cb; }

	void setWriteCompleteCallback(const WriteCompleteCallback& cb)
	{ writeCompleteCallback = cb; }

	void setHighWaterMarkCallback(const HighWaterMarkCallback& cb, size_t highWaterMark)
	{ highWaterMarkCallback = cb; this->highWaterMark  = highWaterMark; }

	void setCloseCallback(const CloseCallback& cb)
	{ closeCallback = cb; }

	void handleRead();
	void handleWrite();
	void handleClose();
	void handleError();

	void sendInLoop(const void* message, size_t len);
	void sendInLoop(const xStringPiece & message);
	void sendPipeInLoop(const void* message, size_t len);
	void sendPipeInLoop(const xStringPiece & message);

	static void bindSendInLoop(xTcpconnection* conn, const xStringPiece& message);
	static void bindSendPipeInLoop(xTcpconnection* conn, const xStringPiece& message);
	
	void sendPipe(const xStringPiece & message);
	void sendPipe(xBuffer* message);
	void sendPipe(const void* message, int len);

  	void send(const void* message, int len);
	void send(xBuffer* message);
	void send(const xStringPiece  &message);


	bool disconnected() const { return state == kDisconnected; }
	bool connected();
	void forceCloseInLoop();
	void connectEstablished();
#ifdef __CXX17__
	const std::any& getContext() const { return context; }
	void setContext(std::any& context) { this->context = context; }
#endif
	void connectDestroyed();
	void setData(void *data) { this->data = data; }
	void * getData()  {return data; }
	void shutdown();
	void shutdownInLoop();
	void forceClose();
	
public:
	xEventLoop  *loop;
	int sockfd;
	xBuffer recvBuff;
	xBuffer sendBuff;
	ConnectionCallback    connectionCallback;
	MessageCallback 	  messageCallback;
	WriteCompleteCallback writeCompleteCallback;
	HighWaterMarkCallback highWaterMarkCallback;
	CloseCallback closeCallback;

	size_t highWaterMark;
	StateE state;
	std::shared_ptr<xChannel> channel;
	void 	*data;
#ifdef __CXX17__
	std::any  context;
#endif
	std::string ip;
	int16_t  port;

};
