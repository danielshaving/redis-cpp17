#pragma once
#include "all.h"
#include "xChannel.h"
#include "xEventLoop.h"
#include "xSocket.h"

class xAcceptor : noncopyable
{
public:
	typedef std::function<void (int32_t sockfd)> NewConnectionCallback;
	xAcceptor(xEventLoop* loop,std::string ip, int16_t port);
	~xAcceptor();

	void setNewConnectionCallback(const NewConnectionCallback& cb){ newConnectionCallback = cb; }
	bool getlistenning() const { return listenning; }
	void listen();
	void handleRead();

private:
	xEventLoop *loop;
	xSocket socket;
	xChannel channel;
	int32_t listenfd;

	NewConnectionCallback newConnectionCallback;
	bool listenning;
	bool protocol;
};
