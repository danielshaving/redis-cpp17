#pragma once
#include "all.h"
#include "channel.h"
#include "eventloop.h"
#include "socket.h"

class Acceptor
{
public:
	typedef std::function<void(int32_t)> NewConnectionCallback;
	Acceptor(EventLoop *loop, const char *ip, int16_t port);
	~Acceptor();

	void setNewConnectionCallback(const NewConnectionCallback &&cb)
	{
		newConnectionCallback = std::move(cb);
	}

	bool getlistenning() const
	{
		return listenning;
	}

	void listen();
	void handleRead();

private:
	Acceptor(const Acceptor&);
	void operator=(const Acceptor&);

	EventLoop *loop;
	Channel channel;
	int32_t sockfd;

	NewConnectionCallback newConnectionCallback;
	bool listenning;
	bool idleFd;
};
