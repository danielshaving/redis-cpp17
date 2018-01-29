#include <stdint.h>
#include "zmalloc.h"

#include <brynet/net/SocketLibFunction.h>
#include <brynet/net/EventLoop.h>
#include <brynet/net/WrapTCPService.h>
#include <brynet/net/ListenThread.h>
#include <brynet/net/Socket.h>
#include <brynet/net/DataSocket.h>
#include <brynet/utils/packet.h>

using namespace brynet;
using namespace brynet::net;

struct packet
{
	packet()
	{
		id = -1;
		data = nullptr;
	}

	~packet()
	{
		zfree(data);
	}

	uint32_t id;
	TCPSession::PTR conn;
	char *data;
};
