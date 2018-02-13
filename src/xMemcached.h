#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xTcpServer.h"
#include "xZmalloc.h"
#include "xObject.h"


class xMemcacheServer : noncopyable
{
public:
	struct Options
	{
		uint16_t port;
		std::string ip;
	};

	xMemcacheServer(xEventLoop *loop,const Options & op);
	~xMemcacheServer();
};