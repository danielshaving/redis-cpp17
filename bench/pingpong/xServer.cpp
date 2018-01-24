#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xTcpServer.h"
#include "xEventLoop.h"
#include "xLog.h"

void onConnection(const xTcpconnectionPtr& conn,void * data)
{
	if (conn->connected())
	{
		
	}
}

void onMessage(const xTcpconnectionPtr& conn, xBuffer* buf, void * data)
{
 	conn->send(buf);
}


int main(int argc, char* argv[])
{
	if (argc < 4)
	{
		fprintf(stderr, "Usage: server <address> <port> <threads>\n");
	}
	else
	{
		LOG_INFO << "ping pong server pid = " << getpid();

		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int threadCount = atoi(argv[3]);

		xEventLoop loop;

		xTcpServer server;
		server.init(&loop, ip,port,nullptr);
		server.setConnectionCallback(onConnection);
		server.setMessageCallback(onMessage);

		if (threadCount > 1)
		{
		     server.setThreadNum(threadCount);
		}

		server.start();

		loop.run();
	}
}



