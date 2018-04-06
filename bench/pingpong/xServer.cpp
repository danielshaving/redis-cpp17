#pragma once
#include "xAll.h"
#include "xTcpConnection.h"
#include "xTcpServer.h"
#include "xEventLoop.h"
#include "xLog.h"

void onConnection(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{

	}
	else
	{

	}
}

void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	conn->send(buffer);
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

		xTcpServer server(&loop, ip,port,nullptr);
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



