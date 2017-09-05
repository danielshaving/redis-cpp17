#pragma once
#include "xSocket.h"
#include "xLog.h"
#include "xTcpconnection.h"
#include "xTcpServer.h"
#include "xTcpClient.h"
#include "xTimer.h"




const size_t frameLen = 2*sizeof(int64_t);

void serverConnectionCallback(const xTcpconnectionPtr& conn, void *data)
{
 
	if (conn->connected())
	{
		LOG_INFO<<" from client connect ";
		xSocket socket;
		socket.setTcpNoDelay(conn->getSockfd(),false);
		
	}
	else
	{
		LOG_INFO<<" from client disconnect ";
	}
}

void serverMessageCallback(const xTcpconnectionPtr& conn,
                           xBuffer* buffer,
                          void * data)
{
	int64_t message[2];
	while (buffer->readableBytes() >= frameLen)
	{
		memcpy(message, buffer->peek(), frameLen);
		buffer->retrieve(frameLen);
		message[1] = xTimestamp::now().getMicroSecondsSinceEpoch();
		conn->send(message, sizeof message);
	}
}



void runServer(uint16_t port)
{
	xEventLoop loop;
	xTcpServer server;
	server.init(&loop,"127.0.0.1",port,nullptr);
	server.setConnectionCallback(serverConnectionCallback);
	server.setMessageCallback(serverMessageCallback);
	server.start();
	loop.run();
}



xTcpconnectionPtr clientConnection;


void clientConnectionErrorCallback()
{
	
}



void clientConnectionCallback(const xTcpconnectionPtr& conn ,void * data)
{
	if (conn->connected())
	{
		LOG_INFO<<"  client connect ";
		clientConnection = conn;
		xSocket socket;
		socket.setSocketBlock(conn->getSockfd());
		socket.setTcpNoDelay(conn->getSockfd(),false);
	}
	else
	{
		LOG_INFO<<"  client disconnect ";
		clientConnection.reset();
	}
}

void clientMessageCallback(const xTcpconnectionPtr& conn,
                           xBuffer* buffer,
                           void * data)
{
	int64_t message[2];
	while (buffer->readableBytes() >= frameLen)
	{
		memcpy(message, buffer->peek(), frameLen);
		buffer->retrieve(frameLen);
		int64_t send = message[0];
		int64_t their = message[1];
		int64_t back = xTimestamp::now().getMicroSecondsSinceEpoch();
		int64_t mine = (back+send)/2;
		LOG_INFO << "round trip " << back - send
		         << " clock error " << their - mine;
	}
}



void sendMyTime(void * data)
{
	if (clientConnection)
	{
		int64_t message[2] = { 0, 0 };
		message[0] = xTimestamp::now().getMicroSecondsSinceEpoch();
		clientConnection->send(message, sizeof message);
	}
}




void runClient(const char* ip, uint16_t port)
{
  xEventLoop loop;
  xTcpClient client(&loop,nullptr);
  client.setConnectionCallback(clientConnectionCallback);
  client.setMessageCallback(clientMessageCallback);
  client.connect(ip,port);
  loop.runAfter(0.2,nullptr,true,sendMyTime);
  loop.run();
}



int main(int argc, char* argv[])
{
	if (argc > 2)
	{
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		 if (strcmp(argv[1], "-s") == 0)
		{
		  	runServer(port);
		}
		else
		{
		 	runClient(argv[1], port);
		}
	}
	else
	{
		  printf("Usage:\n%s -s port\n%s ip port\n", argv[0], argv[0]);
	}

	return 0;
}


