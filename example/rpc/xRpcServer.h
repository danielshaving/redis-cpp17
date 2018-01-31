#pragma once
#include "xTcpServer.h"
#include "xTcpconnection.h"
#include "xRpcChannel.h"

namespace google
{
	namespace protobuf
	{
		class Service;
	}
}

class xRpcServer
{
public:
	xRpcServer(xEventLoop *loop,const char *ip,uint16_t port);
	void setThreadNum(int numThreads)
	{
		server.setThreadNum(numThreads);
	}

	void registerService(::google::protobuf::Service*);
	void start();

private:
	void connectCallback(const xTcpconnectionPtr & conn);
	std::unordered_map<int32_t,RpcChannelPtr> rpcMaps;
	std::unordered_map<std::string,::google::protobuf::Service*> services;
	xTcpServer server;
};
