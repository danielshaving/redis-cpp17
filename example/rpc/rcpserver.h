#pragma once
#include "tcpserver.h"
#include "tcpconnection.h"
#include "rpcchannel.h"

namespace google
{
	namespace protobuf
	{
		class Service;
	}
}

class RpcServer
{
public:
	RpcServer(EventLoop *loop,const char *ip,uint16_t port);
	void setThreadNum(int numThreads)
	{
		server.setThreadNum(numThreads);
	}

	void registerService(::google::protobuf::Service*);
	void start();

private:
	void connectCallback(const TcpConnectionPtr & conn);
	std::unordered_map<int32_t,RpcChannelPtr> rpcMaps;
	std::unordered_map<std::string,::google::protobuf::Service*> services;
	TcpServer server;
};
