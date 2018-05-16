#include "log.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <rcpserver.h>

RpcServer::RpcServer(EventLoop *loop,const char *ip,uint16_t port)
:server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&RpcServer::connectCallback,this,std::placeholders::_1));
}

void RpcServer::registerService(::google::protobuf::Service *service)
{
	const ::google::protobuf::ServiceDescriptor *desc = service->GetDescriptor();
	services[desc->full_name()] = service;
}

void RpcServer::start()
{
	server.start();
}

void RpcServer::connectCallback(const TcpConnectionPtr& conn)
{
	if (conn->connected())
	{
		RpcChannelPtr channel(new RpcChannel(conn));
		channel->setServices(&services);
		conn->setMessageCallback(
			std::bind(&RpcChannel::onMessage,channel.get(),std::placeholders::_1,std::placeholders::_2));
		rpcMaps.insert(std::make_pair(conn->getSockfd(),channel));
	}
	else
	{
		rpcMaps.erase(conn->getSockfd());
	}
}
