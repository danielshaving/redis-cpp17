#include "xRpcServer.h"
#include "xLog.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>


xRpcServer::xRpcServer(xEventLoop *loop,const char *ip,uint16_t port)
:server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&xRpcServer::connectCallback,this,std::placeholders::_1));
}

void xRpcServer::registerService(::google::protobuf::Service* service)
{
	const ::google::protobuf::ServiceDescriptor* desc = service->GetDescriptor();
	services[desc->full_name()] = service;
}

void xRpcServer::start()
{
	server.start();
}

void xRpcServer::connectCallback(const xTcpconnectionPtr& conn)
{
	if (conn->connected())
	{
		RpcChannelPtr channel(new xRpcChannel(conn));
		channel->setServices(&services);
		conn->setMessageCallback(
			std::bind(&xRpcChannel::onMessage, channel.get(), std::placeholders::_1,std::placeholders::_2));
		rpcMaps.insert(std::make_pair(conn->getSockfd(),channel));
	}
	else
	{
		rpcMaps.erase(conn->getSockfd());
	}
}
