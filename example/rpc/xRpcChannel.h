#pragma once

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include "xProtobufCodecLite.h"
#include "xGoogleIn.h"
#include "rpc.pb.h"

class RpcMessage;
typedef std::shared_ptr<RpcMessage> RpcMessagePtr;
extern const char rpctag[];// = "RPC0";
typedef xProtobufCodecLiteT<RpcMessage, rpctag> RpcCodec;

namespace google
{
	namespace protobuf
	{
		// Defined in other files.
		class Descriptor;            // descriptor.h
		class ServiceDescriptor;     // descriptor.h
		class MethodDescriptor;      // descriptor.h
		class Message;               // message.h
		class Closure;
		class RpcController;
		class Service;
	}
}


class xRpcChannel:public ::google::protobuf::RpcChannel
{
public:
	xRpcChannel();
	explicit xRpcChannel(const xTcpconnectionPtr& conn);
	~xRpcChannel();

	void setConnection(const xTcpconnectionPtr & conn){ this->conn = conn; }
	void setServices(const std::unordered_map<std::string,::google::protobuf::Service*> *services) { this->services = services; }

	void CallMethod(const ::google::protobuf::MethodDescriptor* method,
				  ::google::protobuf::RpcController* controller,
				  const ::google::protobuf::Message* request,
				  ::google::protobuf::Message* response,
				  ::google::protobuf::Closure* done);

	void onMessage(const xTcpconnectionPtr& conn,xBuffer* buf);

private:
	void onRpcMessage(const xTcpconnectionPtr& conn, const RpcMessagePtr& messagePtr);
	void doneCallback(::google::protobuf::Message* response, int64_t id);

	struct OutstandingCall
	{
		::google::protobuf::Message* response;
		::google::protobuf::Closure* done;
	};

	RpcCodec codec;
	xTcpconnectionPtr conn;
	std::atomic<int64_t> id;
	std::mutex mtx;
	std::unordered_map<int64_t, OutstandingCall> outstandings;
	const std::unordered_map<std::string,::google::protobuf::Service*> *services;
};

typedef std::shared_ptr<xRpcChannel> RpcChannelPtr;



