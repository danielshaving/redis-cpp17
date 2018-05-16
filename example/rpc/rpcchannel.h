#pragma once
#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <google.h>
#include <protobufcodeclite.h>
#include "rpc.pb.h"

class RpcMessage;
typedef std::shared_ptr<RpcMessage> RpcMessagePtr;
extern const char rpctag[];// = "RPC0";
typedef ProtobufCodecLiteT<RpcMessage,rpctag> RpcCodec;

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

class RpcChannel:public ::google::protobuf::RpcChannel
{
public:
	RpcChannel();
	explicit RpcChannel(const TcpConnectionPtr &conn);
	~RpcChannel();

	void setConnection(const TcpConnectionPtr &conn) { this->conn = conn; }
	void setServices(const std::unordered_map<std::string,::google::protobuf::Service*> *services)
	{ this->services = services; }

	void CallMethod(const ::google::protobuf::MethodDescriptor *method,
				  ::google::protobuf::RpcController *controller,
				  const ::google::protobuf::Message *request,
				  ::google::protobuf::Message *response,
				  ::google::protobuf::Closure *done);

	void onMessage(const TcpConnectionPtr &conn,Buffer *buf);

private:
	void onRpcMessage(const TcpConnectionPtr &conn,const RpcMessagePtr &messagePtr);
	void doneCallback(::google::protobuf::Message *response,int64_t id);

	struct OutstandingCall
	{
		::google::protobuf::Message *response;
		::google::protobuf::Closure *done;
	};

	RpcCodec codec;
	TcpConnectionPtr conn;
	std::atomic<int64_t> id;
	std::mutex mtx;
	std::unordered_map<int64_t,OutstandingCall> outstandings;
	const std::unordered_map<std::string,::google::protobuf::Service*> *services;
};

typedef std::shared_ptr<RpcChannel> RpcChannelPtr;



