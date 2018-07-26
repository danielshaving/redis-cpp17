#include "log.h"
#include "rpc.pb.h"
#include <google/protobuf/descriptor.h>
#include <rpcchannel.h>
const char rpctag [] = "RPC0";

RpcChannel::RpcChannel()
: codec(std::bind(&RpcChannel::onRpcMessage,this,std::placeholders::_1,std::placeholders::_2)),
  services(nullptr)
{
	LOG_INFO << "RpcChannel::ctor - " << this;
}

RpcChannel::RpcChannel(const TcpConnectionPtr &conn)
: codec(std::bind(&RpcChannel::onRpcMessage,this,std::placeholders::_1,std::placeholders::_2)),
  conn(conn),
  services(nullptr)
{
	LOG_INFO << "RpcChannel::ctor - " << this;
}


RpcChannel::~RpcChannel()
{
	LOG_INFO << "RpcChannel::dtor - " << this;
	for (auto it = outstandings.begin(); it != outstandings.end(); ++it)
	{
		OutstandingCall out = it->second;
		delete out.response;
		delete out.done;
	}
}


void RpcChannel::CallMethod(const ::google::protobuf::MethodDescriptor *method,
                            ::google::protobuf::RpcController *controller,
                            const ::google::protobuf::Message *request,
                            ::google::protobuf::Message *response,
                            ::google::protobuf::Closure *done)
{
	RpcMessage message;
	message.set_type(REQUEST);
	int64_t id = ++id;
	message.set_id(id);
	message.set_service(method->service()->full_name());
	message.set_method(method->name());
	message.set_request(request->SerializeAsString()); // FIXME: error check

	OutstandingCall out = { response,done };
	{
		std::unique_lock<std::mutex> lk(mtx);
		outstandings[id] = out;
	}
	codec.send(conn, message);
}

void RpcChannel::onMessage(const TcpConnectionPtr &conn,Buffer *buf)
{
	codec.onMessage(conn,buf);
}

void RpcChannel::onRpcMessage(const TcpConnectionPtr &conn,const RpcMessagePtr &messagePtr)
{
	assert(this->conn == conn);
	//printf("%s\n", message.DebugString().c_str());
	RpcMessage &message = *messagePtr;
	if (message.type() == RESPONSE)
	{
		int64_t id = message.id();
		//assert(message.has_response() || message.has_error());

		OutstandingCall out = { nullptr, nullptr };

		{
			std::unique_lock<std::mutex> lk(mtx);
			auto  it = outstandings.find(id);
			if (it != outstandings.end())
			{
				out = it->second;
				outstandings.erase(it);
			}
		}

		if (out.response)
		{
			  std::unique_ptr<::google::protobuf::Message> d(out.response);
			  out.response->ParseFromString(message.response());

			  if (out.done)
			  {
				  out.done->Run();
			  }
		}
	}
	else if (message.type() == REQUEST)
	{
		// FIXME: extract to a function
		ErrorCode error = WRONG_PROTO;
		if (services)
		{
			auto it = services->find(message.service());
			if (it != services->end())
			{
				::google::protobuf::Service *service = it->second;
				assert(service != nullptr);
				const google::protobuf::ServiceDescriptor *desc = service->GetDescriptor();
				const google::protobuf::MethodDescriptor *method = desc->FindMethodByName(message.method());
				if (method)
				{
					std::unique_ptr<google::protobuf::Message> request(service->GetRequestPrototype(method).New());
					if (request->ParseFromString(message.request()))
					{
						::google::protobuf::Message *response = service->GetResponsePrototype(method).New();
						// response is deleted in doneCallback
						int64_t id = message.id();
						service->CallMethod(method,nullptr,request.get(),response,
											NewCallback(this,&RpcChannel::doneCallback,response,id));
						error = NO_ERROR;
					}
					else
					{
						error = INVALID_REQUEST;
					}
				}
				else
				{
					error = NO_METHOD;
				}
			}
			else
			{
				error = NO_SERVICE;
			}
		}
		else
		{
			error = NO_SERVICE;
		}

		if (error != NO_ERROR)
		{
			RpcMessage response;
			response.set_type(RESPONSE);
			response.set_id(message.id());
			response.set_error(error);
			codec.send(conn, response);
		}
	}
	else if (message.type() == ERROR)
	{

	}
}

void RpcChannel::doneCallback(::google::protobuf::Message *response,int64_t id)
{
	std::unique_ptr<google::protobuf::Message> d(response);
	RpcMessage message;
	message.set_type(RESPONSE);
	message.set_id(id);
	message.set_response(response->SerializeAsString()); // FIXME: error check
	codec.send(conn, message);
}





