#include "xRpcChannel.h"
#include "xLog.h"
#include  "rpc.pb.h"
#include <google/protobuf/descriptor.h>
const char rpctag [] = "RPC0";



xRpcChannel::xRpcChannel()
: codec(std::bind(&xRpcChannel::onRpcMessage, this, std::placeholders::_1,std::placeholders::_2)),
  services(nullptr)
{
	LOG_INFO << "RpcChannel::ctor - " << this;
}


xRpcChannel::xRpcChannel(const xTcpconnectionPtr& conn)
: codec(std::bind(&xRpcChannel::onRpcMessage, this, std::placeholders::_1,std::placeholders::_2)),
  conn(conn),
  services(nullptr)
{
	LOG_INFO << "RpcChannel::ctor - " << this;
}


xRpcChannel::~xRpcChannel()
{
	LOG_INFO << "RpcChannel::dtor - " << this;
	for (auto it = outstandings.begin(); it != outstandings.end(); ++it)
	{
		OutstandingCall out = it->second;
		delete out.response;
		delete out.done;
	}
}


void xRpcChannel::CallMethod(const ::google::protobuf::MethodDescriptor* method,
                            ::google::protobuf::RpcController* controller,
                            const ::google::protobuf::Message* request,
                            ::google::protobuf::Message* response,
                            ::google::protobuf::Closure* done)
{
	RpcMessage message;
	message.set_type(REQUEST);
	int64_t id = ++id;
	message.set_id(id);
	message.set_service(method->service()->full_name());
	message.set_method(method->name());
	message.set_request(request->SerializeAsString()); // FIXME: error check

	OutstandingCall out = { response, done };
	{
		std::unique_lock<std::mutex> lk(mtx);
		outstandings[id] = out;
	}
	codec.send(conn, message);
}


void xRpcChannel::onMessage(const xTcpconnectionPtr& conn, xBuffer* buf)
{
	codec.onMessage(conn, buf);
}

void xRpcChannel::onRpcMessage(const xTcpconnectionPtr& conn,const RpcMessagePtr& messagePtr)
{
	assert(this->conn == conn);
	//printf("%s\n", message.DebugString().c_str());
	RpcMessage& message = *messagePtr;
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
			  //if (message.has_response())
			 // {
			  out.response->ParseFromString(message.response());
			 // }
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
			auto  it = services->find(message.service());
			if (it != services->end())
			{
				::google::protobuf::Service* service = it->second;
				assert(service != nullptr);
				const google::protobuf::ServiceDescriptor* desc = service->GetDescriptor();
				const google::protobuf::MethodDescriptor* method = desc->FindMethodByName(message.method());
				if (method)
				{
					std::unique_ptr<google::protobuf::Message> request(service->GetRequestPrototype(method).New());
					if (request->ParseFromString(message.request()))
					{
						::google::protobuf::Message* response = service->GetResponsePrototype(method).New();
						// response is deleted in doneCallback
						int64_t id = message.id();
						service->CallMethod(method, nullptr, request.get(), response,
											NewCallback(this, &xRpcChannel::doneCallback, response, id));
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

void xRpcChannel::doneCallback(::google::protobuf::Message* response, int64_t id)
{
	std::unique_ptr<google::protobuf::Message> d(response);
	RpcMessage message;
	message.set_type(RESPONSE);
	message.set_id(id);
	message.set_response(response->SerializeAsString()); // FIXME: error check
	codec.send(conn, message);
}





