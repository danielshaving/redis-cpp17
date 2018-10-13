#include <google/protobuf/service.h>
#include "sudoku.pb.h"
#include "log.h"
#include "eventloop.h"
#include "rpcserver.h"

class SudokuServiceImpl : public SudokuService
{
public:
	virtual void Solve(::google::protobuf::RpcController *controller,
                       const ::SudokuRequest *request,
                       ::SudokuResponse *response,
                       ::google::protobuf::Closure *done)
	{
		LOG_INFO << "SudokuServiceImpl::Solve";
		response->set_solved(true);
		response->set_checkerboard("1234567");
		done->Run();
	}
};

int main(int argc,char *argv[])
{
	if(argc < 3)
	{
		fprintf(stderr,"Usage: server <host_ip> <port>\n");
		return 0;
	}

	const char *ip =  argv[1];
	uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
	EventLoop loop;
	RpcServer server(&loop,ip,port);
	SudokuServiceImpl impl;
	server.registerService(&impl);
	server.start();
	loop.run();
	google::protobuf::ShutdownProtobufLibrary();
	return 0;
}
