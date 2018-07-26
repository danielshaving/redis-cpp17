#include "sudoku.pb.h"
#include "tcpconnection.h"
#include "tcpclient.h"
#include "rpcchannel.h"
#include "log.h"


class RpcClient
{
public:
	RpcClient(EventLoop* loop,const char *ip,uint16_t port)
	:loop(loop),
	ip(ip),
	port(port),
	client(loop,nullptr),
	channel(new RpcChannel),
	stub(channel.get())
	{
		client.setConnectionCallback(std::bind(&RpcClient::onConnection,
											   this,std::placeholders::_1));
		client.setMessageCallback(std::bind(&xRpcChannel::onMessage,
											channel.get(),std::placeholders::_1,std::placeholders::_2));
	}

	void connect()
	{
		client.connect(ip,port);
	}

	private:
	void onConnection(const TcpConnectionPtr &conn)
	{
		if (conn->connected())
		{
			channel->setConnection(conn);
			SudokuRequest request;
			request.set_checkerboard("001010");
			SudokuResponse *response = new SudokuResponse;
			stub.Solve(nullptr,&request,response,NewCallback(this,&RpcClient::solved,response));
		}
		else
		{
			loop->quit();
		}
	}

	void solved(SudokuResponse *resp)
	{
		std::cout << "solved:" << resp->DebugString().c_str();
		client.disconnect();
	}

	EventLoop* loop;
	const char *ip;
	uint16_t port;
	TcpClient client;
	RpcChannelPtr channel;
	SudokuService::Stub stub;
};

int main(int argc, char* argv[])
{
	if (argc > 2)
	{
		const char *ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		EventLoop loop;
		RpcClient rpcClient(&loop,ip,port);
		rpcClient.connect();
		loop.run();
	}
	else
	{
		fprintf(stderr,"Usage: server <host_ip> <port>\n");
	}

	google::protobuf::ShutdownProtobufLibrary();
}

