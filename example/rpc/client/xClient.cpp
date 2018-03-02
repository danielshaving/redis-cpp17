#include "sudoku.pb.h"
#include "xTcpconnection.h"
#include "xTcpClient.h"
#include "xRpcChannel.h"
#include "xLog.h"


class xRpcClient : noncopyable
{
public:
	xRpcClient(xEventLoop* loop, const char *ip,uint16_t port)
	:loop(loop),
	ip(ip),
	port(port),
	client(loop,nullptr),
	channel(new xRpcChannel),
	stub(channel.get())
	{
		client.setConnectionCallback(std::bind(&xRpcClient::onConnection, this,std::placeholders::_1));
		client.setMessageCallback(std::bind(&xRpcChannel::onMessage, channel.get(),std::placeholders::_1,std::placeholders::_2));
	}

	void connect()
	{
		client.connect(ip,port);
	}

	private:
	void onConnection(const TcpConnectionPtr& conn)
	{
		if (conn->connected())
		{
			channel->setConnection(conn);
			SudokuRequest request;
			request.set_checkerboard("001010");
			SudokuResponse* response = new SudokuResponse;
			stub.Solve(nullptr, &request, response, NewCallback(this, &xRpcClient::solved, response));
		}
		else
		{
			loop->quit();
		}
	}

	void solved(SudokuResponse* resp)
	{
		std::cout << "solved:" << resp->DebugString().c_str();
		client.disconnect();
	}

	xEventLoop* loop;
	const char *ip;
	uint16_t port;
	xTcpClient client;
	RpcChannelPtr channel;
	SudokuService::Stub stub;
};

int main(int argc, char* argv[])
{
	if (argc > 2)
	{
		const char* ip =  argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		xEventLoop loop;
		xRpcClient rpcClient(&loop, ip,port);
		rpcClient.connect();
		loop.run();
	}
	else
	{
		fprintf(stderr, "Usage: server <host_ip> <port>\n");
	}
	google::protobuf::ShutdownProtobufLibrary();
}

