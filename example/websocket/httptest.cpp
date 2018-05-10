#include "httpserver.h"

void onConnection(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		HttpContext context;
		conn->setContext(context);
	}
	else
	{

	}
}

void onMessage(HttpRequest &rep,const TcpConnectionPtr &conn)
{
	auto context = std::any_cast<HttpContext>(conn->getMutableContext());
	Buffer sendBuf;
	sendBuf.append(rep.getParseString().c_str(),rep.getParseString().size());
	context->wsFrameBuild(sendBuf,HttpRequest::BINARY_FRAME,true,false);
	conn->send(&sendBuf);
}

int main(int argc, char* argv[])
{
	if(argc  !=  4)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <thread>\n");
		exit(1);
	}

	const char *ip = argv[1];
	uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
	int16_t threadNum = static_cast<int16_t>(atoi(argv[3]));

	EventLoop loop;
	HttpServer server(&loop,ip,port);
	server.setThreadNum(threadNum);
	server.setMessageCallback(onMessage);
	server.setConnCallback(onConnection);
	server.start();
	loop.run();

	return 0;
}



















