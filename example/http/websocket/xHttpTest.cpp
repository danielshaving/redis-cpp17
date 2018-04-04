#include "all.h"
#include "xHttpServer.h"

xAsyncLogging *g_asyncLog;
void asyncOutput(const char *msg, int len)
{
	printf("%s\n",msg);
	g_asyncLog->append(msg, len);
}

void onConnection(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		xHttpContext context;
		conn->setContext(context);
		LOG_INFO<<"onConnection";
	}
	else
	{
		LOG_INFO<<"disonConnection";
	}
}

void onMessage(xHttpRequest &rep,const TcpConnectionPtr &conn)
{
	auto context = std::any_cast<xHttpContext>(conn->getContext());
	xBuffer sendBuf;
	sendBuf.append(rep.getParseString().c_str(),rep.getParseString().size());
	context->wsFrameBuild(sendBuf,xHttpRequest::BINARY_FRAME,true,false);
	conn->send(&sendBuf);
}

int main(int argc, char* argv[])
{
	xLogger::setOutput(asyncOutput);
	xAsyncLogging log("http", 4096);
	log.start();
	g_asyncLog = &log;

	if(argc  !=  4)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <thread>\n");
		exit(1);
	}

	const char *ip = argv[1];
	uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
	int16_t threadNum = static_cast<int16_t>(atoi(argv[3]));

	xEventLoop loop;
	xHttpServer server(&loop,ip,port);
	server.setThreadNum(threadNum);
	server.setMessageCallback(onMessage);
	server.setConnCallback(onConnection);
	server.start();
	loop.run();

	return 0;
}



















