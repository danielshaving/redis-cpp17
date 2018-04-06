#include "xAll.h"
#include "xHttpServer.h"
#include "xHttpContext.h"
#include "xHttpResponse.h"

char favicon[555];
bool benchmark = false;

xAsyncLogging *g_asyncLog = nullptr;
void asyncOutput(const char* msg, int len)
{
	printf("%s\n",msg);
	g_asyncLog->append(msg, len);
}


void onMessage(const xHttpRequest & req,xHttpResponse* resp)
{
	std::cout << "Headers " << req.methodString() << " " << req.getPath() << std::endl;
	if (!benchmark)
	{
		auto &headers = req.getHeaders();
		for (auto  it = headers.begin();it != headers.end(); ++it)
		{
			std::cout << it->first << ": " << it->second << std::endl;
		}
	}

	//std::string getContext = req.getQuery();

	if(req.getMethod() == xHttpRequest::kGet)
	{
		if (req.getPath() == "/")
		{
			resp->setStatusCode(xHttpResponse::k2000k);
			resp->setStatusMessage("OK");
			resp->setContentType("text/html");
			resp->addHeader("Server", "xHttp");
			std::string now = xTimestamp::now().toFormattedString();
			resp->setBody("<html><head><title>This is title</title></head>"
				"<body><h1>Hello</h1>Now is " + now +
				"</body></html>");
		}
		else if (req.getPath() == "/favicon.ico")
		{
			resp->setStatusCode(xHttpResponse::k2000k);
			resp->setStatusMessage("OK");
			resp->setContentType("image/png");
			resp->setBody(std::string(favicon, sizeof favicon));
		}
		else if (req.getPath() == "/hello")
		{
			resp->setStatusCode(xHttpResponse::k2000k);
			resp->setStatusMessage("OK");
			resp->setContentType("text/plain");
			resp->addHeader("Server", "xHttp");
			resp->setBody("hello, world!\n");
		}
		else
		{
			resp->setStatusCode(xHttpResponse::k404NotFound);
			resp->setStatusMessage("Not Found");
			resp->setCloseConnection(true);
		}
	}
	else if(req.getMethod() == xHttpRequest::kPost)
	{
		resp->setStatusCode(xHttpResponse::k2000k);
		resp->setStatusMessage("OK");
	}

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

	const char* ip = argv[1];
	uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
	int16_t threadNum = static_cast<int16_t>(atoi(argv[3]));

	xEventLoop loop;
	xHttpServer server(&loop,ip,port);
	server.setThreadNum(threadNum);
	server.setMessageCallback(onMessage);
	server.start();
	loop.run();

	return 0;
}




















