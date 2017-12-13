
#include "all.h"
#include "xHttpServer.h"
#include "xHttpContext.h"
#include "xHttpResponse.h"
#include "xTcpconnection.h"


char favicon[555];
bool benchmark = false;

xAsyncLogging *g_asyncLog = nullptr;
void asyncOutput(const char* msg, int len)
{
	printf("%s\n",msg);
	g_asyncLog->append(msg, len);
}


void onConnection(const xTcpconnectionPtr& conn,void * data)
{
	if (conn->connected())
	{
		LOG_INFO<<"client connect success";
	}
	else
	{
		LOG_INFO<<"client disconnect ";
	}
}

void onMessage(const xTcpconnectionPtr& conn, xBuffer* buf, void * data)
{
	xHttpContext  context;
	xHttpRequest  &req = context.getRequest();
	xHttpResponse resp(true);
	xBuffer sendBuf;
	if(!context.parseRequest(buf))
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->shutdown();
		return ;
	}

	std::cout << "Headers " << req.methodString() << " " << req.getPath() << std::endl;
	if (!benchmark)
	{
		const std::map<std::string, std::string>& headers = req.getHeaders();
		for (auto  it = headers.begin();it != headers.end(); ++it)
		{
			std::cout << it->first << ": " << it->second << std::endl;
		}
	}

	if(req.getMethod() == xHttpRequest::kGet)
	{
		//std::string getContext = req.getQuery();
		if (req.getPath() == "/")
		{
			resp.setStatusCode(xHttpResponse::k2000k);
			resp.setStatusMessage("OK");
			resp.setContentType("text/html");
			resp.addHeader("Server", "xHttp");
			string now = xTimestamp::now().toFormattedString();
			resp.setBody("<html><head><title>This is title</title></head>"
				"<body><h1>Hello</h1>Now is " + now +
				"</body></html>");
		}
		else if (req.getPath() == "/favicon.ico")
		{
			resp.setStatusCode(xHttpResponse::k2000k);
			resp.setStatusMessage("OK");
			resp.setContentType("image/png");
			resp.setBody(string(favicon, sizeof favicon));
		}
		else if (req.getPath() == "/hello")
		{
			resp.setStatusCode(xHttpResponse::k2000k);
			resp.setStatusMessage("OK");
			resp.setContentType("text/plain");
			resp.addHeader("Server", "xHttp");
			resp.setBody("hello, world!\n");
		}
		else
		{
			resp.setStatusCode(xHttpResponse::k404NotFound);
			resp.setStatusMessage("Not Found");
			resp.setCloseConnection(true);
		}

		resp.appendToBuffer(&sendBuf);
		conn->send(&sendBuf);

	}
	else if(req.getMethod() == xHttpRequest::kPost)
	{
		std::cout<<"post body:"<<std::string(buf->peek(),buf->readableBytes())<<std::endl;
		resp.setStatusCode(xHttpResponse::k2000k);
		resp.setStatusMessage("OK");
		resp.appendToBuffer(&sendBuf);
		conn->send(&sendBuf);
	}


	conn->shutdown();
	buf->retrieveAll();

}





int main(int argc, char* argv[])
{
	xLogger::setOutput(asyncOutput);
	xAsyncLogging log("http", 4096);
	log.start();
	g_asyncLog = &log;

	if(argc  !=  3)
	{
		fprintf(stderr, "Usage: client <host_ip> <port>\n");
		exit(1);
	}

	const char* ip;
	uint16_t port;
	ip = argv[1];
	port = static_cast<uint16_t>(atoi(argv[2]));

	xEventLoop loop;
	xTcpServer server;
	signal(SIGPIPE, SIG_IGN);
	server.init(&loop,ip,port,nullptr);
	server.setConnectionCallback(onConnection);
	server.setMessageCallback(onMessage);
	server.start();
	loop.run();

	return 0;
}




















