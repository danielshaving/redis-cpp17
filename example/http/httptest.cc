#include "httpcontext.h"
#include "httpresponse.h"
#include "httpserver.h"
#include "all.h"

char favicon[555];
bool benchmark = false;
void onMessage(const HttpRequest &req, HttpResponse *resp)
{
	std::cout << "Headers " << req.methodString() << " " << req.getPath() << std::endl;
	if (!benchmark)
	{
		auto &headers = req.getHeaders();
		for (auto it = headers.begin(); it != headers.end(); ++it)
		{
			//std::cout << it->first << ": " << it->second << std::endl;
		}
	}

	//std::string getContext = req.getQuery();

	if (req.getMethod() == HttpRequest::kGet)
	{
		if (req.getPath() == "/")
		{
			printf("");
			resp->setStatusCode(HttpResponse::k2000k);
			resp->setStatusMessage("OK");
			resp->setContentType("text/html");
			resp->addHeader("Server", "xHttp");
			std::string now = TimeStamp::now().toFormattedString();
			resp->setBody("<html><head><title>This is title</title></head>"
				"<body><h1>Hello</h1>Now is " + now +
				"</body></html>");
		}
		else if (req.getPath() == "/favicon.ico")
		{
			resp->setStatusCode(HttpResponse::k2000k);
			resp->setStatusMessage("OK");
			resp->setContentType("image/png");
			resp->setBody(std::string(favicon, sizeof favicon));
		}
		else if (req.getPath() == "/hello")
		{
			resp->setStatusCode(HttpResponse::k2000k);
			resp->setStatusMessage("OK");
			resp->setContentType("text/plain");
			resp->addHeader("Server", "xHttp");
			resp->setBody("hello, world!\n");
		}
		else if (req.getPath() == "/test.txt")
		{
			std::string filename = "attachment;filename=";
			filename += "test.txt";
			resp->setStatusCode(HttpResponse::k2000k);
			resp->setStatusMessage("OK");
			resp->setContentType("text/plain");
			resp->addHeader("Content-Disposition", filename);
			resp->setBody("test!\n");
		}
		else
		{
			resp->setStatusCode(HttpResponse::k404NotFound);
			resp->setStatusMessage("Not Found");
			resp->setCloseConnection(true);
		}
	}
	else if (req.getMethod() == HttpRequest::kPost)
	{
		resp->setStatusCode(HttpResponse::k2000k);
		resp->setStatusMessage("OK");
	}
}

int main(int argc, char* argv[])
{
	if (argc != 4)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <thread>\n");
		exit(1);
	}

	const char *ip = argv[1];
	uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
	int16_t threadNum = static_cast<int16_t>(atoi(argv[3]));

	EventLoop loop;
	HttpServer server(&loop, ip, port);
	server.setThreadNum(threadNum);
	server.setMessageCallback(onMessage);
	server.start();
	loop.run();
	return 0;
}




















