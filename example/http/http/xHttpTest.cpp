
#include "all.h"
#include "xHttpServer.h"
#include "xHttpContext.h"
#include "xHttpResponse.h"
#include "xUtil.h"

char favicon[555];
bool benchmark = false;

xAsyncLogging *g_asyncLog = nullptr;
void asyncOutput(const char* msg, int len)
{
	printf("%s\n",msg);
	g_asyncLog->append(msg, len);
}

void webMessage(const xHttpRequest &req,xHttpResponse *resp)
{
	//LOG_INFO<<"req.parseString:"<<req.parseString;
	resp->setBody(req.parseString);
}

void onMessage(const xHttpRequest &req,xHttpResponse *resp)
{
	//std::cout << "Headers " << req.methodString() << " " << req.getPath() << std::endl;
	std::string secKey;
	if (!benchmark)
	{
		auto &headers = req.getHeaders();
		auto it = headers.find("Sec-WebSocket-Key");
		if(it == headers.end())
		{
			assert(false);
		}
		else
		{
			secKey = it->second;
			secKey += "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
		}

//		for (auto  it = headers.begin(); it != headers.end(); ++it)
//		{
//			std::cout << it->first << ": " << it->second << std::endl;
//		}

	}

	CSHA1 s1;
	s1.Update((unsigned char*)secKey.c_str(), static_cast<unsigned int>(secKey.size()));
	s1.Final();
	unsigned char puDest[20];
	s1.GetHash(puDest);

	std::string base64Str = base64_encode((const unsigned char *)puDest, sizeof(puDest));
	resp->setStatusCode(xHttpResponse::k101k);
	resp->setStatusMessage("Switching Protocols");
	resp->addHeader("Upgrade", "websocket");
	resp->addHeader("Connection", "Upgrade");
	resp->addHeader("Sec-WebSocket-Accept", base64Str);
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
	server.setWebCallback(webMessage);
	server.start();
	loop.run();

	return 0;
}




















