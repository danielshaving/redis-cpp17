
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
	resp->setBody(req.parseString);
}

void onMessage(const xHttpRequest &req,xHttpResponse *resp)
{
	SHA1_CTX ctx;
	unsigned char hash[20];
	SHA1Init(&ctx);
	SHA1Update(&ctx, (const unsigned char*)req.getSecKey().c_str(), req.getSecKey().size());
	SHA1Final(hash, &ctx);

	std::string base64Str = base64_encode((const unsigned char *)hash, sizeof(hash));
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




















