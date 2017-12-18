
#include "xRedis.h"
#include "xCurrentThread.h"
#include "xLog.h"

xAsyncLogging *g_asyncLog = nullptr;
void asyncOutput(const char* msg, int len)
{
	printf("%s\n",msg);
	g_asyncLog->append(msg, len);
}


char *ascii_logo =
"                _._                                                  \n"
"           _.-``__ ''-._                                             \n"
"      _.-``    `.  `_.  ''-._           xredis 1.0 	beta		  \n"
"  .-`` .-```.  ```\\/    _.,_ ''-._                                   \n"
" (    '      ,       .-`  | `,    )									  \n"
" |`-._`-...-` __...-.``-._|'` _.-'|									  \n"
" |    `-._   `._    /     _.-'    |  								  \n"
"  `-._    `-._  `-./  _.-'    _.-'                                   \n"
" |`-._`-._    `-.__.-'    _.-'_.-'|                                  \n"
" |    `-._`-._        _.-'_.-'    |                   				  \n"
"  `-._    `-._`-.__.-'_.-'    _.-'                                   \n"
" |`-._`-._    `-.__.-'    _.-'_.-'|                                  \n"
" |    `-._`-._        _.-'_.-'    |                                  \n"
"  `-._    `-._`-.__.-'_.-'    _.-'                                   \n"
"      `-._    `-.__.-'    _.-'                                       \n"
"          `-._        _.-'                                           \n"
"              `-.__.-'                                               \n";




//void onConnection(const xTcpconnectionPtr& conn, void * data)
//{
//	if (conn->connected())
//	{
//		std::cout << "connect" << std::endl;
//		std::string test = "GET / HTTP/1.1\r\ncache-control: no-cache\r\nPostman-Token: \
//		c177456b-1017-4e03-a69a-c36523d69809\r\nUser-Agent: PostmanRuntime/6.3.2\r\nAccept:\
//		 */*\r\nHost: 192.168.0.105:8888\r\naccept-encoding: gzip, deflate\r\nConnection: keep-alive\r\n";
//		conn->send(test.c_str(),test.length());
//	}
//	else
//	{
//		std::cout << "disconnect" << std::endl;
//	}
//}
//
//void onMessage(const xTcpconnectionPtr& conn, xBuffer* buf, void * data)
//{
//
//}

int main(int argc, char* argv[])
{
	/*if (argc != 3)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> \n");
		exit(1);
	}
	const char* ip;
	uint16_t port;
	ip = argv[1];
	port = static_cast<uint16_t>(atoi(argv[2]));
	xEventLoop loop;
	xTcpClient client(&loop, nullptr);
	client.setConnectionCallback(std::bind(onConnection, std::placeholders::_1, std::placeholders::_2));
	client.setMessageCallback(std::bind(onMessage, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
	client.connect(ip, port);
	loop.run();*/

	xLogger::setOutput(asyncOutput);
	xAsyncLogging log("xredis", 4096);
	log.start();
	g_asyncLog = &log;
	printf("%s\n", ascii_logo);

	if(argc == 5)
	{
		const char* ip;
		uint16_t port;
		int16_t threadCount;
		bool clusterEnbaled;
		ip = argv[1];
		port = static_cast<uint16_t>(atoi(argv[2]));
		threadCount = atoi(argv[3]);
		clusterEnbaled =  atoi(argv[4]);		
		xRedis redis(ip,port,threadCount,clusterEnbaled);
		redis.run();
	}
	else if (argc == 1)
	{
		 xRedis redis("0.0.0.0", 6379, 0, 0);
		 redis.run();
	}
	else
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <threads> <cluster>\n");
	}
	

	return 0;
}




















