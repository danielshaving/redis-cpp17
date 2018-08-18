#include "redis.h"
const char *logo =
"                _._                                                  \n"
"           _.-``__ ''-._                                             \n"
"      _.-``    `.  `_.  ''-._           redis-cpp server 1.0    	  \n"
"  .-`` .-```.  ```\\/    _.,_ ''-._                                  \n"
" (    '      ,       .-`  | `,    )								  \n"
" |`-._`-...-` __...-.``-._|'` _.-'|								  \n"
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

std::unique_ptr<LogFile> logFile;
void dummyOutput(const char *msg, int len)
{
	printf("%s\n", msg);
	logFile->append(msg, len);
}

int main(int argc, char *argv[])
{
#ifdef _WIN32
	WSADATA wsaData;
	int32_t iRet = WSAStartup(MAKEWORD(2, 2), &wsaData);
	assert(iRet == 0);
#else
	signal(SIGPIPE, SIG_IGN);
	signal(SIGHUP, SIG_IGN);
#endif

	logFile.reset(new LogFile("redislog", "redis", 4096, false));
	Logger::setOutput(dummyOutput);
	printf("%s\n", logo);

	if (argc == 5)
	{
		const char *ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int16_t threadCount = atoi(argv[3]);
		bool clusterEnbaled = atoi(argv[4]);
		LOG_INFO << "ip:" << ip;
		LOG_INFO << "port:" << port;
		LOG_INFO << "thread:" << threadCount;
		LOG_INFO << "cluster:" << clusterEnbaled;
		Redis redis(ip, port, threadCount, clusterEnbaled);
		redis.run();
	}
	else if (argc == 1)
	{
		Redis redis("127.0.0.1", 6379, 0);
		redis.run();
	}
	else
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <threads> <cluster>\n");
	}
	return 0;
}

