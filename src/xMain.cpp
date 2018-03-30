#include "xRedis.h"
#include "xLog.h"


/*
xAsyncLogging *g_asyncLog = nullptr;
void asyncOutput(const char *msg, int len)
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



int main(int argc, char *argv[])
{
	signal(SIGPIPE, SIG_IGN);
	signal(SIGHUP, SIG_IGN);
	xLogger::setOutput(asyncOutput);
	xAsyncLogging log("xredis", 4096);
	log.start();
	g_asyncLog = &log;
	printf("%s\n", ascii_logo);

	if(argc == 5)
	{
		const char *ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int16_t threadCount = atoi(argv[3]);
		bool clusterEnbaled = atoi(argv[4]);
		xRedis redis(ip,port,threadCount,clusterEnbaled);
		LOG_INFO<<"ip:"<<ip;
		LOG_INFO<<"port:"<<port;
		LOG_INFO<<"thread:"<<threadCount;
		LOG_INFO<<"cluster:"<<clusterEnbaled;
		redis.run();
	}
	else if (argc == 1)
	{
		xRedis redis("0.0.0.0", 6379, 0);
		redis.run();
	}
	else
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <threads> <cluster>\n");
	}

	return 0;
}

*/


























