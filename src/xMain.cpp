
//redis srever
#include "xRedis.h"
#include "xCurrentThread.h"
#include "xLog.h"
#include <stdio.h>
//#include <gperftools/profiler.h>

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




int main(int argc, char* argv[])
{
	if (argc < 5)
	{
		fprintf(stderr, "Usage: server <address> <port> <threads> <cluster>\n");
	}
	else
	{
		//ProfilerStart("benchmark.prof");
		printf("%s\n",ascii_logo);
		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int16_t threadCount =  atoi(argv[3]);
		uint16_t clusterEnbaled =  atoi(argv[4]);
		xLogger::setOutput(asyncOutput);

		xAsyncLogging log("xredis", 20000);
		log.start();
		g_asyncLog = &log;
		xRedis redis(ip,port,threadCount,clusterEnbaled);
		redis.run();
		//ProfilerStop();

	}

	return 0;
}

















