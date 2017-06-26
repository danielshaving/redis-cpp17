
//redis srever
#include "xRedis.h"
#include "xCurrentThread.h"
#include "xLog.h"
#include <stdio.h>

xAsyncLogging *g_asyncLog = nullptr;
void asyncOutput(const char* msg, int len)
{
	printf("%s\n",msg);
	g_asyncLog->append(msg, len);
}

char *ascii_logo =
"                _._                                                  \n"
"           _.-``__ ''-._                                             \n"
"      _.-``    `.  `_.  ''-._           redis 1.0 					  \n"
"  .-`` .-```.  ```\\/    _.,_ ''-._                                  \n"
" (    '      ,       .-`  | `,    )     Running in release			  \n"
" |`-._`-...-` __...-.``-._|'` _.-'|     Port: 6379                   \n"
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
		fprintf(stderr, "Usage: server <address> <port> <threads> <enabled cluster 1 true 0 false>\n");
	}
	else
	{
		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int32_t threadCount =  atoi(argv[3]);
		bool 	enbaledCluster = argv[4];
		xLogger::setOutput(asyncOutput);

		xAsyncLogging log("libredis", 2000);
		log.start();
		g_asyncLog = &log;
		xRedis redis(ip,port,threadCount,enbaledCluster);
		redis.run();


	}

		return 0;
}



//hiredis client test asyncbenchmark
//#include "all.h"
//#include "xEventLoop.h"
//#include "xLog.h"
//#include "xHiredis.h"
//
//
//int main(int argc, char* argv[])
//{
//	 if (argc != 6)
//	  {
//	    fprintf(stderr, "Usage: client <host_ip> <port> <threads> <blocksize> ");
//	    fprintf(stderr, "<sessions> \n");
//	  }
//	  else
//	  {
//	    const char* ip = argv[1];
//	    uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
//	    int threadCount = atoi(argv[3]);
//	    int blockSize = atoi(argv[4]);
//	    int sessionCount = atoi(argv[5]);
//
//	    xEventLoop loop;
//	    xClient client(&loop, ip,port, blockSize, sessionCount, threadCount);
//	    loop.run();
//	  }
//}

//hiredis client test sync
//#include "all.h"
//#include "xLog.h"
//#include "xHiredis.h"
//
//
//int main(int argc, char* argv[])
//{
//	if (argc != 3)
//	{
//		fprintf(stderr, "Usage: client <host_ip> <port> \n ");
//	}
//	else
//	{
//		  const char* ip = argv[1];
//		  uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
//		  xClient client(ip,port);
//	}
//	return 0;
//}



