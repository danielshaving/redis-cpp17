
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




int main(int argc, char* argv[])
{
	loadConfig("./redis.conf");

	printf("%s\n",ascii_logo);
	const char* ip = configs["ip"].c_str();
	uint16_t port = atoi(configs["port"].c_str());
	int16_t threadCount =  atoi(configs["thread"].c_str());
	uint16_t clusterEnbaled =  atoi(configs["clusterEnabled"].c_str());
	xLogger::setOutput(asyncOutput);

	xAsyncLogging log("xredis", 4096);
	log.start();
	g_asyncLog = &log;
	xRedis redis(ip,port,threadCount,clusterEnbaled);
	redis.run();
	return 0;
}





















