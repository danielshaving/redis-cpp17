#include "redisproxy.h"

const char *logo =
"                _._                                                  \n"
"           _.-``__ ''-._                                             \n"
"      _.-``    `.  `_.  ''-._           redis-proxy 1.0	    	  \n"
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


int main(int argc, char *argv[]) {
	printf("%s\n", logo);
#ifdef _WIN64
	WSADATA wsaData;
	int32_t iRet = WSAStartup(MAKEWORD(2, 2), &wsaData);
	assert(iRet == 0);
#else
	signal(SIGPIPE, SIG_IGN);
#endif

    const char *ip = "127.0.0.1"
    uint16_t port = 6379;
    int16_t thread = 0;
    int32_t session = 1;
    const char *redisIp = "127.0.0.1";
    int16_t redisPort = 7000;
	const char *logPath = "../log/";
	bool cluster = false;
	
	RedisProxy proxy(ip, port, redisIp, redisPort, thread, session, logPath, cluster);
	proxy.run();
	return 0;
}
