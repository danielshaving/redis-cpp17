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
	logFile->flush();
}

int main(int argc, char *argv[])
{
#ifdef _WIN64
	WSADATA wsaData;
	int32_t iRet = WSAStartup(MAKEWORD(2, 2), &wsaData);
	assert(iRet == 0);
#else
	signal(SIGPIPE, SIG_IGN);
	signal(SIGHUP, SIG_IGN);
#endif

	logFile.reset(new LogFile("redislog", "redis", 65536, false));
	Logger::setOutput(dummyOutput);
	printf("%s\n", logo);

	Redis redis("127.0.0.1", 6379, 0);
	redis.run();
	return 0;
}
