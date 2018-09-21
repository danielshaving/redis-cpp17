#include "redisproxy.h"

std::unique_ptr<LogFile> logFile;
void dummyOutput(const char *msg, int len)
{
	printf("%s", msg);
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
#endif
	logFile.reset(new LogFile("log", "proxy", 65536, false));
	Logger::setOutput(dummyOutput);
	RedisProxy proxy("127.0.0.1", 6378, 0, 100);
	proxy.run();
	return 0;
}
