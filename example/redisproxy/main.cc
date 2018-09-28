#include "redisproxy.h"

std::unique_ptr<LogFile> logFile;
void dummyOutput(const char *msg, int len)
{
	printf("%s", msg);
	logFile->append(msg, len);
	logFile->flush();
}

//./redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 1

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
	RedisProxy proxy("127.0.0.1", 6378, "127.0.0.1", 7000, 4, 10);
	proxy.run();
	return 0;
}
