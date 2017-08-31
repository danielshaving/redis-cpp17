//hiredis client test async
#include "all.h"
#include "xLog.h"
#include "xHiredis.h"


/*for(int i = 0 ; i < 10;i++)
{
	std::string str = "set test" + std::to_string(conn->getSockfd()) + " "	+ " %b";
	redisAsyncCommand(ac, nullptr, nullptr, str.c_str(),str.c_str(), str.length());
	std::string str1 = "get test" + std::to_string(conn->getSockfd());
	redisAsyncCommand(ac, getCallback, nullptr,str1.c_str());
}
*/


int main(int argc, char* argv[])
{
	if (argc != 6)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <blockSize> <sessionCount> <timeOut> <threadCount> \n ");
	}
	else
	{
		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int blockSize = atoi(argv[3]);
		int sessionCount = atoi(argv[4]);
		int threadCount = atoi(argv[5]);

		xEventLoop loop;
		xClient client(&loop, ip,port, blockSize, sessionCount, threadCount);
		loop.run();
	}
	return 0;
}

