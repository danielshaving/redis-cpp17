//hiredis client test async
#include "all.h"
#include "xLog.h"
#include "xHiredis.h"


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
		int threadCount = atoi(argv[3]);
		int blockSize = atoi(argv[4]);
		int sessionCount = atoi(argv[5]);

		xEventLoop loop;
		xClient client(&loop, ip,port, blockSize, sessionCount, threadCount);
		loop.run();
	}
	return 0;
}

