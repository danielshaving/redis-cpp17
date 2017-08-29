//hiredis client test sync
#include "all.h"
#include "xLog.h"
#include "xHiredis.h"


int main(int argc, char* argv[])
{
	if (argc != 3)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> \n ");
	}
	else
	{
		  const char* ip = argv[1];
		  uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		  xClient client(ip,port);
	}
	return 0;
}

