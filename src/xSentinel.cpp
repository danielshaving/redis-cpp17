#include "xSentinel.h"
#include "xRedis.h"

xSentinel::xSentinel(const char *ip,uint16_t port,bool clusterEnabled,int32_t threadCount)
:redis(new xRedis())
{
	server.init(&loop, ip, port,this);
	server.setConnectionCallback(std::bind(&xRedis::connCallBack, redis, std::placeholders::_1,std::placeholders::_2));
	server.setThreadNum(threadCount);
	server.start();

}

void xSentinel::connErrorCallBack()
{

}


void xSentinel::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
{

}


void xSentinel::connCallBack(const xTcpconnectionPtr& conn,void *data)
{

}


void xSentinel::run()
{
	loop.run();
}
