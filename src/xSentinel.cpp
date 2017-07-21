#include "xSentinel.h"
#include "xRedis.h"
#include "xLog.h"

xSentinel::xSentinel()
:start(false),
 isreconnect(true),
 port(0)
{

}


xSentinel::~xSentinel()
{
	
}

void xSentinel::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
{

}


void xSentinel::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	
}


void xSentinel::connectSentinel()
{
	start = true;
	xEventLoop loop;
	xTcpClient client(&loop,this);
	client.setConnectionCallback(std::bind(&xSentinel::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	client.setMessageCallback( std::bind(&xSentinel::readCallBack, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
	client.setConnectionErrorCallBack(std::bind(&xSentinel::connErrorCallBack, this));
	this->client = & client;
	this->loop = &loop;
	loop.run();
}

void xSentinel::reconnectTimer(void * data)
{
	LOG_INFO<<"Reconnect..........";
	client->connect(ip.c_str(),port);
}



void xSentinel::connErrorCallBack()
{
	if(!isreconnect)
	{
		return ;
	}
	
	if(connectCount >= REDIS_RECONNECT_COUNT)
	{
		LOG_WARN<<"Reconnect failure";
		ip.clear();
		port = 0;
		isreconnect = true;
		return ;
	}
	
	++connectCount;
	loop->runAfter(5,nullptr,false,std::bind(&xSentinel::reconnectTimer,this,std::placeholders::_1));
}




