#include "xSentinel.h"
#include "xRedis.h"
#include "xLog.h"

xSentinel::xSentinel(xRedis *redis)
:redis(redis),
 start(false),
 isreconnect(true),
 port(0)
{

}

xSentinel::~xSentinel()
{
	
}

void xSentinel::readCallBack(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	
}

void xSentinel::connCallBack(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		this->conn = conn;
		{
			std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
			redis->getSlaveConn().insert(std::make_pair(conn->getSockfd(),conn));
		}
		LOG_INFO<<"connect Sentinel suucess ";
		
	}
	else
	{
		{
			std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
			redis->getSlaveConn().erase(conn->getSockfd());
		}
		LOG_INFO<<"disconnect sentinel ";
	}
}


void xSentinel::connectSentinel()
{
	start = true;
	xEventLoop loop;
	this->loop = &loop;
	loop.run();
}

void xSentinel::reconnectTimer(const std::any &context)
{

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
}



