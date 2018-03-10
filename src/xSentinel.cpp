#include "xSentinel.h"
#include "xRedis.h"
#include "xLog.h"

xSentinel::xSentinel(xRedis * redis)
:redis(redis),
 start(false),
 isreconnect(true),
 port(0)
{

}


xSentinel::~xSentinel()
{
	
}

void xSentinel::readCallBack(const TcpConnectionPtr &conn, xBuffer *buffer)
{
	
}


void xSentinel::connCallBack(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		this->conn = conn;
		socket.getpeerName(conn->getSockfd(),&(conn->ip),conn->port);
		{
			std::unique_lock <std::mutex> lck(redis->sentinelMutex);
			redis->slaveConns.insert(std::make_pair(conn->getSockfd(), conn));
		}
		LOG_INFO<<"connect Sentinel suucess ";
		
	}
	else
	{
		{
			std::unique_lock <std::mutex> lck(redis->sentinelMutex);
			redis->slaveConns.erase(conn->getSockfd());
		}
		LOG_INFO<<"disconnect sentinel ";
	}
}


void xSentinel::connectSentinel()
{
	start = true;
	xEventLoop loop;
	xTcpClient client(&loop,this);
	client.setConnectionCallback(std::bind(&xSentinel::connCallBack, this, std::placeholders::_1));
	client.setMessageCallback( std::bind(&xSentinel::readCallBack, this, std::placeholders::_1,std::placeholders::_2));
	client.setConnectionErrorCallBack(std::bind(&xSentinel::connErrorCallBack, this));
	this->loop = &loop;
	this->client = & client;
	this->loop = &loop;
	loop.run();
}

void xSentinel::reconnectTimer(const std::any &context)
{
	LOG_INFO<<"Reconnect..........";
	client->asyncConnect(ip.c_str(),port);
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



