#include "xCluster.h"
#include "xRedis.h"
#include "xLog.h"

xCluster::xCluster()
	:start(false),
	isreconnect(true),
	port(0)
{

}


xCluster::~xCluster()
{

}

void xCluster::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf, void *data)
{
	while (recvBuf->readableBytes() > 0)
	{

	}
}


void xCluster::connCallBack(const xTcpconnectionPtr& conn, void *data)
{
	if (conn->connected())
	{
		this->conn = conn;
		socket.getpeerName(conn->getSockfd(), &(conn->host), conn->port);

		{
			xBuffer sendBuf;
			MutexLockGuard lc(redis->sentinelMutex);
			for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); it++)
			{
				//structureProtocol();
				//conn->send();
			}

			redis->clustertcpconnMaps.insert(std::make_pair(conn->getSockfd(), conn));

		
		}
		
		


		std::shared_ptr<xSession> session(new xSession(redis, conn));
		{
			MutexLockGuard mu(redis->mutex);
			redis->sessions[conn->getSockfd()] = session;
		}
		LOG_INFO << "connect cluster suucess ";

	}
	else
	{
		{
			MutexLockGuard lc(redis->sentinelMutex);
			redis->clustertcpconnMaps.erase(conn->getSockfd());
		}

		LOG_INFO << "disconnect cluster ";
	}
}


void xCluster::connectCluster()
{
	start = true;
	xEventLoop loop;
	xTcpClient client(&loop, this);
	client.setConnectionCallback(std::bind(&xCluster::connCallBack, this, std::placeholders::_1, std::placeholders::_2));
	client.setMessageCallback(std::bind(&xCluster::readCallBack, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
	client.setConnectionErrorCallBack(std::bind(&xCluster::connErrorCallBack, this));
	this->loop = &loop;
	this->client = &client;
	this->loop = &loop;
	loop.run();
}

void xCluster::reconnectTimer(void * data)
{
	LOG_INFO << "Reconnect..........";
	client->connect(ip.c_str(), port);
}



void xCluster::connErrorCallBack()
{

	if (!isreconnect)
	{
		return;
	}

	if (connectCount >= REDIS_RECONNECT_COUNT)
	{
		LOG_WARN << "Reconnect failure";
		ip.clear();
		port = 0;
		isreconnect = true;
		return;
	}

	++connectCount;
	loop->runAfter(5, nullptr, false, std::bind(&xCluster::reconnectTimer, this, std::placeholders::_1));
}

void xCluster::init()
{

}



