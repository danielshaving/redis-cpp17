#include "xCluster.h"
#include "xRedis.h"
#include "xLog.h"

xCluster::xCluster()
{

}


xCluster::~xCluster()
{

}

void xCluster::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf, void *data)
{
	while (recvBuf->readableBytes() > 0)
	{
		if (memcmp(recvBuf->peek(), shared.ok->ptr, 3) == 0)
		{
			recvBuf->retrieve(5);
			if (recvBuf->readableBytes() == 0)
			{
				std::shared_ptr<xSession> session(new xSession(redis, conn));
				{
					MutexLockGuard lk(redis->mutex);
					redis->sessions[conn->getSockfd()] = session;
				}

				break;
			}	
		}
		else
		{
			conn->forceClose();
			LOG_WARN << "connect cluster read msg error";
			break;
		}

	}
}


void xCluster::structureProtocolSetCluster(std::string host, int32_t port, xBuffer &sendBuf, std::deque<rObj*> &robj,const xTcpconnectionPtr & conn)
{
	rObj * ip = createStringObject(host.c_str(), host.length());
	char buf[32];
	int32_t len = ll2string(buf, sizeof(buf), port);
	rObj * p = createStringObject((const char*)buf, len);
	robj.push_back(ip);
	robj.push_back(p);
	redis->structureRedisProtocol(sendBuf, robj);
	robj.pop_back();
	robj.pop_back();
	zfree(ip);
	zfree(p);
	conn->send(&sendBuf);
	sendBuf.retrieveAll();
}


void xCluster::connCallBack(const xTcpconnectionPtr& conn, void *data)
{
	if (conn->connected())
	{
		socket.getpeerName(conn->getSockfd(), &(conn->host), conn->port);
		{
			xBuffer sendBuf;
			std::deque<rObj*> robj;
			rObj * c = createStringObject("cluster", 7);
			rObj * m = createStringObject("connect", 7);
			robj.push_back(c);
			robj.push_back(m);

			MutexLockGuard lk(redis->clusterMutex);
			for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); it++)
			{
				structureProtocolSetCluster(it->second->host, it->second->port, sendBuf, robj,conn);
			}

			structureProtocolSetCluster(redis->host, redis->port, sendBuf, robj,conn);
			for (auto it = robj.begin(); it != robj.end(); it++)
			{
				zfree(*it);
			}

			redis->clustertcpconnMaps.insert(std::make_pair(conn->getSockfd(), conn));

		}
		LOG_INFO << "connect cluster suucess ";

	}
	else
	{
		{
			MutexLockGuard lc(redis->clusterMutex);
			redis->clustertcpconnMaps.erase(conn->getSockfd());
			for (auto it = tcpvectors.begin(); it != tcpvectors.end(); it++)
			{
				if ((*it)->connection->host == conn->host && (*it)->connection->port == conn->port)
				{
					tcpvectors.erase(it);
					break;
				}
			}
			
		}

		LOG_INFO << "disconnect cluster ";
	}
}


void xCluster::connSetCluster(std::string ip, int32_t port,xRedis * redis)
{
	this->redis = redis;
	std::shared_ptr<xTcpClient> client(new xTcpClient(loop, this));
	client->setConnectionCallback(std::bind(&xCluster::connCallBack, this, std::placeholders::_1, std::placeholders::_2));
	client->setMessageCallback(std::bind(&xCluster::readCallBack, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
	client->setConnectionErrorCallBack(std::bind(&xCluster::connErrorCallBack, this));
	client->connect(ip.c_str(), port);
	tcpvectors.push_back(client);
}

void xCluster::connectCluster()
{
	xEventLoop loop;
	this->loop = &loop;
	loop.run();
}

void xCluster::reconnectTimer(void * data)
{
	LOG_INFO << "Reconnect..........";

}



void xCluster::connErrorCallBack()
{
	return;
}

void xCluster::init()
{

}



