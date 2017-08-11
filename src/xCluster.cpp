#include "xCluster.h"
#include "xRedis.h"
#include "xLog.h"
#include "xCrc16.h"

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


void xCluster::clusterRedirectClient(xSession * session, xClusterNode * n, int hashSlot, int errCode)
{
	if (errCode == CLUSTER_REDIR_CROSS_SLOT) {
		addReplySds(session->sendBuf, sdsnew("-CROSSSLOT Keys in request don't hash to the same slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_UNSTABLE) {
		/* The request spawns mutliple keys in the same slot,
		* but the slot is not "stable" currently as there is
		* a migration or import in progress. */
		addReplySds(session->sendBuf,  sdsnew("-TRYAGAIN Multiple keys request during rehashing of slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_STATE) {
		addReplySds(session->sendBuf,  sdsnew("-CLUSTERDOWN The cluster is down\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_UNBOUND) {
		addReplySds(session->sendBuf, sdsnew("-CLUSTERDOWN Hash slot not served\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_MOVED ||
		errCode == CLUSTER_REDIR_ASK)
	{
		addReplySds(session->sendBuf,  sdscatprintf(sdsempty(),
			"-%s %d %s:%d\r\n",
			(errCode == CLUSTER_REDIR_ASK) ? "ASK" : "MOVED",
			hashSlot, n->ip.c_str(), n->port));
	}
	else {
		LOG_WARN << "getNodeByQuery() unknown error.";
	}
} 


void xCluster::syncClusterSlot(std::deque<rObj*> &robj)
{
	for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); it++)
	{
		xBuffer sendBuf;
		redis->structureRedisProtocol(sendBuf, robj);
		it->second->send(&sendBuf);
	}

	for (auto it = robj.begin(); it != robj.end(); it++)
	{
		zfree(*it);
	}

}

unsigned int xCluster::keyHashSlot(char *key, int keylen)
{
	int s, e; /* start-end indexes of { and } */

	for (s = 0; s < keylen; s++)
		if (key[s] == '{') break;

	/* No '{' ? Hash the whole key. This is the base case. */
	if (s == keylen) return crc16(key, keylen) & 0x3FFF;

	/* '{' found? Check if we have the corresponding '}'. */
	for (e = s + 1; e < keylen; e++)
		if (key[e] == '}') break;

	/* No '}' or nothing betweeen {} ? Hash the whole key. */
	if (e == keylen || e == s + 1) return crc16(key, keylen) & 0x3FFF;

	/* If we are here there is both a { and a } on its right. Hash
	* what is in the middle between { and }. */
	return crc16(key + s + 1, e - s - 1) & 0x3FFF;
}

int xCluster::getSlotOrReply(xSession  * session,rObj * o)
{
	long long slot;

	if (getLongLongFromObject(o, &slot) != REDIS_OK ||
		slot < 0 || slot >= CLUSTER_SLOTS)
	{
		addReplyError(session->sendBuf, "Invalid or out of range slot");
		return  REDIS_ERR;
	}
	return (int)slot;
}

void xCluster::structureProtocolSetCluster(std::string host, int16_t port, xBuffer &sendBuf, std::deque<rObj*> &robj,const xTcpconnectionPtr & conn)
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


void xCluster::delClusterImport(std::deque<rObj*> &robj)
{
	for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); it++)
	{
		xBuffer sendBuf;
		redis->structureRedisProtocol(sendBuf, robj);
		it->second->send(&sendBuf);
	}

	for (auto it = robj.begin(); it != robj.end(); it++)
	{
		zfree(*it);
	}
}


void xCluster::asyncReplicationToNode(std::string ip,int16_t port)
{
	std::unordered_set<int32_t>  uset;
	int32_t fd = 0;
	std::string ipPort = ip + "::" + std::to_string(port);
	{
		MutexLockGuard lk(redis->clusterMutex);
		auto it = migratingSlosTos.find(ipPort);
		if(it == migratingSlosTos.end())
		{
			LOG_WARN<<"async slot error";
			return ;
		}
			
		if(it->second.size() == 0)
		{
			LOG_WARN<<"async slot error";
			return ;
		}

		 uset = it->second;

		 for(auto iter = redis->clustertcpconnMaps.begin(); iter != redis->clustertcpconnMaps.end(); iter++)
		 {
		 	if(iter->second->host == ip && iter->second->port == port)
		 	{
		 		fd = iter->first;
				break;
		 	}
		 }
	}

	xBuffer sendBuf;
	
	if(fd == 0)
	{
		LOG_WARN<<"clustertcpconnMaps error";
		return ;
	}


		
	redis->clusterRepliMigratEnabled = true;
	std::deque<rObj*> deques;
	for(auto it = redis->setMapShards.begin(); it != redis->setMapShards.end(); it ++)
	{
		{
			MutexLockGuard lock((*it).mutex);
			for(auto iter = (*it).setMap.begin(); iter !=  (*it).setMap.end(); iter ++)
			{
				unsigned int slot = keyHashSlot((char*)iter->first->ptr,sdsllen(iter->first->ptr));
				auto iterr = uset.find(slot);
				if(iterr != uset.end())
				{
					deques.push_back(iter->first);
					deques.push_back(iter->second);
					redis->structureRedisProtocol(sendBuf,deques);
					deques.clear();
				}				
			}
		}

		{
			MutexLockGuard lock(redis->clusterMutex);
			auto iter = redis->clustertcpconnMaps.find(fd);
			if(iter == redis->clustertcpconnMaps.end())
			{
				LOG_WARN<<"cluster disconnect error";
				return ;
			}
			
			if(sendBuf.readableBytes() > 0)
			{
				iter->second->send(&sendBuf);
			}
		
		}
	}
	
	
	redis->clusterRepliMigratEnabled = false;
	
	{

		MutexLockGuard lock(redis->clusterMutex);	
		if(redis->clusterMigratCached.readableBytes()> 0 )
		{
			
			auto iter = redis->clustertcpconnMaps.find(fd);
			if(iter == redis->clustertcpconnMaps.end())
			{
				LOG_WARN<<"cluster disconnect error";
				return ;
			}
			
			iter->second->send(&redis->clusterMigratCached);
		}
		
		
		xBuffer buff;
		buff.swap(redis->clusterMigratCached);
		migratingSlosTos.clear();

		
			
		for(auto it = uset.begin(); it != uset.end(); it ++)
		{
			clusterSlotNodes.erase(*it);

			std::deque<rObj*> robj;
			rObj * c = createStringObject("cluster", 7);
			rObj * d = createStringObject("delsync", 7);
			robj.push_back(c);
			robj.push_back(d);
			char buf[32];
			int32_t len = ll2string(buf, sizeof(buf), *it);
			rObj * o = createStringObject((const char*)buf, len);
		
			robj.push_back(o);
			syncClusterSlot(robj);	
			
			
		}


		std::deque<rObj*> robj;
		rObj * c = createStringObject("cluster", 7);
		rObj * d = createStringObject("delimport", 9);
		robj.push_back(c);
		robj.push_back(d);
		delClusterImport(robj);	
			
	}

	LOG_INFO<<"Cluster aysncRepliction success";
	
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


void xCluster::connSetCluster(std::string ip, int16_t port,xRedis * redis)
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




