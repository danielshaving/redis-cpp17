#include "xCluster.h"
#include "xRedis.h"
#include "xLog.h"
#include "xCrc16.h"

xCluster::xCluster():state(true),
isConnect(false),
replyCount(0)
{

}


xCluster::~xCluster()
{

}

void xCluster::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf, void *data)
{
	while (recvBuf->readableBytes() > 0)
	{
		if (!memcmp(recvBuf->peek(), shared.ok->ptr, sdslen(shared.ok->ptr)))
		{	
			std::unique_lock <std::mutex> lck(redis->clusterMutex);
			if (++replyCount == redis->clustertcpconnMaps.size())
			{
				redis->clusterRepliMigratEnabled = false;
				if (redis->clusterMigratCached.readableBytes() > 0)
				{
					if (conn->connected())
					{
						conn->send(&redis->clusterMigratCached);
					}

					xBuffer buffer;
					buffer.swap(redis->clusterMigratCached);
				}

				migratingSlosTos.clear();
				replyCount = 0;
				assert(recvBuf->readableBytes() == 0);

				for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); ++it)
				{
					std::shared_ptr<xSession> session(new xSession(redis, conn));
					std::unique_lock <std::mutex> lck(redis->mtx);
					redis->sessions[conn->getSockfd()] = session;
				}


                std::string ipPort = conn->host + "::" + std::to_string(conn->port);
				if(!getSlotSet(ipPort))
                {

                    break;
                }

                for(auto it = uset.begin(); it != uset.end(); ++it)
                {
                    clusterSlotNodes.erase(*it);
                }


                migratingSlosTos.erase(ipPort);
                importingSlotsFrom.erase(ipPort);

                clear();

				LOG_INFO << "cluster migrate success " << conn->host << " " << conn->port;
			}

		}
		else
		{
			conn->forceClose();
			LOG_INFO<<"cluster migrate failure " <<conn->host<<" " <<conn->port;
			break;
		}

		recvBuf->retrieve(sdslen(shared.ok->ptr));

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
    std::unique_lock <std::mutex> lck(redis->clusterMutex);
    for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); ++it)
    {
        redis->structureRedisProtocol(sendBuf, robj);
        it->second->send(&sendBuf);
        clear();
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

void xCluster::structureProtocolSetCluster(std::string host, int32_t port, xBuffer &sendBuf,const xTcpconnectionPtr & conn)
{
    deques.push_back(shared.cluster);
	deques.push_back(shared.connect);

	char buf[8];
	int32_t len = ll2string(buf, sizeof(buf), port);
	deques.push_back(createStringObject((char*)(host.c_str()), host.length()));
	deques.push_back(createStringObject(buf, len));
	redis->structureRedisProtocol(sendBuf, deques);
	conn->send(&sendBuf);
	redis->clearDeques(deques);
	clear();
}


void xCluster::delClusterImport(std::deque<rObj*> &robj)
{
	for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); ++it)
	{
		redis->structureRedisProtocol(sendBuf, robj);
		it->second->send(&sendBuf);
		clear();
	}

	for (auto it = robj.begin(); it != robj.end(); ++it)
	{
		zfree(*it);
	}
}



void xCluster::clear()
{
    uset.clear();
    deques.clear();
    sendBuf.retrieveAll();
}

bool  xCluster::replicationToNode(xSession * session,const std::string &ip,int32_t port)
{
    clear();

	xTcpconnectionPtr conn;
	bool mark = false;
	std::string ipPort = ip + "::" + std::to_string(port);
	{
        if(!getSlotSet(ipPort))
        {
            return false;
        }

        for(auto iter = redis->clustertcpconnMaps.begin(); iter != redis->clustertcpconnMaps.end(); ++iter)
        {
            if(iter->second->host == ip && iter->second->port == port)
            {
                conn = iter->second;
                mark = true;
            }
        }
	}

	if (!mark)
	{
		LOG_WARN << "cluster node disconnect:" << ipPort;
		return false;
	}

	mark = false;



	auto threadPoolVec = redis->server.getThreadPool()->getAllLoops();
	for (auto it = threadPoolVec.begin(); it != threadPoolVec.end(); ++it)
	{
		if (session->conn->getLoop()->getThreadId() == (*it)->getThreadId())
		{
			continue;
		}

		(*it)->runInLoop(std::bind(&xRedis::handleForkTimeOut, redis));
	}

    if(redis->threadCount > 1)
    {
        std::unique_lock <std::mutex> lck(redis->forkMutex);
        while (redis->forkCondWaitCount < redis->threadCount)
        {
            redis->expireCondition.wait(lck);
        }
    }



	for(auto it = redis->setMapShards.begin(); it != redis->setMapShards.end(); ++it)
	{
		std::unique_lock <std::mutex> lck((*it).mtx);
		for(auto iter = (*it).setMap.begin(); iter !=  (*it).setMap.end(); ++iter)
		{
			unsigned int slot = keyHashSlot((char*)iter->first->ptr,sdslen(iter->first->ptr));
			auto iterr = uset.find(slot);
			if(iterr != uset.end())
			{
				deques.push_back(shared.set);
				deques.push_back(iter->first);
				deques.push_back(iter->second);
				redis->structureRedisProtocol(sendBuf,deques);
				if(conn->connected())
				{
					conn->sendPipe(&sendBuf);
				}
				else
				{
					LOG_INFO<<"cluster disconnect "<<conn->host<<" "<<conn->port;
					return false;
				}
				
			}
		
		}
	}

    clear();
	redis->forkCondWaitCount = 0;
	redis->clusterRepliMigratEnabled = true;

	if(redis->threadCount > 1)
    {
        redis->forkCondition.notify_all();
    }



    {
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); ++it)
		{
			it->second->setMessageCallback(std::bind(&xCluster::readCallBack, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
		}

	}


    deques.push_back(shared.cluster);
    deques.push_back(shared.setslot);
    deques.push_back(shared.node);
    deques.push_back(createStringObject((char*)redis->ipPort.c_str(),redis->ipPort.length()));
    deques.push_back(createStringObject((char*)ipPort.c_str(),ipPort.length()));


    if(!getSlotSet(ipPort))
    {
        return false;
    }

    for(auto it = uset.begin(); it != uset.end(); ++it)
    {
        char buf[4];
        int len = ll2string(buf, sizeof(buf), *it);
        deques.push_back(createStringObject(buf, len));
    }


    syncClusterSlot(deques);
    redis->clearDeques(deques);



	LOG_INFO<<"cluster send  success";
	
}



bool xCluster::getSlotSet(const std::string &ipPort)
{

    std::unique_lock <std::mutex> lck(redis->clusterMutex);
    auto it = migratingSlosTos.find(ipPort);
    assert(it->second.size());
    if(it == migratingSlosTos.end())
    {
        LOG_INFO<<"migratingSlosTos not found " << ipPort;
        return false;
    }

    uset = it->second;
}


void xCluster::connCallBack(const xTcpconnectionPtr& conn, void *data)
{
	if (conn->connected())
	{
		isConnect = true;
		state = false;
		{
			std::unique_lock <std::mutex> lck(cmtex);
			condition.notify_one();
		}

		socket.getpeerName(conn->getSockfd(), &(conn->host), conn->port);

		
		{
			std::unique_lock <std::mutex> lck(redis->clusterMutex);
			for (auto it = redis->clustertcpconnMaps.begin(); it != redis->clustertcpconnMaps.end(); ++it)
			{
				structureProtocolSetCluster(it->second->host, it->second->port, sendBuf,conn);
			}

			structureProtocolSetCluster(redis->host, redis->port, sendBuf,conn);
			redis->clustertcpconnMaps.insert(std::make_pair(conn->getSockfd(), conn));
		}

		std::shared_ptr<xSession> session(new xSession(redis, conn));
		{
			std::unique_lock <std::mutex> lck(redis->mtx);
			redis->sessions[conn->getSockfd()] = session;
		}

		LOG_INFO << "connect cluster success "<<"ip:"<<conn->host<<" port:"<<conn->port;

	}
	else
	{
		{
			std::unique_lock <std::mutex> lck(redis->clusterMutex);
			redis->clustertcpconnMaps.erase(conn->getSockfd());
			for (auto it = tcpvectors.begin(); it != tcpvectors.end(); ++it)
			{
				if ((*it)->connection->host == conn->host && (*it)->connection->port == conn->port)
				{
					tcpvectors.erase(it);
					break;
				}
			} 
			eraseClusterNode(conn->host,conn->port);
			migratingSlosTos.erase(conn->host + std::to_string(conn->port));
			importingSlotsFrom.erase(conn->host + std::to_string(conn->port));

		}

		LOG_INFO << "disconnect cluster "<<"ip:"<<conn->host<<" port:"<<conn->port;
	}
}



void xCluster::getKeyInSlot(int hashslot,rObj **keys,int count)
{
	int j = 0;
	for(auto it = redis->setMapShards.begin(); it != redis->setMapShards.end();++it)
	{
		std::unique_lock <std::mutex> lck((*it).mtx);
		for(auto iter = (*it).setMap.begin(); iter !=  (*it).setMap.end(); iter ++)
		{
			if(count ==0)
			{
				return ;
			}
			
			unsigned int slot = keyHashSlot((char*)iter->first->ptr,sdslen(iter->first->ptr));
			if(slot == hashslot)
			{
				keys[j++] = iter->first;
				count --;
			}
			
		}
	}
	
}


void xCluster::eraseImportSlot(int slot)
{

}

void xCluster::eraseClusterNode(const std::string &ip ,int32_t port)
{
	for(auto it = clusterSlotNodes.begin(); it != clusterSlotNodes.end(); )
	{
		if(ip == it->second.ip && port == it->second.port)
		{
			clusterSlotNodes.erase(it++);
			continue;
		}

		it ++;
	}

}


bool  xCluster::connSetCluster(const std::string &ip, int32_t port)
{
	std::shared_ptr<xTcpClient> client(new xTcpClient(loop, this));
	client->setConnectionCallback(std::bind(&xCluster::connCallBack, this, std::placeholders::_1, std::placeholders::_2));
	client->setMessageCallback(std::bind(&xCluster::readCallBack, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
	client->setConnectionErrorCallBack(std::bind(&xCluster::connErrorCallBack, this));
	client->connect(ip.c_str(), port);
	tcpvectors.push_back(client);

	std::unique_lock <std::mutex> lck(cmtex);
	while(state)
	{
		condition.wait(lck);
	}
	
	state = true;

	if(isConnect)
	{
		isConnect = false;
		return true;
	}
	else
	{
		return false;
	}
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
	state = false;
	isConnect = false;

	{
		std::unique_lock <std::mutex> lck(cmtex);
		condition.notify_one();
	}
}

void xCluster::init(xRedis * redis)
{
	this->redis = redis;
}




