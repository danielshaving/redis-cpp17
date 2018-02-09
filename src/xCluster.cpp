#include "xCluster.h"
#include "xRedis.h"
#include "xLog.h"
#include "xCrc16.h"
#include "xUtil.h"

xCluster::xCluster(xRedis * redis)
:redis(redis),
state(true),
isConnect(false),
replyCount(0)
{

}


xCluster::~xCluster()
{

}


void xCluster::cretateClusterNode(int32_t slot,const std::string &ip,int16_t port,const std::string &name)
{
	xClusterNode node;
	node.name = name;
	node.configEpoch = -1;
	node.createTime = time(0);
	node.ip = ip;
	node.port = port;
	node.master = nullptr;
	node.slaves = nullptr;
	clusterSlotNodes.insert(std::make_pair(slot, node));

}

void xCluster::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf)
{
	while (recvBuf->readableBytes() > 0)
	{
		if (!memcmp(recvBuf->peek(), redis->object.ok->ptr, sdslen(redis->object.ok->ptr)))
		{
			LOG_INFO<<"reply to cluster ok";

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


				replyCount = 0;

				for (auto &it : redis->clustertcpconnMaps)
				{
					xSeesionPtr session(new xSession(redis, conn));
					std::unique_lock <std::mutex> lck(redis->mtx);
					redis->sessions[conn->getSockfd()] = session;
				}


				std::string ipPort = conn->ip + "::" + std::to_string(conn->port);
				if(!getSlotSet(ipPort))
				{
				    LOG_WARN<<"getSlot error";
				    break;
				}


				for(auto &it : uset)
				{
					auto iter = clusterSlotNodes.find(it);
					if(iter != clusterSlotNodes.end())
					{
						iter->second.ip = conn->ip;
						iter->second.port = conn->port;
					}
					else
					{
						LOG_WARN<<"slot not found error";
					}

				}

				for(auto &it : redis->redisShards)
				{
					auto &mu = it.mtx;
					auto &map = it.redis;
					std::unique_lock <std::mutex> lck(mu);
					
					for(auto &iter : map)
					{
						if(iter->type == OBJ_STRING)
						{
							
						}

					}
					
				}

				migratingSlosTos.erase(ipPort);
				clear();

				LOG_INFO << "cluster migrate success " << conn->ip << " " << conn->port;
			}

		}
		else
		{
			conn->forceClose();
			LOG_INFO<<"cluster migrate failure " <<conn->ip<<" " <<conn->port;
			break;
		}

		recvBuf->retrieve(sdslen(redis->object.ok->ptr));

	}
}


void xCluster::clusterRedirectClient(const xSeesionPtr &session, xClusterNode * n, int32_t hashSlot, int32_t errCode)
{
	if (errCode == CLUSTER_REDIR_CROSS_SLOT)
	{
		redis->object.addReplySds(session->sendBuf, sdsnew("-CROSSSLOT Keys in request don't hash to the same slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_UNSTABLE)
	{
		redis->object.addReplySds(session->sendBuf,  sdsnew("-TRYAGAIN Multiple keys request during rehashing of slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_STATE)
	{
		redis->object.addReplySds(session->sendBuf,  sdsnew("-CLUSTERDOWN The cluster is down\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_UNBOUND)
	{
		redis->object.addReplySds(session->sendBuf, sdsnew("-CLUSTERDOWN Hash slot not served\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_MOVED ||
		errCode == CLUSTER_REDIR_ASK)
	{
		redis->object.addReplySds(session->sendBuf,  sdscatprintf(sdsempty(),
			"-%s %d %s:%d\r\n",
			(errCode == CLUSTER_REDIR_ASK) ? "ASK" : "MOVED",
			hashSlot, n->ip.c_str(), n->port));
	}
	else
	{
		LOG_WARN << "getNodeByQuery() unknown error.";
	}
} 


void xCluster::syncClusterSlot()
{
    for (auto &it : redis->clustertcpconnMaps)
    {
        redis->structureRedisProtocol(sendBuf, deques);
        it.second->send(&sendBuf);
    }

    redis->clearDeques(deques);
    clear();

}

uint32_t xCluster::keyHashSlot(char *key, int32_t keylen)
{
	int32_t s, e; /* start-end indexes of { and } */

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

int32_t xCluster::getSlotOrReply(const xSeesionPtr &session,rObj * o)
{
	long long slot;

	if (redis->object.getLongLongFromObject(o, &slot) != REDIS_OK ||
		slot < 0 || slot >= CLUSTER_SLOTS)
	{
		redis->object.addReplyError(session->sendBuf, "Invalid or out of range slot");
		return  REDIS_ERR;
	}
	return (int32_t)slot;
}

void xCluster::structureProtocolSetCluster(std::string ip, int16_t port, xBuffer &sendBuf,const xTcpconnectionPtr & conn)
{
      deques.push_back(redis->object.cluster);
	deques.push_back(redis->object.connect);

	char buf[8];
	int32_t len = ll2string(buf, sizeof(buf), port);
	deques.push_back(redis->object.createStringObject((char*)(ip.c_str()), ip.length()));
	deques.push_back(redis->object.createStringObject(buf, len));
	redis->structureRedisProtocol(sendBuf, deques);
	conn->send(&sendBuf);
	redis->clearDeques(deques);
	clear();
}


void xCluster::delClusterImport(std::deque<rObj*> &robj)
{
	for (auto &it : redis->clustertcpconnMaps)
	{
		redis->structureRedisProtocol(sendBuf, robj);
		it.second->send(&sendBuf);
		clear();
	}

	for (auto &it : robj)
	{
		zfree(it);
	}
}



void xCluster::clear()
{
    uset.clear();
    deques.clear();
    sendBuf.retrieveAll();
}

bool  xCluster::replicationToNode(const xSeesionPtr &session,const std::string &ip,int16_t port)
{
  	clear();
	xTcpconnectionPtr conn;
	bool mark = false;
	std::string ipPort = ip + "::" + std::to_string(port);
	{
	    std::unique_lock <std::mutex> lck(redis->clusterMutex);
        if(!getSlotSet(ipPort))
        {
            return false;
        }

        for(auto &iter : redis->clustertcpconnMaps)
        {
            if(iter.second->ip == ip && iter.second->port == port)
            {
                conn = iter.second;
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
	for (auto &it : threadPoolVec)
	{
		if (session->conn->getLoop()->getThreadId() == it->getThreadId())
		{
			continue;
		}

		it->runInLoop(std::bind(&xRedis::handleForkTimeOut, redis));
	}

	if(redis->threadCount > 1)
	{
	    std::unique_lock <std::mutex> lck(redis->forkMutex);
	    while (redis->forkCondWaitCount < redis->threadCount)
	    {
	        redis->expireCondition.wait(lck);
	    }
	}

   	LOG_INFO<<"sendPipe cluster sync success .........";


	redis->forkCondWaitCount = 0;
	redis->clusterRepliMigratEnabled = true;

	if(redis->threadCount > 1)
	{
		redis->forkCondition.notify_all();
	}


     {
        std::unique_lock <std::mutex> lck(redis->clusterMutex);
        for (auto &it : redis->clustertcpconnMaps)
        {
            it.second->setMessageCallback(std::bind(&xCluster::readCallBack, this, std::placeholders::_1, std::placeholders::_2));
        }

        deques.push_back(redis->object.cluster);
        deques.push_back(redis->object.setslot);
        deques.push_back(redis->object.node);
        deques.push_back(redis->object.createStringObject((char*)redis->ipPort.c_str(),redis->ipPort.length()));
        deques.push_back(redis->object.createStringObject((char*)ipPort.c_str(),ipPort.length()));

        if(!getSlotSet(ipPort))
        {
            return false;
        }

        for(auto &it : uset)
        {
            char buf[8];
            long long  len = ll2string(buf, sizeof(buf), it);
            deques.push_back(redis->object.createStringObject(buf, len));
        }

        syncClusterSlot();
    }


	LOG_INFO<<"cluster send  success";
	
}



bool xCluster::getSlotSet(const std::string &ipPort)
{

    auto it = migratingSlosTos.find(ipPort);
    assert(it->second.size());
    if(it == migratingSlosTos.end())
    {
        LOG_INFO<<"migratingSlosTos not found " << ipPort;
        return false;
    }

    uset = it->second;
    return true;
}


void xCluster::connCallBack(const xTcpconnectionPtr& conn)
{
	if (conn->connected())
	{
		isConnect = true;
		state = false;
		{
			std::unique_lock <std::mutex> lck(cmtex);
			condition.notify_one();
		}

		socket.getpeerName(conn->getSockfd(), &(conn->ip), conn->port);

		{
			std::unique_lock <std::mutex> lck(redis->clusterMutex);
			for (auto &it : redis->clustertcpconnMaps)
			{
				structureProtocolSetCluster(it.second->ip, it.second->port, sendBuf,conn);
			}

			structureProtocolSetCluster(redis->ip, redis->port, sendBuf,conn);
			redis->clustertcpconnMaps.insert(std::make_pair(conn->getSockfd(), conn));
		}

		xSeesionPtr  session(new xSession(redis, conn));
		{
			std::unique_lock <std::mutex> lck(redis->mtx);
			redis->sessions[conn->getSockfd()] = session;
		}

		LOG_INFO <<"connect cluster success "<<"ip:"<<conn->ip<<" port:"<<conn->port;

	}
	else
	{
		{
			std::unique_lock <std::mutex> lck(redis->clusterMutex);
			redis->clustertcpconnMaps.erase(conn->getSockfd());
			for (auto it = tcpvectors.begin(); it != tcpvectors.end(); ++it)
			{
				if ((*it)->connection->ip == conn->ip && (*it)->connection->port == conn->port)
				{
					tcpvectors.erase(it);
					break;
				}
			} 
			eraseClusterNode(conn->ip,conn->port);
			migratingSlosTos.erase(conn->ip + std::to_string(conn->port));
			importingSlotsFrom.erase(conn->ip + std::to_string(conn->port));
		}

		LOG_INFO << "disconnect cluster "<<"ip:"<<conn->ip<<" port:"<<conn->port;
	}
}


xClusterNode  *xCluster::checkClusterSlot(int32_t slot)
{
	auto it = clusterSlotNodes.find(slot);
	if(it == clusterSlotNodes.end())
	{
		return nullptr;
	}
	return &(it->second);
}



void xCluster::eraseMigratingNode(const std::string &name)
{
	
}

void xCluster::eraseImportingNode(const std::string &name)
{

}

void xCluster::addSlotDeques(rObj * slot,const std::string &name)
{
	deques.push_back(redis->object.cluster);
	deques.push_back(redis->object.addsync);
	deques.push_back(redis->object.createStringObject(slot->ptr, sdslen(slot->ptr)));
	deques.push_back(redis->object.rIp);
	deques.push_back(redis->object.rPort);
	deques.push_back(redis->object.createStringObject((char*)(name.c_str()), name.length()));
}

void xCluster::delSlotDeques(rObj * obj,int32_t slot)
{
	deques.push_back(redis->object.cluster);
	deques.push_back(redis->object.delsync);
	deques.push_back(redis->object.createStringObject(obj->ptr, sdslen(obj->ptr)));
	clusterSlotNodes.erase(slot);
}


sds xCluster::showClusterNodes()
{
	sds ci = sdsempty(), ni = sdsempty();
	{
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		for (auto &it : clusterSlotNodes)
		{
			ni = sdscatprintf(sdsempty(), "%s %s:%d slot:",it.second.name.c_str(),it.second.ip.c_str(),it.second.port);
			ci = sdscatsds(ci, ni);
			sdsfree(ni);
			ni = sdscatprintf(sdsempty(), "%d ",it.first);
			ci = sdscatsds(ci, ni);
			sdsfree(ni);
			ci = sdscatlen(ci, "\n", 1);
		}
	}

	ci = sdscatlen(ci, "\n", 1);
	return ci;
}

void xCluster::getKeyInSlot(int32_t hashslot,std::vector<rObj*> &keys ,int32_t count)
{
	int32_t j = 0;
	for(auto &it : redis->redisShards)
	{
		auto &mu = it.mtx;
		auto &map = it.redis;
		auto &setMap = it.setMap;
		std::unique_lock <std::mutex> lck(mu);
		for (auto &iter : map)
		{
			if(iter->type == OBJ_STRING)
			{
				auto iterr = setMap.find(iter);
			#ifdef __DEBUG__
				assert(iterr != setMap.end());
			#endif
				uint32_t slot = keyHashSlot(iter->ptr,sdslen(iter->ptr));
				if(slot == hashslot)
				{
					keys.push_back(redis->object.createRawStringObject(iter->ptr,sdslen(iter->ptr)));
					if(--count == 0)
					{
						return ;
					}
				}
			}
		}
	}


}


void xCluster::eraseImportSlot(int32_t slot)
{

}


void xCluster::eraseClusterNode(int32_t slot)
{
	auto it = clusterSlotNodes.find(slot);
	assert(it != clusterSlotNodes.end());
	clusterSlotNodes.erase(slot);
}


void xCluster::eraseClusterNode(const std::string &ip ,int16_t port)
{
	for(auto it = clusterSlotNodes.begin(); it != clusterSlotNodes.end(); )
	{
		if(ip == it->second.ip && port == it->second.port)
		{
			clusterSlotNodes.erase(it++);
			continue;
		}

		++it;
	}

}


bool  xCluster::connSetCluster(const char *ip, int16_t port)
{
	std::shared_ptr<xTcpClient> client(new xTcpClient(loop, this));
	client->setConnectionCallback(std::bind(&xCluster::connCallBack, this, std::placeholders::_1));
	client->setMessageCallback(std::bind(&xCluster::readCallBack, this, std::placeholders::_1, std::placeholders::_2));
	client->setConnectionErrorCallBack(std::bind(&xCluster::connErrorCallBack, this));
	client->connect(ip, port);
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

void xCluster::reconnectTimer(const std::any &context)
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




