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
	clusterNode node;
	node.name = name;
	node.configEpoch = 0;
	node.createTime = time(0);
	node.ip = ip;
	node.port = port;
	node.master = nullptr;
	node.slaves = nullptr;
	clusterSlotNodes.insert(std::make_pair(slot, node));

}

void xCluster::readCallBack(const TcpConnectionPtr &conn, xBuffer *recvBuf)
{
	while (recvBuf->readableBytes() > 0)
	{
		if (!memcmp(recvBuf->peek(), redis->object.ok->ptr, sdslen(redis->object.ok->ptr)))
		{
			LOG_INFO<<"reply to cluster ok";

			std::unique_lock <std::mutex> lck(redis->clusterMutex);
			if (++replyCount == redis->clusterConns.size())
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

				for (auto &it : redis->clusterConns)
				{
					SessionPtr session(new xSession(redis, conn));
					std::unique_lock <std::mutex> lck(redis->mtx);
					redis->sessions[conn->getSockfd()] = session;
				}


				std::string ipPort = conn->ip + "::" + std::to_string(conn->port);
				
				for(auto &it : slotSets)
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


void xCluster::clusterRedirectClient(const SessionPtr &session, clusterNode *n, int32_t hashSlot, int32_t errCode)
{
	if (errCode == CLUSTER_REDIR_CROSS_SLOT)
	{
		redis->object.addReplySds(session->clientBuffer, sdsnew("-CROSSSLOT Keys in request don't hash to the same slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_UNSTABLE)
	{
		redis->object.addReplySds(session->clientBuffer,  sdsnew("-TRYAGAIN Multiple keys request during rehashing of slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_STATE)
	{
		redis->object.addReplySds(session->clientBuffer,  sdsnew("-CLUSTERDOWN The cluster is down\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_UNBOUND)
	{
		redis->object.addReplySds(session->clientBuffer, sdsnew("-CLUSTERDOWN Hash slot not served\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_MOVED ||
		errCode == CLUSTER_REDIR_ASK)
	{
		redis->object.addReplySds(session->clientBuffer,  sdscatprintf(sdsempty(),
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
    for (auto &it : redis->clusterConns)
    {
        redis->structureRedisProtocol(buffer, commands);
        it.second->send(&buffer);
    }

    redis->clearCommand(commands);
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

int32_t xCluster::getSlotOrReply(const SessionPtr &session,rObj * o)
{
	long long slot;

	if (redis->object.getLongLongFromObject(o, &slot) != REDIS_OK ||
		slot < 0 || slot >= CLUSTER_SLOTS)
	{
		redis->object.addReplyError(session->clientBuffer, "Invalid or out of range slot");
		return  REDIS_ERR;
	}
	return (int32_t)slot;
}

void xCluster::structureProtocolSetCluster(std::string ip, int16_t port, xBuffer &buffer,const TcpConnectionPtr &conn)
{
    commands.push_back(redis->object.cluster);
	commands.push_back(redis->object.connect);

	char buf[8];
	int32_t len = ll2string(buf, sizeof(buf), port);
	commands.push_back(redis->object.createStringObject((char*)(ip.c_str()), ip.length()));
	commands.push_back(redis->object.createStringObject(buf, len));
	redis->structureRedisProtocol(buffer, commands);
	conn->send(&buffer);
	redis->clearCommand(commands);
	clear();
}


void xCluster::delClusterImport(std::deque<rObj*> &robj)
{
	for (auto &it : redis->clusterConns)
	{
		redis->structureRedisProtocol(buffer, robj);
		it.second->send(&buffer);
		clear();
	}

	for (auto &it : robj)
	{
		zfree(it);
	}
}


bool xCluster::getKeySlot(const std::string &name)
{
	auto it = migratingSlosTos.find(std::move(name));
	if (it == migratingSlosTos.end())
	{
		return false;
	}
	return true;
}

void xCluster::clear()
{
	clusterDelCopys.clear();
	clusterDelkeys.clear();
	slotSets.clear();
	commands.clear();
	buffer.retrieveAll();
}

bool  xCluster::replicationToNode(const std::deque<rObj*> &obj, const SessionPtr &session,
	const std::string &ip, int16_t port, int8_t copy, int8_t replace,int32_t numKeys,int32_t firstKey)
{
	clear();

	TcpConnectionPtr clusterConn;
	int8_t count;

	{
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		for (auto &it : redis->clusterConns)
		{
			if (it.second->ip == ip && it.second->port == port)
			{
				clusterConn = it.second;
				count++;
			}
		}

		for (auto &it : clusterSlotNodes)
		{
			if (it.second.ip == ip && it.second.port == port)
			{
				std::string nodeName = it.second.name;
				auto it = migratingSlosTos.find(std::move(nodeName));
				if (it != migratingSlosTos.end())
				{
					slotSets = it->second;
					count++;
				}
			}
		}
	}

	if (count != 2)
	{
		return false;
	}

	if (copy == 0)
	{
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		for (int j = 0; j < numKeys; j++)
		{
			clusterDelkeys.push_back(redis->object.createStringObject(obj[firstKey + j]->ptr, sdslen(obj[firstKey + j]->ptr)));
		}
	}

	if (!strcasecmp(obj[2]->ptr, ""))
	{
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		for (auto &it : clusterDelkeys)
		{
			char *key = it->ptr;
			int32_t hashslot = redis->clus.keyHashSlot(key, sdslen(key));
			auto iter = slotSets.find(hashslot);
			if (iter == slotSets.end())
			{
				return false;
			}

			rObj * dump = redis->createDumpPayload(it);
			if (dump == nullptr)
			{
				return false;
			}

			clusterConn->sendPipe(dump->ptr);
		}
	}
	else
	{
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		char *key = obj[2]->ptr;
		int32_t hashslot = redis->clus.keyHashSlot(key, sdslen(key));
		auto iter = slotSets.find(hashslot);
		if (iter == slotSets.end())
		{
			return false;
		}

		rObj * dump = redis->createDumpPayload(obj[2]);
		if (dump == nullptr)
		{
			return false;
		}

		clusterConn->sendPipe(dump->ptr);
	}



	LOG_INFO<<"cluster send  success";
	
}

void xCluster::connCallBack(const TcpConnectionPtr& conn)
{
	if (conn->connected())
	{
		isConnect = true;
		state = false;
		{
			std::unique_lock <std::mutex> lck(mtx);
			condition.notify_one();
		}

		socket.getpeerName(conn->getSockfd(), &(conn->ip), conn->port);

		{
			std::unique_lock <std::mutex> lck(redis->clusterMutex);
			for (auto &it : redis->clusterConns)
			{
				structureProtocolSetCluster(it.second->ip, it.second->port, buffer,conn);
			}

			structureProtocolSetCluster(redis->ip, redis->port, buffer,conn);
			redis->clusterConns.insert(std::make_pair(conn->getSockfd(), conn));
		}

		SessionPtr  session(new xSession(redis, conn));
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
			redis->clusterConns.erase(conn->getSockfd());
			for (auto it = clusterConns.begin(); it != clusterConns.end(); ++it)
			{
				if ((*it)->getConnection()->ip == conn->ip && (*it)->getConnection()->port == conn->port)
				{
					clusterConns.erase(it);
					break;
				}
			} 
			eraseClusterNode(conn->ip,conn->port);
			migratingSlosTos.erase(conn->ip + std::to_string(conn->port));
			importingSlotsFroms.erase(conn->ip + std::to_string(conn->port));
		}

		LOG_INFO << "disconnect cluster "<<"ip:"<<conn->ip<<" port:"<<conn->port;
	}
}


clusterNode  *xCluster::checkClusterSlot(int32_t slot)
{
	auto it = clusterSlotNodes.find(slot);
	if(it == clusterSlotNodes.end())
	{
		return nullptr;
	}
	return &(it->second);
}

void xCluster::eraseMigratingSlot(const std::string &name)
{
	migratingSlosTos.erase(name);
}

void xCluster::eraseImportingSlot(const std::string &name)
{
	importingSlotsFroms.erase(name);
}

void xCluster::addSlotDeques(rObj * slot,const std::string &name)
{
	commands.push_back(redis->object.cluster);
	commands.push_back(redis->object.addsync);
	commands.push_back(redis->object.createStringObject(slot->ptr, sdslen(slot->ptr)));
	commands.push_back(redis->object.rIp);
	commands.push_back(redis->object.rPort);
	commands.push_back(redis->object.createStringObject((char*)(name.c_str()), name.length()));
}

void xCluster::delSlotDeques(rObj * obj,int32_t slot)
{
	commands.push_back(redis->object.cluster);
	commands.push_back(redis->object.delsync);
	commands.push_back(redis->object.createStringObject(obj->ptr, sdslen(obj->ptr)));
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
		auto &stringMap = it.stringMap;
		auto &hashMap = it.hashMap;
		auto &zsetMap = it.zsetMap;
		auto &setMap = it.setMap;
		auto &listMap = it.listMap;

		std::unique_lock <std::mutex> lck(mu);
		for (auto &iter : map)
		{
			if(iter->type == OBJ_STRING)
			{
				auto iterr = stringMap.find(iter);
			#ifdef __DEBUG__
				assert(iterr != stringMap.end());
				assert(iterr->first->type == iter->type);
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
			else if(iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
			#ifdef __DEBUG__
				assert(iterr != hashMap.end());
				assert(iterr->first->type == iter->type);
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
			else if(iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
			#ifdef __DEBUG__
				assert(iterr != listMap.end());
				assert(iterr->first->type == iter->type);
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
			else if(iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
			#ifdef __DEBUG__
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == iter->type);
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
			else if(iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
			#ifdef __DEBUG__
				assert(iterr != setMap.end());
				assert(iterr->first->type == iter->type);
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
			else
			{
				LOG_ERROR<<"unknown type error "<<iter->type;
			#ifdef __DEBUG__
				assert(false);
			#endif
			}
		}
	}


}



void xCluster::eraseClusterNode(int32_t slot)
{
	auto it = clusterSlotNodes.find(slot);
	assert(it != clusterSlotNodes.end());
	clusterSlotNodes.erase(slot);
}


void xCluster::eraseClusterNode(const std::string &ip ,int16_t port)
{
	for(auto it = clusterSlotNodes.begin(); it != clusterSlotNodes.end();)
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
	clusterConns.push_back(client);

	std::unique_lock <std::mutex> lck(mtx);
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
		std::unique_lock <std::mutex> lck(mtx);
		condition.notify_one();
	}
}




