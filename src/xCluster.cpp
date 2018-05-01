#include "xCluster.h"
#include "xRedis.h"
#include "xLog.h"
#include "xCrc16.h"
#include "xUtil.h"

xCluster::xCluster(xRedis *redis)
:redis(redis),
state(true),
isConnect(false),
replyCount(0)
{

}

xCluster::~xCluster()
{

}

void xCluster::cretateClusterNode(int32_t slot,const std::string &ip,
		int16_t port,const std::string &name)
{
	clusterNode node;
	node.name = name;
	node.configEpoch = 0;
	node.createTime = time(0);
	node.ip = ip;
	node.port = port;
	node.master = nullptr;
	node.slaves = nullptr;
	clusterSlotNodes.insert(std::make_pair(slot,node));
}

void xCluster::readCallBack(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	while (buffer->readableBytes() > 0)
	{
		if (!memcmp(buffer->peek(),shared.ok->ptr,sdslen(shared.ok->ptr)))
		{
			LOG_INFO<<"reply to cluster ok";

			std::unique_lock <std::mutex> lck(redis->getClusterMutex());
			auto &clusterConn = redis->getClusterConn();
			if (++replyCount == clusterConn.size())
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

				for (auto &it : clusterConn)
				{
					SessionPtr session(new xSession(redis,conn));
					std::unique_lock <std::mutex> lck(redis->getMutex());
					auto &sessions = redis->getSession();
					sessions[conn->getSockfd()] = session;
				}

				std::string ip = conn->getip();
				std::string ipPort = ip + "::" + std::to_string(conn->getport());
				
				for(auto &it : slotSets)
				{
					auto iter = clusterSlotNodes.find(it);
					if(iter != clusterSlotNodes.end())
					{
						iter->second.ip = conn->getip();
						iter->second.port = conn->getport();
					}
					else
					{
						LOG_WARN<<"slot not found error";
					}

				}

				auto &redisShards = redis->getRedisShards();
				for(auto &it : redisShards)
				{
					auto &mu = it.mtx;
					auto &map = it.redisMap;
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
				LOG_INFO <<"cluster migrate success "<< conn->getip() <<" "<< conn->getport();
			}
		}
		else
		{
			conn->forceClose();
			LOG_INFO<<"cluster migrate failure "<<conn->getip()<<" "<<conn->getport();
			break;
		}
		buffer->retrieve(sdslen(shared.ok->ptr));
	}
}

void xCluster::clusterRedirectClient(const SessionPtr &session,clusterNode *n,int32_t hashSlot,int32_t errCode)
{
	if (errCode == CLUSTER_REDIR_CROSS_SLOT)
	{
		addReplySds(session->clientBuffer, sdsnew("-CROSSSLOT Keys in request don't hash to the same slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_UNSTABLE)
	{
		addReplySds(session->clientBuffer,sdsnew("-TRYAGAIN Multiple keys request during rehashing of slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_STATE)
	{
		addReplySds(session->clientBuffer,sdsnew("-CLUSTERDOWN The cluster is down\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_UNBOUND)
	{
		addReplySds(session->clientBuffer,sdsnew("-CLUSTERDOWN Hash slot not served\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_MOVED ||
		errCode == CLUSTER_REDIR_ASK)
	{
		addReplySds(session->clientBuffer,sdscatprintf(sdsempty(),
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
	auto clusterConn = redis->getClusterConn();
    for (auto &it : clusterConn)
    {
        redis->structureRedisProtocol(buffer, commands);
        it.second->send(&buffer);
    }

    redis->clearCommand(commands);
    clear();
}

uint32_t xCluster::keyHashSlot(char *key,int32_t keylen)
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

int32_t xCluster::getSlotOrReply(const SessionPtr &session,rObj *o)
{
	int64_t slot;

	if (getLongLongFromObject(o,&slot) != REDIS_OK ||
		slot < 0 || slot >= CLUSTER_SLOTS)
	{
		addReplyError(session->clientBuffer, "Invalid or out of range slot");
		return  REDIS_ERR;
	}
	return (int32_t)slot;
}

void xCluster::structureProtocolSetCluster(const std::string &ip,int16_t port,
		xBuffer &buffer,const TcpConnectionPtr &conn)
{
    commands.push_back(shared.cluster);
	commands.push_back(shared.clusterconnect);

	char buf[32];
	int32_t len = ll2string(buf,sizeof(buf),port);
	commands.push_back(createStringObject((char*)(ip.c_str()),ip.length()));
	commands.push_back(createStringObject(buf,len));
	redis->structureRedisProtocol(buffer,commands);
	conn->send(&buffer);
	redis->clearCommand(commands);
	clear();
}

void xCluster::delClusterImport(std::deque<rObj*> &robj)
{
	auto &clusterConn = redis->getClusterConn();
	for (auto &it : clusterConn)
	{
		redis->structureRedisProtocol(buffer,robj);
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
	clusterDelKeys.clear();
	slotSets.clear();
	commands.clear();
	buffer.retrieveAll();
}

bool xCluster::replicationToNode(const std::deque<rObj*> &obj,const SessionPtr &session,
	const std::string &ip,int16_t port,int8_t copy,int8_t replace,int32_t numKeys,int32_t firstKey)
{
	clear();

	TcpConnectionPtr conn;
	int8_t count;

	{
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		auto &clusterConn = redis->getClusterConn();
		for (auto &it : clusterConn)
		{
			if (it.second->getip() == ip && it.second->getport() == port)
			{
				conn = it.second;
				count++;
			}
		}

		for (auto &it : clusterSlotNodes)
		{
			if (it.second.ip== ip && it.second.port == port)
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
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		for (int j = 0; j < numKeys; j++)
		{
			clusterDelKeys.push_back(createStringObject(obj[firstKey + j]->ptr,sdslen(obj[firstKey + j]->ptr)));
		}
	}

	if (!strcasecmp(obj[2]->ptr, ""))
	{
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		for (auto &it : clusterDelKeys)
		{
			char *key = it->ptr;
			int32_t hashslot = keyHashSlot(key,sdslen(key));
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

			conn->sendPipe(dump->ptr);
		}
	}
	else
	{
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		char *key = obj[2]->ptr;
		int32_t hashslot = keyHashSlot(key,sdslen(key));
		auto iter = slotSets.find(hashslot);
		if (iter == slotSets.end())
		{
			return false;
		}

		rObj *dump = redis->createDumpPayload(obj[2]);
		if (dump == nullptr)
		{
			return false;
		}
		conn->sendPipe(dump->ptr);
	}
	LOG_INFO<<"cluster send  success";
}

void xCluster::connCallBack(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		isConnect = true;
		state = false;
		{
			std::unique_lock <std::mutex> lck(redis->getMutex());
			condition.notify_one();
		}

		char buf[64] = "";
		uint16_t port = 0;
		auto addr = socket.getPeerAddr(conn->getSockfd());
		socket.toIp(buf,sizeof(buf),(const struct sockaddr *)&addr);
		socket.toPort(&port,(const struct sockaddr *)&addr);
		conn->setip(buf);
		conn->setport(port);

		{
			std::unique_lock <std::mutex> lck(redis->getClusterMutex());
			auto &clusterConn = redis->getClusterConn();
			for (auto &it : clusterConn)
			{
				structureProtocolSetCluster(it.second->getip(),it.second->getport(),buffer,conn);
			}

			structureProtocolSetCluster(redis->getIp(),redis->getPort(),buffer,conn);
			redis->getClusterConn().insert(std::make_pair(conn->getSockfd(), conn));
		}

		SessionPtr session(new xSession(redis,conn));
		{
			std::unique_lock <std::mutex> lck(redis->getMutex());
			auto &sessions = redis->getSession();
			sessions[conn->getSockfd()] = session;
		}

		LOG_INFO <<"connect cluster success "<<"ip:"<<conn->getip()<<" port:"<<conn->getport();

	}
	else
	{
		{
			std::unique_lock <std::mutex> lck(redis->getClusterMutex());
			redis->getClusterConn().erase(conn->getSockfd());
			for (auto it = clusterConns.begin(); it != clusterConns.end(); ++it)
			{
				if ((*it)->getConnection()->getip() == conn->getip() && (*it)->getConnection()->getport() == conn->getport())
				{
					it = clusterConns.erase(it);
					break;
				}
			} 
			eraseClusterNode(conn->getip(),conn->getport());
			migratingSlosTos.erase(conn->getip() + std::to_string(conn->getport()));
			importingSlotsFroms.erase(conn->getip() + std::to_string(conn->getport()));
		}

		LOG_INFO << "disconnect cluster "<<"ip:"<<conn->getip()<<" port:"<<conn->getport();
	}
}


clusterNode *xCluster::checkClusterSlot(int32_t slot)
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

void xCluster::addSlotDeques(rObj *slot,const std::string &name)
{
	commands.push_back(shared.cluster);
	commands.push_back(shared.addsync);
	commands.push_back(createStringObject(slot->ptr,sdslen(slot->ptr)));
	commands.push_back(shared.rIp);
	commands.push_back(shared.rPort);
	commands.push_back(createStringObject((char*)(name.c_str()),name.length()));
}

void xCluster::delSlotDeques(rObj *obj,int32_t slot)
{
	commands.push_back(shared.cluster);
	commands.push_back(shared.delsync);
	commands.push_back(createStringObject(obj->ptr,sdslen(obj->ptr)));
	clusterSlotNodes.erase(slot);
}

sds xCluster::showClusterNodes()
{
	sds ci = sdsempty(), ni = sdsempty();
	{
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		for (auto &it : clusterSlotNodes)
		{
			ni = sdscatprintf(sdsempty(),"%s %s:%d slot:",it.second.name.c_str(),it.second.ip.c_str(),it.second.port);
			ci = sdscatsds(ci,ni);
			sdsfree(ni);
			ni = sdscatprintf(sdsempty(),"%d ",it.first);
			ci = sdscatsds(ci,ni);
			sdsfree(ni);
			ci = sdscatlen(ci, "\n", 1);
		}
	}

	ci = sdscatlen(ci, "\n", 1);
	return ci;
}

void xCluster::getKeyInSlot(int32_t hashslot,std::vector<rObj*> &keys,int32_t count)
{
	int32_t j = 0;
	auto &redisShards = redis->getRedisShards();
	for(auto &it : redisShards)
	{
		auto &mu = it.mtx;
		auto &map = it.redisMap;
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
				assert(iterr != stringMap.end());
				assert(iterr->first->type == iter->type);
				uint32_t slot = keyHashSlot(iter->ptr,sdslen(iter->ptr));
				if(slot == hashslot)
				{
					keys.push_back(createRawStringObject(iter->ptr,sdslen(iter->ptr)));
					if(--count == 0)
					{
						return ;
					}
				}
			}
			else if(iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
				assert(iterr != hashMap.end());
				assert(iterr->first->type == iter->type);
				uint32_t slot = keyHashSlot(iter->ptr,sdslen(iter->ptr));
				if(slot == hashslot)
				{
					keys.push_back(createRawStringObject(iter->ptr,sdslen(iter->ptr)));
					if(--count == 0)
					{
						return ;
					}
				}
			}
			else if(iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
				assert(iterr != listMap.end());
				assert(iterr->first->type == iter->type);
				uint32_t slot = keyHashSlot(iter->ptr,sdslen(iter->ptr));
				if(slot == hashslot)
				{
					keys.push_back(createRawStringObject(iter->ptr,sdslen(iter->ptr)));
					if(--count == 0)
					{
						return ;
					}
				}
			}
			else if(iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == iter->type);
				uint32_t slot = keyHashSlot(iter->ptr,sdslen(iter->ptr));
				if(slot == hashslot)
				{
					keys.push_back(createRawStringObject(iter->ptr,sdslen(iter->ptr)));
					if(--count == 0)
					{
						return ;
					}
				}
			}
			else if(iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
				assert(iterr != setMap.end());
				assert(iterr->first->type == iter->type);
				uint32_t slot = keyHashSlot(iter->ptr,sdslen(iter->ptr));
				if(slot == hashslot)
				{
					keys.push_back(createRawStringObject(iter->ptr,sdslen(iter->ptr)));
					if(--count == 0)
					{
						return ;
					}
				}
			}
			else
			{
				LOG_ERROR<<"unknown type error "<<iter->type;
				assert(false);
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

void xCluster::eraseClusterNode(const std::string &ip,int16_t port)
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

bool xCluster::connSetCluster(const char *ip,int16_t port)
{
	TcpClientPtr client(new xTcpClient(loop,ip,port,this));
	client->setConnectionCallback(std::bind(&xCluster::connCallBack,this,std::placeholders::_1));
	client->setMessageCallback(std::bind(&xCluster::readCallBack,this,std::placeholders::_1, std::placeholders::_2));
	client->asyncConnect();
	if(state)
	{
		clusterConns.push_back(client);
	}
	return state;
}

void xCluster::connectCluster()
{
	xEventLoop loop;
	this->loop = &loop;
	loop.run();
}

void xCluster::reconnectTimer(const std::any &context)
{
	LOG_INFO << "reconnect cluster";
}




