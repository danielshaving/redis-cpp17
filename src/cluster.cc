#include "cluster.h"
#include "redis.h"
#include "log.h"
#include "util.h"

Cluster::Cluster(Redis *redis)
:redis(redis),
state(true),
isConnect(false),
replyCount(0)
{

}

Cluster::~Cluster()
{

}

void Cluster::cretateClusterNode(int32_t slot,const std::string &ip,
		int16_t port,const std::string &name)
{
	ClusterNode node;
	node.name = name;
	node.configEpoch = 0;
	node.createTime = time(0);
	node.ip = ip;
	node.port = port;
	node.master = nullptr;
	node.slaves = nullptr;
	clusterSlotNodes.insert(std::make_pair(slot,node));
}

void Cluster::readCallback(const TcpConnectionPtr &conn,Buffer *buffer)
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

					Buffer buffer;
					buffer.swap(redis->clusterMigratCached);
				}

				replyCount = 0;

				for (auto &it : clusterConn)
				{
					SessionPtr session(new Session(redis,conn));
					std::unique_lock <std::mutex> lck(redis->getMutex());
					auto &sessions = redis->getSession();
					sessions[conn->getSockfd()] = session;
					auto &sessionConns = redis->getSessionConn();
					sessionConns[conn->getSockfd()] = conn;
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

void Cluster::clusterRedirectClient(const TcpConnectionPtr &conn,const SessionPtr &session,
		ClusterNode *n,int32_t hashSlot,int32_t errCode)
{
	if (errCode == CLUSTER_REDIR_CROSS_SLOT)
	{
		addReplySds(conn->outputBuffer(),
				sdsnew("-CROSSSLOT Keys in request don't hash to the same slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_UNSTABLE)
	{
		addReplySds(conn->outputBuffer(),
				sdsnew("-TRYAGAIN Multiple keys request during rehashing of slot\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_STATE)
	{
		addReplySds(conn->outputBuffer(),sdsnew("-CLUSTERDOWN The cluster is down\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_DOWN_UNBOUND)
	{
		addReplySds(conn->outputBuffer(),sdsnew("-CLUSTERDOWN Hash slot not served\r\n"));
	}
	else if (errCode == CLUSTER_REDIR_MOVED ||
		errCode == CLUSTER_REDIR_ASK)
	{
		addReplySds(conn->outputBuffer(),sdscatprintf(sdsempty(),
			"-%s %d %s:%d\r\n",
			(errCode == CLUSTER_REDIR_ASK) ? "ASK" : "MOVED",
			hashSlot,n->ip.c_str(),n->port));
	}
	else
	{
		LOG_WARN << "getNodeByQuery unknown error.";
	}
} 


void Cluster::syncClusterSlot()
{
	auto clusterConn = redis->getClusterConn();
    for (auto &it : clusterConn)
    {
        redis->structureRedisProtocol(buffer,redisCommands);
        it.second->send(&buffer);
    }

    redis->clearCommand(redisCommands);
    clear();
}

uint32_t Cluster::keyHashSlot(char *key,int32_t keylen)
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

int32_t Cluster::getSlotOrReply(const SessionPtr &session,const RedisObjectPtr &o,const TcpConnectionPtr &conn)
{
	int64_t slot;

	if (getLongLongFromObject(o,&slot) != REDIS_OK ||
		slot < 0 || slot >= CLUSTER_SLOTS)
	{
		addReplyError(conn->outputBuffer(),"Invalid or out of range slot");
		return  REDIS_ERR;
	}
	return (int32_t)slot;
}

void Cluster::structureProtocolSetCluster(std::string ip,int16_t port,
		Buffer &buffer,const TcpConnectionPtr &conn)
{
    redisCommands.push_back(shared.cluster);
	redisCommands.push_back(shared.clusterconnect);

	char buf[32];
	int32_t len = ll2string(buf,sizeof(buf),port);
	redisCommands.push_back(createStringObject(ip.data(),ip.length()));
	redisCommands.push_back(createStringObject(buf,len));
	redis->structureRedisProtocol(buffer,redisCommands);
	conn->send(&buffer);
	redis->clearCommand(redisCommands);
	clear();
}

void Cluster::delClusterImport(std::deque<RedisObjectPtr> &robj)
{
	auto &clusterConn = redis->getClusterConn();
	for (auto &it : clusterConn)
	{
		redis->structureRedisProtocol(buffer,robj);
		it.second->send(&buffer);
		clear();
	}

	robj.clear();
}

bool Cluster::getKeySlot(const std::string &name)
{
	auto it = migratingSlosTos.find(std::move(name));
	if (it == migratingSlosTos.end())
	{
		return false;
	}
	return true;
}

void Cluster::clear()
{
	clusterDelCopys.clear();
	clusterDelKeys.clear();
	slotSets.clear();
	redisCommands.clear();
	buffer.retrieveAll();
}

bool Cluster::replicationToNode(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session,
	const std::string &ip,int16_t port,int8_t copy,int8_t replace,int32_t numKeys,int32_t firstKey)
{
	clear();

	TcpConnectionPtr conn;
	int8_t count = 0;

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
			clusterDelKeys.push_back(
					createRawStringObject(obj[firstKey + j]->type,
							obj[firstKey + j]->ptr,sdslen(obj[firstKey + j]->ptr)));
		}
	}

	if (!strcmp(obj[2]->ptr, ""))
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

			const RedisObjectPtr & dump = redis->createDumpPayload(it);
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

		const RedisObjectPtr &dump = redis->createDumpPayload(obj[2]);
		if (dump == nullptr)
		{
			return false;
		}
		conn->sendPipe(dump->ptr);
	}
	return true;
}

void Cluster::connCallback(const TcpConnectionPtr &conn)
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
		auto addr = Socket::getPeerAddr(conn->getSockfd());
		Socket::toIp(buf,sizeof(buf),(const struct sockaddr *)&addr);
		Socket::toPort(&port,(const struct sockaddr *)&addr);
		conn->setip(buf);
		conn->setport(port);

		{
			std::unique_lock <std::mutex> lck(redis->getClusterMutex());
			auto &clusterConn = redis->getClusterConn();
			for (auto &it : clusterConn)
			{
				structureProtocolSetCluster(it.second->getip(),
						it.second->getport(),buffer,conn);
			}

			structureProtocolSetCluster(redis->getIp(),redis->getPort(),buffer,conn);
			redis->getClusterConn().insert(std::make_pair(conn->getSockfd(), conn));
		}

		SessionPtr session(new Session(redis,conn));
		{
			std::unique_lock <std::mutex> lck(redis->getMutex());
			auto &sessions = redis->getSession();
			sessions[conn->getSockfd()] = session;

			auto &sessionConns = redis->getSessionConn();
			sessionConns[conn->getSockfd()] = conn;
		}
		LOG_INFO <<"connect cluster success "<<"ip:"<<conn->getip()<<" port:"<<conn->getport();
	}
	else
	{
		redis->clearSessionState(conn->getSockfd());
		{
			std::unique_lock <std::mutex> lck(redis->getClusterMutex());
			redis->getClusterConn().erase(conn->getSockfd());
			eraseClusterNode(conn->getip(),conn->getport());
			migratingSlosTos.erase(conn->getip() + std::to_string(conn->getport()));
			importingSlotsFroms.erase(conn->getip() + std::to_string(conn->getport()));

			for (auto it = clusterConns.begin(); it != clusterConns.end(); ++it)
			{
				if ((*it)->getConnection()->getip() ==
						conn->getip() && (*it)->getConnection()->getport() == conn->getport())
				{
					it = clusterConns.erase(it);
					break;
				}
			}
		}

		LOG_INFO << "disconnect cluster "<<"ip:"<<conn->getip()<<" port:"<<conn->getport();
	}
}

ClusterNode *Cluster::checkClusterSlot(int32_t slot)
{
	auto it = clusterSlotNodes.find(slot);
	if(it == clusterSlotNodes.end())
	{
		return nullptr;
	}
	return &(it->second);
}

void Cluster::eraseMigratingSlot(const std::string &name)
{
	migratingSlosTos.erase(name);
}

void Cluster::eraseImportingSlot(const std::string &name)
{
	importingSlotsFroms.erase(name);
}

void Cluster::addSlotDeques(const RedisObjectPtr &slot,std::string name)
{
	redisCommands.push_back(shared.cluster);
	redisCommands.push_back(shared.addsync);
	redisCommands.push_back(createStringObject(slot->ptr,sdslen(slot->ptr)));
	redisCommands.push_back(shared.rIp);
	redisCommands.push_back(shared.rPort);
	redisCommands.push_back(createStringObject(name.data(),name.length()));
}

void Cluster::delSlotDeques(const RedisObjectPtr &obj,int32_t slot)
{
	redisCommands.push_back(shared.cluster);
	redisCommands.push_back(shared.delsync);
	redisCommands.push_back(createStringObject(obj->ptr,sdslen(obj->ptr)));
	clusterSlotNodes.erase(slot);
}

sds Cluster::showClusterNodes()
{
	sds ci = sdsempty(), ni = sdsempty();
	{
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		for (auto &it : clusterSlotNodes)
		{
			ni = sdscatprintf(sdsempty(),"%s %s:%d slot:",
					it.second.name.c_str(),it.second.ip.c_str(),it.second.port);
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

void Cluster::getKeyInSlot(int32_t hashslot,std::vector<RedisObjectPtr> &keys,int32_t count)
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
					keys.push_back(createRawStringObject(OBJ_STRING,iter->ptr,sdslen(iter->ptr)));
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
					keys.push_back(createRawStringObject(OBJ_HASH,iter->ptr,sdslen(iter->ptr)));
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
					keys.push_back(createRawStringObject(OBJ_LIST,iter->ptr,sdslen(iter->ptr)));
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
					keys.push_back(createRawStringObject(OBJ_ZSET,iter->ptr,sdslen(iter->ptr)));
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
					keys.push_back(createRawStringObject(OBJ_SET,iter->ptr,sdslen(iter->ptr)));
					if(--count == 0)
					{
						return ;
					}
				}
			}
			else
			{
				assert(false);
			}
		}
	}
}

void Cluster::eraseClusterNode(int32_t slot)
{
	auto it = clusterSlotNodes.find(slot);
	assert(it != clusterSlotNodes.end());
	clusterSlotNodes.erase(slot);
}

void Cluster::eraseClusterNode(const std::string &ip,int16_t port)
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

bool Cluster::connSetCluster(const char *ip,int16_t port)
{
	TcpClientPtr client(new TcpClient(loop,ip,port,this));
	client->setConnectionCallback(std::bind(&Cluster::connCallback,
			this,std::placeholders::_1));
	client->setMessageCallback(std::bind(&Cluster::readCallback,
			this,std::placeholders::_1, std::placeholders::_2));
	client->connect();
	if(state)
	{
		clusterConns.push_back(client);
	}
	return state;
}

void Cluster::connectCluster()
{
	EventLoop loop;
	this->loop = &loop;
	loop.run();
}

void Cluster::reconnectTimer(const std::any &context)
{
	LOG_INFO << "reconnect cluster";
}




