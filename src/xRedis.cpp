#include "xRedis.h"



xRedis::xRedis(const char * ip,int32_t port,int32_t threadCount,bool enbaledCluster)
:host(ip),
port(port),
threadCount(threadCount),
masterPort(0),
clusterEnabled(enbaledCluster),
slaveEnabled(false),
authEnabled(false),
salveCount(0),
threads(new std::thread(std::bind(&xReplication::connectMaster,&repli)))
{
	threads->detach();
	//loop.runAfter(10,nullptr,true,std::bind(&xRedis::handleTimeOut,this,std::placeholders::_1));
	rObj * obj = createStringObject("set",3);
	handlerCommondMap[obj] =std::bind(&xRedis::setCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("get",3);
	handlerCommondMap[obj] =std::bind(&xRedis::getCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("flushdb",7);
	handlerCommondMap[obj] =std::bind(&xRedis::flushdbCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("dbsize",6);
	handlerCommondMap[obj] =std::bind(&xRedis::dbsizeCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("hset",4);
	handlerCommondMap[obj] =std::bind(&xRedis::hsetCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("hget",4);
	handlerCommondMap[obj] =std::bind(&xRedis::hgetCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("hgetall",7);
	handlerCommondMap[obj] =std::bind(&xRedis::hgetallCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("ping",4);
	handlerCommondMap[obj] =std::bind(&xRedis::pingCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("save",4);
	handlerCommondMap[obj] =std::bind(&xRedis::saveCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("slaveof",7);
	handlerCommondMap[obj] =std::bind(&xRedis::slaveofCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("sync",4);
	handlerCommondMap[obj] =std::bind(&xRedis::syncCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("command",7);
	handlerCommondMap[obj] =std::bind(&xRedis::commandCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("config",6);
    handlerCommondMap[obj] = std::bind(&xRedis::configCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("auth",4);
    handlerCommondMap[obj] = std::bind(&xRedis::authCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("info",4);
	handlerCommondMap[obj] = std::bind(&xRedis::infoCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("echo",4);
	handlerCommondMap[obj] = std::bind(&xRedis::echoCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("client",5);
	handlerCommondMap[obj] = std::bind(&xRedis::clientCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("subscribe",9);
	handlerCommondMap[obj] = std::bind(&xRedis::subscribeCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("hkeys",5);
    handlerCommondMap[obj] = std::bind(&xRedis::hkeysCommond, this, std::placeholders::_1, std::placeholders::_2);
    obj = createStringObject("select",6);
    handlerCommondMap[obj] = std::bind(&xRedis::selectCommond, this, std::placeholders::_1, std::placeholders::_2);
    obj = createStringObject("sadd",4);
    handlerCommondMap[obj] = std::bind(&xRedis::saddCommond, this, std::placeholders::_1, std::placeholders::_2);
    obj = createStringObject("scard",6);
    handlerCommondMap[obj] = std::bind(&xRedis::scardCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("publish",7);
	handlerCommondMap[obj] = std::bind(&xRedis::publishCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("del",3);
	handlerCommondMap[obj] = std::bind(&xRedis::delCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("unsubscribe",11);
	handlerCommondMap[obj] = std::bind(&xRedis::unsubscribeCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("hlen",4);
	handlerCommondMap[obj] = std::bind(&xRedis::hlenCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("zadd",4);
	handlerCommondMap[obj] = std::bind(&xRedis::zaddCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("zcard",5);
	handlerCommondMap[obj] = std::bind(&xRedis::zcardCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("zrange",6);
	handlerCommondMap[obj] = std::bind(&xRedis::zrangeCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("zrank",5);
	handlerCommondMap[obj] = std::bind(&xRedis::zrankCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("zrevrange",9);
	handlerCommondMap[obj] = std::bind(&xRedis::zrevrangeCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("keys",4);
	handlerCommondMap[obj] = std::bind(&xRedis::keysCommond, this, std::placeholders::_1, std::placeholders::_2);


	obj = createStringObject("set",3);
	unorderedmapCommonds[obj] = 2;
	obj = createStringObject("hset",4);
	unorderedmapCommonds[obj] = 3;

	password.clear();
	createSharedObjects();
	loadDataFromDisk();
	server.init(&loop, host, port,this);
	server.setConnectionCallback(std::bind(&xRedis::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	server.setThreadNum(threadCount);
	server.start();

}

xRedis::~xRedis()
{
	destorySharedObjects();
	{
		for(auto it = handlerCommondMap.begin(); it!= handlerCommondMap.end(); it++)
		{
			zfree(it->first);
		}
		handlerCommondMap.clear();
	}

	{
		for(auto it = unorderedmapCommonds.begin(); it!= unorderedmapCommonds.end(); it++)
		{
			zfree(it->first);
		}
		unorderedmapCommonds.clear();
	}
	
	{
		for(auto it = setMapShards.begin(); it != setMapShards.end(); it++)
		{
			auto &map = (*it).setMap;
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto sit = map.begin(); sit !=map.end(); sit++)
			{
				zfree(sit->first);
				zfree(sit->second);
			}
			map.clear();
		}

	}

	{
		for(auto it = hsetMapShards.begin(); it != hsetMapShards.end(); it++)
		{
			auto &map = (*it).hsetMap;
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto sit = map.begin(); sit!=map.end(); sit++)
			{
				zfree(sit->first);
				auto  &mmap = sit->second;
				for(auto ssit = mmap.begin(); ssit!=mmap.end(); ssit++)
				{
					zfree(ssit->first);
					zfree(ssit->second);
				}
				mmap.clear();
			}
			map.clear();

		}
	}

}



void xRedis::handleTimeOut(void * data)
{
	loop.quit();
}

void xRedis::handleSalveRepliTimeOut(void * data)
{
	int32_t *sockfd = (int32_t *)data;
	{
		MutexLockGuard mu(slaveMutex);
		tcpconnMaps.erase(*sockfd);
		if(tcpconnMaps.size() == 0)
		{
			slaveCached.retrieveAll();
		}
	}
	LOG_INFO<<"sync connect repli  timeout ";
}


void xRedis::handleRepliCacheTimeOut()
{
	LOG_INFO<<"disconnect salve repli  timeout ";
}


void xRedis::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		socket.getpeerName(conn->getSockfd(),conn->host,conn->port);
		std::shared_ptr<xSession> session (new xSession(this,conn));
		MutexLockGuard mu(mutex);
		sessions[conn->getSockfd()] = session;
		LOG_INFO<<"Client connect success";
	}
	else
	{
		{
			MutexLockGuard mu(mutex);
			sessions.erase(conn->getSockfd());
		}

		{
			MutexLockGuard mu(slaveMutex);
			auto it =  tcpconnMaps.find(conn->getSockfd());
			if(it != tcpconnMaps.end())
			{
				tcpconnMaps.erase(conn->getSockfd());
			}
		}

		{
			for(auto it = pubSubShards.begin(); it != pubSubShards.end(); it++)
			{
				MutexLockGuard lock((*it).mutex);
				auto pubSub = (*it).pubSub;
				for(auto iter = pubSub.begin(); iter != pubSub.end(); iter++)
				{
					for(auto item  = iter->second.begin();  item != iter->second.end();)
					{
						if((*item)->getSockfd() == conn->getSockfd())
						{
							item = iter->second.erase(item);
							continue;
						}

						item++;
					}
				}
			}
		}
		LOG_INFO<<"Client disconnect";
	}
}


bool xRedis::deCodePacket(const xTcpconnectionPtr& conn,xBuffer *recvBuf,void  *data)
{
 	return true;
}


void xRedis::run()
{
	loop.run();
}


void xRedis::loadDataFromDisk()
{
	char rdb_filename[] = "dump.rdb";
	  
	if(rdbLoad(rdb_filename,this) == REDIS_OK)
	{
		LOG_INFO<<"load rdb success";
	}
	else if (errno != ENOENT)
	{
        LOG_WARN<<"Fatal error loading the DB:  Exiting."<<strerror(errno);
    }

}


bool xRedis::zrevrangeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  < 3  || obj.size() > 4)
		{
			addReplyErrorFormat(session->sendBuf,"unknown zrange  error");
			return REDIS_ERR;
		}

		obj[0]->calHash();
		size_t hash = obj[0]->hash;
		long long  begin = 0;
		string2ll(obj[1]->ptr,sdsllen(obj[1]->ptr),&begin);
		long long  end = 0;
		string2ll(obj[2]->ptr,sdsllen(obj[2]->ptr),&end);

		MutexLock &mu = sortSetShards[hash% kShards].mutex;
		auto &sset = sortSetShards[hash% kShards].sset;
		{
			MutexLockGuard lock(mu);
			for(int i = 0 ; i < begin; i ++)
			{
				auto iter = sset.rbegin();
				iter++;
			}

			if(end == -1)
			{
				end = sset.size();
			}

			addReplyMultiBulkLen(session->sendBuf,sset.size() * 2);
			int i  = 0 ;
			for(auto iter = sset.rbegin(); iter != sset.rend(); iter ++)
			{
				addReplyBulkCBuffer(session->sendBuf,(*iter).value->ptr,sdsllen((*iter).value->ptr));
				addReplyBulkCBuffer(session->sendBuf,(*iter).key->ptr,sdsllen((*iter).key->ptr));

				if(++i >= end)
				{
					break;
				}
			}
		}

		for(auto it  = obj.begin(); it != obj.end(); it ++)
		{
			zfree(*it);
		}

		return true;
}


bool xRedis::zaddCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() < 3  ||   (obj.size() - 1 ) % 2  != 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown zadd  error");
		return REDIS_ERR;
	}

	int count = 0;
	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	MutexLock &mu = sortSetShards[hash% kShards].mutex;
	auto &set = sortSetShards[hash% kShards].set;
	auto &sset = sortSetShards[hash% kShards].sset;
	{
		MutexLockGuard lock(mu);
		auto  iter = set.find(obj[0]);
		if(iter ==  set.end())
		{
			SetMap setMap;
			for(int i = 1; i < obj.size(); i +=2)
			{
				obj[i + 1]->calHash();
				auto it = setMap.find(obj[i + 1]);
				if(it  == setMap.end())
				{
					setMap.insert(std::make_pair(obj[i + 1],obj[i]));
					rSObj sobj;
					sobj.key = obj[i];
					sobj.value = obj[i + 1];
					sset.insert(std::move(sobj));
					count++;
					set.insert(std::make_pair(obj[0],std::move(setMap)));
				}
				else
				{
					{
						rSObj sobj;
						sobj.key = it->second;
						sobj.value =  it->first;
						sset.erase(std::move(sobj));
						setMap.erase(it);
						zfree(it->first);
						zfree(it->second);
					}

					{
						rSObj sobj;
						sobj.key = obj[i];
						sobj.value = obj[i + 1];
						sset.insert(std::move(sobj));
						setMap.insert(std::make_pair(obj[i],obj[i+1]));
					}
				}

			}
		}
		else
		{


			for(int i = 1; i < obj.size(); i +=2)
			{
				obj[i + 1]->calHash();
				auto it = iter->second.find(obj[i + 1]);
				if(it == iter->second.end())
				{
					count ++;
					iter->second.insert(std::make_pair(obj[i + 1],obj[i]));
					rSObj sobj;
					sobj.key = obj[i];
					sobj.value = obj[i + 1];
					sset.insert(std::move(sobj));
				}
				else
				{
					{
						rSObj sobj;
						sobj.key = it->second;
						sobj.value =  it->first;
						sset.erase(std::move(sobj));
						iter->second.erase(it);
						zfree(it->first);
						zfree(it->second);
					}

					{
						rSObj sobj;
						sobj.key = obj[i];
						sobj.value = obj[i + 1];
						sset.insert(std::move(sobj));
						iter->second.insert(std::make_pair(obj[i + 1],obj[i]));
					}
				}

			}
			zfree(obj[0]);
		}
	}

	addReplyLongLong(session->sendBuf,count);
	return true;
}

bool xRedis::zcardCommond(const std::deque <rObj*> & obj,xSession * session)
{
	return true;
}

bool xRedis::zcountCommond(const std::deque <rObj*> & obj,xSession * session)
{
	return true;
}


bool xRedis::keysCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1 )
	{
		addReplyErrorFormat(session->sendBuf,"unknown keys  error");
		return REDIS_ERR;
	}


	return true;
}

bool xRedis::zrangeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  < 3  || obj.size() > 4)
	{
		addReplyErrorFormat(session->sendBuf,"unknown zrange  error");
		return REDIS_ERR;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	long long  begin = 0;
	string2ll(obj[1]->ptr,sdsllen(obj[1]->ptr),&begin);
	long long  end = 0;
	string2ll(obj[2]->ptr,sdsllen(obj[2]->ptr),&end);

	MutexLock &mu = sortSetShards[hash% kShards].mutex;
	auto &sset = sortSetShards[hash% kShards].sset;
	{
		MutexLockGuard lock(mu);
		for(int i = 0 ; i < begin; i ++)
		{
			auto iter = sset.begin();
			iter++;
		}

		if(end == -1)
		{
			end = sset.size();
		}

		addReplyMultiBulkLen(session->sendBuf,sset.size() * 2);
		int i  = 0 ;
		for(auto iter = sset.begin(); iter != sset.end(); iter ++)
		{
			addReplyBulkCBuffer(session->sendBuf,(*iter).value->ptr,sdsllen((*iter).value->ptr));
			addReplyBulkCBuffer(session->sendBuf,(*iter).key->ptr,sdsllen((*iter).key->ptr));

			if(++i >= end)
			{
				break;
			}
		}
	}

	for(auto it  = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}

bool xRedis::zrankCommond(const std::deque <rObj*> & obj,xSession * session)
{
	return true;
}


bool xRedis::unsubscribeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown unsubscribe  error");
		return REDIS_ERR;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	MutexLock &mu = pubSubShards[hash% kShards].mutex;
	auto &pubSub = pubSubShards[hash% kShards].pubSub;
	{
		MutexLockGuard lock(mu);
		auto iter = pubSub.find(obj[0]);
		if(iter == pubSub.end())
		{
			addReply(session->sendBuf,shared.czero);
		}
		else
		{
			pubSub.erase(iter);
			zfree(iter->first);
			addReply(session->sendBuf,shared.ok);
		}
	}

	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}

bool xRedis::infoCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  < 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown info  error");
		return REDIS_ERR;
	}


	sds info = sdsempty();

	char hmem[64];
	char peak_hmem[64];
	char total_system_hmem[64];
	char used_memory_rss_hmem[64];
	char maxmemory_hmem[64];
	size_t zmalloc_used = zmalloc_used_memory();
	size_t total_system_mem =  zmalloc_get_memory_size();
	bytesToHuman(hmem,zmalloc_used);
	bytesToHuman(peak_hmem,zmalloc_used);
	bytesToHuman(total_system_hmem,total_system_mem);
	bytesToHuman(used_memory_rss_hmem,zmalloc_get_rss());
	bytesToHuman(maxmemory_hmem, 8);

	info = sdscatprintf(info,
	"# Memory\r\n"
	"used_memory:%zu\r\n"
	"used_memory_human:%s\r\n"
	"used_memory_rss:%zu\r\n"
	"used_memory_rss_human:%s\r\n"
	"used_memory_peak:%zu\r\n"
	"used_memory_peak_human:%s\r\n"
	"total_system_memory:%lu\r\n"
	"total_system_memory_human:%s\r\n"
	"maxmemory_human:%s\r\n"
	"mem_fragmentation_ratio:%.2f\r\n"
	"mem_allocator:%s\r\n",
	zmalloc_used,
	hmem,
	zmalloc_get_rss(),
	used_memory_rss_hmem,
	zmalloc_used,
	peak_hmem,
	(unsigned long)total_system_mem,
	total_system_hmem,
	maxmemory_hmem,
	zmalloc_get_fragmentation_ratio(zmalloc_get_rss()),
	ZMALLOC_LIB
	);

	addReplyBulkSds(session->sendBuf, info);

	for(auto it = obj.begin(); it != obj.end(); it++)
	{
		zfree(*it);
	}

	return true;
}


bool xRedis::clientCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown client  error");
		return REDIS_ERR;
	}

	addReply(session->sendBuf,shared.ok);
	return true;
}


bool xRedis::echoCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown echo  error");
		return REDIS_ERR;
	}

	addReplyBulk(session->sendBuf,obj[0]);
	return true;
}

bool xRedis::publishCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  != 2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown publish  error");
		return REDIS_ERR;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	MutexLock &mu = pubSubShards[hash% kShards].mutex;
	auto &pubSub = pubSubShards[hash% kShards].pubSub;
	{
		MutexLockGuard lock(mu);
		auto iter = pubSub.find(obj[0]);
		if(iter != pubSub.end())
		{
			for(auto list = iter->second.begin(); list != iter->second.end(); list ++)
			{
				session->pubSubTcpconn.push_back(*list);
			}

			addReply(session->sendPubSub,shared.mbulkhdr[3]);
			addReply(session->sendPubSub,shared.messagebulk);
			addReplyBulk(session->sendPubSub,obj[0]);
			addReplyBulk(session->sendPubSub,obj[1]);
			addReplyLongLong(session->sendPubSub,iter->second.size());
			addReply(session->sendBuf,shared.ok);
		}
		else
		{
			addReply(session->sendBuf,shared.czero);
		}
	}

	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}

bool xRedis::subscribeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  < 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown subscribe  error");
		return REDIS_ERR;
	}

	int count = 0;

	for(auto it = obj.begin(); it != obj.end(); it++)
	{
		(*it)->calHash();
	    size_t hash = (*it)->hash;

	    MutexLock &mu = pubSubShards[hash% kShards].mutex;
		auto &pubSub = pubSubShards[hash% kShards].pubSub;
		{
			MutexLockGuard lock(mu);
			auto iter = pubSub.find(*it);
			if(iter != pubSub.end())
			{
				iter->second.push_back(session->conn);
			}
			else
			{
				std::list<xTcpconnectionPtr> list;
				list.push_back(session->conn);
				pubSub.insert(std::make_pair(*it,std::move(list)));
			}

		}

		addReply(session->sendBuf,shared.mbulkhdr[3]);
		addReply(session->sendBuf,shared.subscribebulk);
		addReplyBulk(session->sendBuf,*it);
		addReplyLongLong(session->sendBuf,++count);
	}


	return true;
}

bool xRedis::authCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown auth  error");
	}

	if(password.c_str() == nullptr)
	{
		addReplyError(session->sendBuf,"Client sent AUTH, but no password is set");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,password.c_str()))
	{
		session->authEnabled = true;
		addReply(session->sendBuf,shared.ok);
	}
	else
	{
		addReplyError(session->sendBuf,"invalid password");
	}

	return true;
}

bool xRedis::configCommond(const std::deque <rObj*> & obj,xSession * session)
{

	if(obj.size() > 3)
	{
		addReplyErrorFormat(session->sendBuf,"unknown config  error");
	}

	if (!strcasecmp(obj[0]->ptr,"set"))
	{
		if(obj.size() != 3)
		{
			addReplyErrorFormat(session->sendBuf,"Wrong number of arguments for CONFIG %s",
			        (char*)obj[0]->ptr);
			return false;
		}

		if (!strcasecmp(obj[1]->ptr,"requirepass"))
		{
			password = obj[2]->ptr;
			authEnabled = true;
			session->authEnabled = false;
			addReply(session->sendBuf,shared.ok);
		}
		else
		{
			addReplyErrorFormat(session->sendBuf,"Invalid argument  for CONFIG SET '%s'",
						            (char*)obj[1]->ptr);
		}

	}
	else
	{
		addReplyError(session->sendBuf, "CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
	}


	return true;
}

bool xRedis::clusterCommond(const std::deque <rObj*> & obj,xSession * session)
{

	addReply(session->sendBuf,shared.ok);
	return true;
	if(!clusterEnabled)
	{
		addReplyError(session->sendBuf,"This instance has cluster support disabled");
		return false;
	}

	if(obj.size() != 3)
	{
		addReplyErrorFormat(session->sendBuf,"unknown cluster  error");
		return false;
	}

	if(!strcasecmp(obj[0]->ptr,"meet"))
	{
		long long port;
		
		if (getLongLongFromObject(obj[2], &port) != REDIS_OK)
		{
			 addReplyErrorFormat(session->sendBuf,"Invalid TCP port specified: %s",
								 (char*)obj[2]->ptr);
			 return false;
		}

	}
	
	return  true;
}



bool xRedis::saveCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown save error");
		return false;
	}

	int64_t start =  mstime();
	{
		MutexLockGuard lk(mutex);
		char filename[] = "dump.rdb";
		if(rdbSave(filename,this) == REDIS_OK)
		{
			addReply(session->sendBuf,shared.ok);
			LOG_INFO<<"Save rdb success";
		}
		else
		{
			addReply(session->sendBuf,shared.err);
			LOG_INFO<<"Save rdb failure";
		}
	}


	printf("use mem %ld\n",zmalloc_used_memory());
	float dt = (float)(mstime() - start)/1000.0;
	printf("saveCommond :%.2f\n", dt);

	return true;
}


bool xRedis::slaveofCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() !=  2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown slaveof error");
		return false;
	}

	MutexLockGuard lk(mutex);
	if (!strcasecmp(obj[0]->ptr,"no") &&!strcasecmp(obj[1]->ptr,"one")) 
	{
		if (masterHost.c_str() && masterPort) 
		{
			LOG_WARN<<"MASTER MODE enabled (user request from "<<masterHost.c_str()<<":"<<masterPort;
			repli.client->disconnect();
			repli.isreconnect = false;
		}

	}
	else
	{
		long   port;
		if ((getLongFromObjectOrReply(session->sendBuf, obj[1], &port, nullptr) != REDIS_OK))
		return false;

		if (host.c_str() && !memcmp(host.c_str(), obj[0]->ptr,sdsllen(obj[0]->ptr))
		&& this->port == port)
		{
			LOG_WARN<<"SLAVE OF connect self error .";
			addReplySds(session->sendBuf,sdsnew("Don't connect master self \r\n"));
			return false;
		}

		if (masterHost.c_str() && !memcmp(masterHost.c_str(), obj[0]->ptr,sdsllen(obj[0]->ptr))
		&& masterPort == port)
		{
			LOG_WARN<<"SLAVE OF would result into synchronization with the master we are already connected with. No operation performed.";
			addReplySds(session->sendBuf,sdsnew("+OK Already connected to specified master\r\n"));
			return false;
		}

		if(masterPort)
		{
			repli.client->disconnect();
			repli.isreconnect = false;
		}

		repli.replicationSetMaster(this,obj[0],port);
		LOG_INFO<<"SLAVE OF "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
		zfree(obj[0]);
		zfree(obj[1]);
	}

	addReply(session->sendBuf,shared.ok);
	return true;
}



bool xRedis::commandCommond(const std::deque <rObj*> & obj,xSession * session)
{
	addReply(session->sendBuf,shared.ok);
	return true;
}


bool xRedis::syncCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() >  0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown sync  error");
		return false;
	}

	xTimer * timer = nullptr;
	rObj * buf  = nullptr;
	{
		MutexLockGuard lk(slaveMutex);
		auto it = repliTimers.find(session->conn->getSockfd());
		if(it != repliTimers.end())
		{
			LOG_WARN<<"Client repeat send sync ";
			session->conn->forceClose();
			return false;
		}

		timer = session->conn->getLoop()->runAfter(REPLI_TIME_OUT,reinterpret_cast<void *>(session->conn->getSockfd()),
				true,std::bind(&xRedis::handleSalveRepliTimeOut,this,std::placeholders::_1));
		repliTimers.insert(std::make_pair(session->conn->getSockfd(),timer));
		tcpconnMaps.insert(std::make_pair(session->conn->getSockfd(),session->conn));
	}


	saveCommond(obj,session);
	char rdb_filename[] = "dump.rdb";
	size_t len = 0;

	{
		MutexLockGuard lk(mutex);
		buf = rdbLoad(rdb_filename,len);
	}

	if(buf != nullptr)
	{
		char str[4];
		int * sendLen = (int*)str;
		*sendLen = len;
		session->sendBuf.append((const char*)str,4);
		session->sendBuf.append(buf->ptr,len);
		zfree(buf);
		LOG_INFO<<"Sync load rdb success";
	}
	else if (errno != ENOENT)
	{
		{
			MutexLockGuard lk(slaveMutex);
			if(timer)
			{
				session->conn->getLoop()->cancelAfter(timer);
			}
			repliTimers.erase(session->conn->getSockfd());
			tcpconnMaps.erase(session->conn->getSockfd());
		}
	    LOG_WARN<<"Fatal error loading the DB:  Exiting."<<strerror(errno);
	}
	return true;
}


bool xRedis::psyncCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() >  0)
	{
		LOG_WARN<<"unknown psync  error";
		addReplyErrorFormat(session->sendBuf,"unknown psync  error");
		return false;
	}

	return true;
}


bool xRedis::dbsizeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown dbsize error");
		return false;
	}

	int64_t size = 0;
	{
	
		for(auto it = setMapShards.begin(); it != setMapShards.end(); it++)
		{	
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lk(mu);
			size+=(*it).setMap.size();
		}
	}

	{
		for(auto it = hsetMapShards.begin(); it != hsetMapShards.end(); it++)
		{
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lk(mu);
			size+=(*it).hsetMap.size();
		}
	}
	
	addReplyLongLong(session->sendBuf,size);
	
	return true;
}


bool xRedis::delCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  del error");
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;


	int count = 0;
	{
		MutexLock &mu = setMapShards[hash% kShards].mutex;
		auto &setMap = setMapShards[hash% kShards].setMap;
		{
			MutexLockGuard lock(mu);

			auto it = setMap.find(obj[0]);
			if(it != setMap.end())
			{
				count++;
				setMap.erase(it);
				zfree(it->first);
				zfree(it->second);
			}

		}
	}

	{
		MutexLock &mu = hsetMapShards[hash% kShards].mutex;
		auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
		{
			MutexLockGuard lock(mu);
			auto hmap = hsetMap.find(obj[0]);
			if(hmap != hsetMap.end())
			{
				count++;
				for(auto iter = hmap->second.begin(); iter != hmap->second.end(); iter++)
				{
					zfree(iter->first);
					zfree(iter->second);
				}
			}
			hsetMap.erase(obj[0]);
		}
	}

	{
		MutexLock &mu = pubSubShards[hash% kShards].mutex;
		auto &pubSub = pubSubShards[hash% kShards].pubSub;
		{

			MutexLockGuard lock(mu);
			auto it = pubSub.find(obj[0]);
			if(it != pubSub.end())
			{
				count++;
				pubSub.erase(it);
				zfree(it->first);
			}
		}
	}

	{
		MutexLock &mu = setShards[hash% kShards].mutex;
		auto &set = setShards[hash% kShards].set;
		{
			MutexLockGuard lock(mu);
			auto it = set.find(obj[0]);
			if(it != set.end())
			{
				count++;
				for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
				{
					zfree(*iter);
				}

				set.erase(it);
				zfree(it->first);
			}
		}
	}



	{
		MutexLock &mu = sortSetShards[hash% kShards].mutex;
		auto &set = sortSetShards[hash% kShards].set;
		auto &sset = sortSetShards[hash% kShards].sset;
		{
			count = 1;
			MutexLockGuard lock(mu);
			auto it = set.find(obj[0]);
			if(it != set.end())
			{
				for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
				{
					zfree(iter->first);
					zfree(iter->second);
				}


				for(auto iter = sset.begin(); iter != sset.end(); iter ++)
				{
					zfree((*iter).key);
					zfree((*iter).value);
				}

				set.erase(it);
				zfree(it->first);
				sset.clear();

			}
		}
	}




	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	addReplyLongLong(session->sendBuf,count);
	return true;
}


bool setnxCommond(const std::deque <rObj*> & obj,xSession * session)
{
	return true;
}


bool xRedis::scardCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1 )
	{
		addReplyErrorFormat(session->sendBuf,"unknown  scard error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;
	int count = 0;
	MutexLock &mu = setShards[hash% kShards].mutex;
	auto &set = setShards[hash% kShards].set;
	{
		MutexLockGuard lock(mu);
		auto it = set.find(obj[0]);
		if(it != set.end())
		{
			count = it->second.size();
		}
	}

	 addReplyLongLong(session->sendBuf,count);

	 zfree(obj[0]);
	return true;
}

bool xRedis::saddCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() < 2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  sadd error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;

	int count = 0;
	MutexLock &mu = setShards[hash% kShards].mutex;
	auto &set = setShards[hash% kShards].set;
	{

		MutexLockGuard lock(mu);
		auto iter = obj.begin();
		iter++;
		auto it = set.find(obj[0]);
		if(it == set.end())
		{
			std::unordered_set<rObj*,Hash,Equal> uSet;
			for( ;iter != obj.end(); iter ++)
			{
				(*iter)->calHash();
				count++;
				uSet.insert(*iter);
			}

			set.insert(std::make_pair(obj[0],std::move(uSet)));
		}
		else
		{
			for( ;iter != obj.end(); iter ++)
			{
				(*iter)->calHash();
				auto item = it->second.find(*iter);
				if(item == it->second.end())
				{
					count++;
					it->second.insert(*iter);
				}
				else
				{
					zfree(*iter);
				}
			}

			zfree(obj[0]);
		}
	}


	addReplyLongLong(session->sendBuf,count);

	return true;
}

bool xRedis::selectCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  select error");
		return false;
	}

	addReply(session->sendBuf,shared.ok);
	return true;
}

bool xRedis::hkeysCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hkeys error");
		return false;
	}

	obj[0]->calHash();
    size_t hash= obj[0]->hash;


    MutexLock &mu = hsetMapShards[hash% kShards].mutex;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		MutexLockGuard lock(mu);

		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			addReply(session->sendBuf,shared.emptymultibulk);
			return true;
		}

		addReplyMultiBulkLen(session->sendBuf,it->second.size());

		for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
		{
			addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
		}
	}

	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}


	return true;
}

bool xRedis::pingCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown ping error");
		return false;
	}

	addReply(session->sendBuf,shared.pong);
	return true;
}


bool xRedis::hgetallCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hgetall error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;
	
	MutexLock &mu = hsetMapShards[hash% kShards].mutex;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		MutexLockGuard lock(mu);
		
		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			addReply(session->sendBuf,shared.emptymultibulk);
			return true;
		}

		addReplyMultiBulkLen(session->sendBuf,it->second.size() * 2);

		for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
		{
			addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
			addReplyBulkCBuffer(session->sendBuf,iter->second->ptr,sdsllen(iter->second->ptr));
		}
	}

	zfree(obj[0]);

	return true;
}

bool xRedis::hlenCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hlen error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;
	bool update = false;

	int count = 0;
	MutexLock &mu = hsetMapShards[hash% kShards].mutex;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		MutexLockGuard lock(mu);
		auto it = hsetMap.find(obj[0]);
		if(it != hsetMap.end())
		{
			count = it->second.size();
		}
	}

	zfree(obj[0]);

	addReplyLongLong(session->sendBuf,count);
	return  true;
}

bool xRedis::hsetCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 3)
	{
		if(!slaveEnabled)
		addReplyErrorFormat(session->sendBuf,"unknown  hset error");
		return false;
	}

	obj[0]->calHash();
	obj[1]->calHash();
	size_t hash= obj[0]->hash;
	bool update = false;
	
	MutexLock &mu = hsetMapShards[hash% kShards].mutex;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		MutexLockGuard lock(mu);
		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			std::unordered_map<rObj*,rObj*,Hash,Equal> hset;
			hset.insert(std::make_pair(obj[1],obj[2]));
			hsetMap.insert(std::make_pair(obj[0],std::move(hset)));
		}
		else
		{
			zfree(obj[0]);
			auto iter = it->second.find(obj[1]);
			if(iter ==  it->second.end())
			{
				it->second.insert(std::make_pair(obj[1],obj[2]));
			}
			else
			{
				zfree(iter->first);
				zfree(iter->second);
				it->second.erase(iter);
				it->second.insert(std::make_pair(obj[1],obj[2]));
				update = true;
			}
		}
	}
	
	if(!slaveEnabled)
	addReply(session->sendBuf,update ? shared.czero : shared.cone);

	return true;
}

bool xRedis::hgetCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hget error");
		return false;
	}
	
	obj[0]->calHash();
	obj[1]->calHash();
	size_t hash= obj[0]->hash;
	MutexLock &mu = hsetMapShards[hash% kShards].mutex;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		MutexLockGuard lock(mu);
		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			zfree(obj[0]);
			zfree(obj[1]);
			addReply(session->sendBuf,shared.nullbulk);
			return true;
		}

		auto iter = it->second.find(obj[1]);
		if(iter == it->second.end())
		{
			zfree(obj[0]);
			zfree(obj[1]);
			addReply(session->sendBuf,shared.nullbulk);
			return true;
		}
		addReplyBulk(session->sendBuf,iter->second);
	}

	zfree(obj[0]);
	zfree(obj[1]);
			
	return true;
}



bool xRedis::flushdbCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  flushdb error");
		return false;
	}
	
	{
		for(auto it = setMapShards.begin(); it != setMapShards.end(); it++)
		{
			auto &map = (*it).setMap;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto sit = map.begin(); sit !=map.end(); sit++)
			{
				zfree(sit->first);
				zfree(sit->second);
			}
			map.clear();
		}
		
	}

	{
		for(auto it = hsetMapShards.begin(); it != hsetMapShards.end(); it++)
		{
			auto &map = (*it).hsetMap;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto sit = map.begin(); sit!=map.end(); sit++)
			{
				zfree(sit->first);
				auto  &mmap = sit->second;
				for(auto ssit = mmap.begin(); ssit!=mmap.end(); ssit++)
				{
					zfree(ssit->first);
					zfree(ssit->second);
				}
				mmap.clear();
			}
			map.clear();
			
		}
	}
	
	
	addReply(session->sendBuf,shared.ok);	
	return true;
}

bool xRedis::quitCommond(const std::deque <rObj*> & obj,xSession * session)
{
	session->conn->forceClose();
	return true;
}


bool xRedis::setCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 2)
	{
		if(!slaveEnabled)
		addReplyErrorFormat(session->sendBuf,"unknown  param error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;
	MutexLock &mu = setMapShards[hash% kShards].mutex;
	SetMap & setMap = setMapShards[hash % kShards].setMap;
	{
		MutexLockGuard lock(mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			setMap.insert(std::make_pair(obj[0],obj[1]));
		}
		else
		{
			zfree(it->second);
			zfree(it->first);
			setMap.erase(it);
			setMap.insert(std::make_pair(obj[0],obj[1]));
		}
	}
	

	if(!slaveEnabled)
	addReply(session->sendBuf,shared.ok);
	return true;
}

bool xRedis::getCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  param error");
		return false;
	}
	
	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	MutexLock &mu = setMapShards[hash  % kShards].mutex;
	SetMap & setMap = setMapShards[hash  % kShards].setMap;
	{
		MutexLockGuard lock(mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			zfree(obj[0]);
			addReply(session->sendBuf,shared.nullbulk);
			return true;
		}
		
		addReplyBulk(session->sendBuf,it->second);
	}

	zfree(obj[0]);
	
	return true;
}


void xRedis::flush()
{

}



