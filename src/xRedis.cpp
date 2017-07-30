#include "xRedis.h"




xRedis::xRedis(const char * ip, int16_t port, int16_t threadCount,bool enbaledCluster,bool enabledSentinel)
:host(ip),
port(port),
threadCount(threadCount),
masterPort(0),
clusterEnabled(enbaledCluster),
slaveEnabled(false),
authEnabled(false),
repliEnabled(false),
salveCount(0),
clusterSlotEnabled(false),
count(0),
pingPong(false)
{
	init();
	sentiThreads =  std::shared_ptr<std::thread>(new std::thread(std::bind(&xSentinel::connectSentinel,&senti)));
	sentiThreads->detach();
	repliThreads = std::shared_ptr<std::thread>(new std::thread(std::bind(&xReplication::connectMaster,&repli)));
	repliThreads->detach();
	clusterThreads = std::shared_ptr<std::thread>(new std::thread(std::bind(&xCluster::connectCluster, &clus)));
	clusterThreads->detach();

	createSharedObjects();
	loadDataFromDisk();
	server.init(&loop, host, port,this);
	server.setConnectionCallback(std::bind(&xRedis::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	server.setThreadNum(threadCount);
	server.start();
	zmalloc_enable_thread_safeness();
}

xRedis::~xRedis()
{
	destorySharedObjects();
	clearCommond();
}


void xRedis::test(void * data)
{
	count++;
	LOG_INFO << count;
}

void xRedis::handleTimeOut(void * data)
{
	loop.quit();
}

void xRedis::handleSetExpire(void * data)
{
	rObj * obj = (rObj*) data;
	int count = 0 ;
	removeCommond(obj,count);
}


void xRedis::handleSalveRepliTimeOut(void * data)
{
	int32_t *sockfd = (int32_t *)data;
	MutexLockGuard mu(slaveMutex);
	auto it = salvetcpconnMaps.find(*sockfd);
	if(it != salvetcpconnMaps.end())
	{
		it->second->forceClose();
	}
	LOG_INFO<<"sync connect repli  timeout ";
}


void xRedis::clearRepliState(int32_t sockfd)
{
	{
		MutexLockGuard mu(slaveMutex);
		auto it = salvetcpconnMaps.find(sockfd);
		if(it != salvetcpconnMaps.end())
		{
			salveCount--;
			salvetcpconnMaps.erase(sockfd);
			if(salvetcpconnMaps.size() == 0)
			{
				repliEnabled = false;
				xBuffer buffer;
				slaveCached.swap(buffer);
			}
		}

		auto iter = repliTimers.find(sockfd);
		if(iter != repliTimers.end())
		{
			loop.cancelAfter(iter->second);
			repliTimers.erase(iter);
		}

	}


}
void xRedis::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		socket.getpeerName(conn->getSockfd(),&(conn->host),conn->port);
		std::shared_ptr<xSession> session (new xSession(this,conn));
		MutexLockGuard mu(mutex);
		sessions[conn->getSockfd()] = session;
		LOG_INFO<<"Client connect success";
	}
	else
	{
		{
			clearRepliState(conn->getSockfd());
		}
	
		{
			MutexLockGuard mu(mutex);
			sessions.erase(conn->getSockfd());
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
		addReplyErrorFormat(session->sendBuf,"unknown zrevrange  error");
		return false;
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
		auto it = sset.find(obj[0]);
		if(it != sset.end())
		{
			addReplyMultiBulkLen(session->sendBuf,sset.size() * 2);
			int i  = 0 ;
			for(auto iter = it->second.rbegin(); iter != it->second.rend(); iter ++)
			{
				addReplyBulkCBuffer(session->sendBuf,(*iter).value->ptr,sdsllen((*iter).value->ptr));
				addReplyBulkCBuffer(session->sendBuf,(*iter).key->ptr,sdsllen((*iter).key->ptr));
			}
		}
		else
		{
			addReplyMultiBulkLen(session->sendBuf,0);
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
		return false;
	}

	int count = 0;
	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	MutexLock &mu = sortSetShards[hash% kShards].mutex;
	auto &set = sortSetShards[hash% kShards].set;
	auto &sset = sortSetShards[hash% kShards].sset;
	{
		MutexLockGuard lock(mu);
		auto it = set.find(obj[0]);
		if(it == set.end())
		{
			SetMap setMap;
			std::set<rSObj> sObj;
		
			for(int i = 1; i < obj.size(); i +=2)
			{
				obj[i]->calHash();
				obj[i + 1]->calHash();
				auto iter = setMap.find(obj[i + 1]);
				if(iter == setMap.end())
				{
					rSObj robj;
					robj.key = obj[i];
					robj.value = obj[i + 1];
					sObj.insert(robj);
					setMap.insert(std::make_pair(obj[i + 1],obj[i]));
					count++;
				}
				else
				{
					{
						rSObj  robj;
						robj.key = iter->second;
						robj.value = iter->first;
						sObj.erase(robj);
					}

					zfree(obj [i + 1]);
					zfree(iter->second);
					iter->second = obj[i];
					
					{
						rSObj  robj;
						robj.key = iter->second;
						robj.value = iter->first;
						sObj.insert(robj);
					}
					
				}
			}
			
			set.insert(std::make_pair(obj[0],setMap));
			sset.insert(std::make_pair(obj[0],sObj));
			
		}
		else
		{
			auto itt = sset.find(obj[0]);
			if(itt == sset.end())
			{
				assert(false);
			}
			
			zfree(obj[0]);
			for(int i = 1; i < obj.size(); i +=2)
			{
				obj[i]->calHash();
				obj[i + 1]->calHash();
				auto iter = it->second.find(obj[i + 1]);
				if(iter == it->second.end())
				{
					rSObj robj;
					robj.key = obj[i];
					robj.value = obj[i + 1];
					itt->second.insert(robj);
					it->second.insert(std::make_pair(obj[i + 1],obj[i]));
					count++;
				}
				else
				{
					{
						rSObj  robj;
						robj.key = iter->second;
						robj.value = iter->first;
						itt->second.erase(robj);
					}
				
					zfree(obj [i + 1]);	
					zfree(iter->second);
					iter->second = obj[i];
					{
					
						rSObj  robj;
						robj.key = iter->second;
						robj.value = iter->first;
						itt->second.insert(robj);
					}
				
				}
			}
				
		}
	}

	addReplyLongLong(session->sendBuf,count);
	return true;
}

bool xRedis::zcardCommond(const std::deque <rObj*> & obj,xSession * session)
{
	for(auto it  = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}

bool xRedis::zcountCommond(const std::deque <rObj*> & obj,xSession * session)
{
	for(auto it  = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}


bool xRedis::keysCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1 )
	{
		addReplyErrorFormat(session->sendBuf,"unknown keys  error");
		return false;
	}

	std::string des  = "*";
	std::string src = obj[0]->ptr;


	addReplyMultiBulkLen(session->sendBuf,getDbsize());

	{
		for(auto it = setMapShards.begin(); it != setMapShards.end(); it++)
		{
			auto &map = (*it).setMap;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter != map.end(); iter++)
			{
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
			}

		}

	}

	{
		for(auto it = hsetMapShards.begin(); it != hsetMapShards.end(); it++)
		{
			auto &map = (*it).hsetMap;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter!=map.end(); iter++)
			{
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
			}

		}

	}

	{
		for(auto it = setShards.begin(); it != setShards.end(); it++)
		{
			auto &map = (*it).set;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter != map.end(); iter ++)
			{
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
			}

		}
	}



	{
		for(auto it = sortSetShards.begin(); it != sortSetShards.end(); it++)
		{
			auto &map = (*it).set;
			auto &mmap = (*it).sset;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter != map.end(); iter ++)
			{
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
			}

		}
	}


	{
		for(auto it = pubSubShards.begin(); it != pubSubShards.end(); it++)
		{
			auto &map = (*it).pubSub;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter != map.end(); iter ++)
			{
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
			}

		}
	}




	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}

bool xRedis::zrangeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  < 3  || obj.size() > 4)
	{
		addReplyErrorFormat(session->sendBuf,"unknown zrange  error");
		return false;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	long long  begin = 0;
	string2ll(obj[1]->ptr,sdsllen(obj[1]->ptr),&begin);
	long long  end = 0;
	string2ll(obj[2]->ptr,sdsllen(obj[2]->ptr),&end);
	MutexLock &mu =  sortSetShards[hash% kShards].mutex;
	auto &sset = sortSetShards[hash% kShards].sset; 
	{
		MutexLockGuard lock(mu);
		auto it = sset.find(obj[0]);
		if(it != sset.end())
		{
			addReplyMultiBulkLen(session->sendBuf,it->second.size() * 2);
			for(auto iter = it->second.begin(); iter != it->second.end(); iter ++)
			{
				addReplyBulkCBuffer(session->sendBuf,(*iter).value->ptr,sdsllen((*iter).value->ptr));
				addReplyBulkCBuffer(session->sendBuf,(*iter).key->ptr,sdsllen((*iter).key->ptr));
			}
		}
		else
		{
			addReplyMultiBulkLen(session->sendBuf,0);
		}
	
	}

	return false;
}

bool xRedis::zrankCommond(const std::deque <rObj*> & obj,xSession * session)
{
	for(auto it  = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}


bool xRedis::unsubscribeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown unsubscribe  error");
		return false;
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


bool xRedis::sentinelCommond(const std::deque<rObj*> & obj, xSession * session)
{
	return true;
}

bool xRedis::memoryCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  > 2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown memory  error");
		return false;
	}
#ifdef USE_JEMALLOC

	char tmp[32];
	unsigned narenas = 0;
	size_t sz = sizeof(unsigned);
	if (!je_mallctl("arenas.narenas", &narenas, &sz, NULL, 0))
	{
		sprintf(tmp, "arena.%d.purge", narenas);
		if (!je_mallctl(tmp, NULL, 0, NULL, 0))
		{
			addReply(session->sendBuf, shared.ok);
			return false;
		}
	}

#endif

    addReply(session->sendBuf,shared.ok);


	return false;
}

bool xRedis::infoCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  < 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown info  error");
		return false;
	}

	struct rusage self_ru, c_ru;
	getrusage(RUSAGE_SELF, &self_ru);
	getrusage(RUSAGE_CHILDREN, &c_ru);
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

	info = sdscat(info,"\r\n");
	info = sdscatprintf(info,
	"# CPU\r\n"
	"used_cpu_sys:%.2f\r\n"
	"used_cpu_user:%.2f\r\n"
	"used_cpu_sys_children:%.2f\r\n"
	"used_cpu_user_children:%.2f\r\n",
	(float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
	(float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
	(float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000,
	(float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000);

	info = sdscat(info,"\r\n");
	info = sdscatprintf(info,
	"# Server\r\n"
	"tcp_connect_count:%d\r\n"
	"local_ip:%s\r\n"
	"local_port:%d\r\n"
	"local_thread_count:%d\n",
	sessions.size(),
	host.c_str(),
	port,
	threadCount+=4);

	{
		MutexLockGuard lock(slaveMutex);
		for(auto it = salvetcpconnMaps.begin(); it != salvetcpconnMaps.end(); it ++)
		{
			info = sdscat(info,"\r\n");
			info = sdscatprintf(info,
			"# SlaveInfo \r\n"
			"slave_ip:%s\r\n"
			"slave_port:%d\r\n",
			it->second->host.c_str(),
			it->second->port);

		}

	}
	
	addReplyBulkSds(session->sendBuf, info);
	
	return false ;
}


bool xRedis::clientCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown client  error");
		return false;
	}

	addReply(session->sendBuf,shared.ok);

	return false;
}


bool xRedis::echoCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown echo  error");
		return false;
	}

	addReplyBulk(session->sendBuf,obj[0]);

	
	return false;
}

bool xRedis::publishCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  != 2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown publish  error");
		return false;
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

	return false;
}

bool xRedis::subscribeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size()  < 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown subscribe  error");
		return false;
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
				zfree(*it);
				iter->second.push_back(session->conn);
			}
			else
			{
				std::list<xTcpconnectionPtr> list;
				list.push_back(session->conn);
				pubSub.insert(std::make_pair(*it, std::move(list)));
			}

		}

		addReply(session->sendBuf, shared.mbulkhdr[3]);
		addReply(session->sendBuf, shared.subscribebulk);
		addReplyBulk(session->sendBuf, *it);
		addReplyLongLong(session->sendBuf, ++count);

	}

	return true;
}

bool xRedis::authCommond(const std::deque <rObj*> & obj, xSession * session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf, "unknown auth  error");
		return false;
	}

	if (password.c_str() == nullptr)
	{
		addReplyError(session->sendBuf, "Client sent AUTH, but no password is set");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr, password.c_str()))
	{
		session->authEnabled = true;
		addReply(session->sendBuf, shared.ok);
	}
	else
	{
		addReplyError(session->sendBuf, "invalid password");
	}


	return false;
}

bool xRedis::configCommond(const std::deque <rObj*> & obj, xSession * session)
{

	if (obj.size() > 3)
	{
		addReplyErrorFormat(session->sendBuf, "unknown config  error");
		return false;
	}

if (!strcasecmp(obj[0]->ptr, "set"))
{
if (obj.size() != 3)
{
	addReplyErrorFormat(session->sendBuf, "Wrong number of arguments for CONFIG %s",
		(char*)obj[0]->ptr);
	return false;
}

if (!strcasecmp(obj[1]->ptr, "requirepass"))
{
	password = obj[2]->ptr;
	authEnabled = true;
	session->authEnabled = false;
	addReply(session->sendBuf, shared.ok);
}
else
{
	addReplyErrorFormat(session->sendBuf, "Invalid argument  for CONFIG SET '%s'",
		(char*)obj[1]->ptr);
}

}
else
{
	addReplyError(session->sendBuf, "CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
}

return false;

}

bool xRedis::clusterCommond(const std::deque <rObj*> & obj, xSession * session)
{
	if (obj.size() < 2)
	{
		addReplyErrorFormat(session->sendBuf, "unknown cluster  error");
		return false;
	}

	if (!clusterEnabled)
	{
		addReplyError(session->sendBuf, "This instance has cluster support disabled");
		return false;
	}

	long long port;

	if (getLongLongFromObject(obj[2], &port) != REDIS_OK)
	{
		addReplyErrorFormat(session->sendBuf, "Invalid TCP port specified: %s",
			(char*)obj[2]->ptr);
		return false;
	}

	if (host.c_str() && !memcmp(host.c_str(), obj[1]->ptr, sdsllen(obj[1]->ptr))
		&& this->port == port)
	{
		LOG_WARN << "cluster  meet  connect self error .";
		addReplyErrorFormat(session->sendBuf, "Don't connect self ");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr, "meet"))
	{
		{
			MutexLockGuard lk(clusterMutex);
			for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); it++)
			{
				if (port == it->second->port && !memcmp(it->second->host.c_str(), obj[1]->ptr, sdsllen(obj[1]->ptr)))
				{
					LOG_WARN << "cluster  meet  already exists .";
					addReplyErrorFormat(session->sendBuf, "cluster  meet  already exists ");
					return false;
				}
			}
		}
		clus.connSetCluster(obj[1]->ptr, (int32_t)port, this);
	}
	else if (!strcasecmp(obj[0]->ptr, "connect"))
	{
		{
			MutexLockGuard lk(clusterMutex);
			for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); it++)
			{
				if (port == it->second->port && !memcmp(it->second->host.c_str(), obj[1]->ptr, sdsllen(obj[1]->ptr)))
				{
					return false;
				}
			}
		}
		clus.connSetCluster(obj[1]->ptr, (int32_t)port, this);
	}
	else if (!strcasecmp(obj[0]->ptr, "slots") && obj.size() == 1)
	{

	}
	else if (!strcasecmp(obj[0]->ptr, "keyslot") && obj.size() == 2)
	{
		const char * key = obj[1]->ptr;
		addReplyLongLong(session->sendBuf, clus.keyHashSlot((char*)key, sdsllen(key)));
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr, "sync"))
	{
		int32_t slot;
		long long  p;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == 0)
		{
			LOG_INFO << "getSlotOrReply error ";
			return false;
		}

		if (getLongLongFromObject(obj[3], &p) != REDIS_OK)
		{
			addReplyError(session->sendBuf, "Invalid or out of range port");
			return  REDIS_ERR;
		}

		MutexLockGuard lk(clusterMutex);
		auto it = clus.clusterSlotNodes.find(slot);
		if (it == clus.clusterSlotNodes.end())
		{
			xClusterNode node;
			node.ip = obj[1]->ptr;
			node.port = (int16_t)p;
			clus.clusterSlotNodes.insert(std::make_pair(slot,std::move(node)));
		}
		else
		{
			LOG_INFO << "clusterSlotNodes insert error ";
		}
		
	}
	else if (!strcasecmp(obj[0]->ptr, "delslots"))
	{
		MutexLockGuard lk(clusterMutex);
	}
	else if (!strcasecmp(obj[0]->ptr, "addslots"))
	{
		int32_t  j, slot;
		int32_t del = !strcasecmp(obj[0]->ptr, "delslots");
		for (j = 1; j < obj.size(); j++)
		{
			if ((slot = clus.getSlotOrReply(session, obj[j])) == 0)
			{
				return false;
			}
			MutexLockGuard lk(clusterMutex);
			auto it = clus.clusterSlotNodes.find(slot);
			if (it == clus.clusterSlotNodes.end())
			{
				xClusterNode  node;
				node.ip = host;
				node.port = port;
				rObj * i = createStringObject(host.c_str(), host.length());
				char buf[32];
				int32_t len = ll2string(buf, sizeof(buf), port);
				rObj * p = createStringObject((const char*)buf, len);
				clus.syncClusterSlot(i, p, obj[j]);
				clus.clusterSlotNodes.insert(std::make_pair(slot, std::move(node)));
			}
			else
			{
				addReplyErrorFormat(session->sendBuf, "Slot %d specified multiple times", slot);
			}
		}
		clusterSlotEnabled = true;

	}
	else
	{
		addReplyErrorFormat(session->sendBuf, "unknown param error");
		return false;
	}
	
	addReply(session->sendBuf, shared.ok);
	return false;
	
}

void xRedis::structureRedisProtocol(xBuffer &  sendBuf, std::deque<rObj*> &robjs)
{
	int len, j;
	char buf[32];
	buf[0] = '*';
	len = 1 + ll2string(buf + 1, sizeof(buf) - 1, robjs.size());
	buf[len++] = '\r';
	buf[len++] = '\n';
	sendBuf.append(buf, len);

	for (int i = 0; i < robjs.size(); i++)
	{
		buf[0] = '$';
		len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdsllen(robjs[i]->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		sendBuf.append(buf, len);
		sendBuf.append(robjs[i]->ptr, sdsllen(robjs[i]->ptr));
		sendBuf.append("\r\n", 2);
	}
}


bool  xRedis::save(xSession * session)
{
	int64_t start =  mstime();
	{
		MutexLockGuard lk(mutex);
		char filename[] = "dump.rdb";
		if(rdbSave(filename,this) == REDIS_OK)
		{
			LOG_INFO<<"Save rdb success";
		}
		else
		{
			LOG_INFO<<"Save rdb failure";

			return false;
		}
	}

	return true;
}


bool xRedis::bgsaveCommond(const std::deque <rObj*> & obj,xSession * session)
{
	
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown bgsave error");
		return false;
	}
	
	if(save(session))
	{
		addReply(session->sendBuf,shared.ok);
			
	}
	else
	{
		addReply(session->sendBuf,shared.err);
	}

	return true;
	
}

bool xRedis::saveCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown save error");
		return false;
	}

	if(save(session))
	{
		addReply(session->sendBuf,shared.ok);
	}
	else
	{
		addReply(session->sendBuf,shared.err);
	}

	return true;
}


bool xRedis::slaveofCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() !=  2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown slaveof error");
		return false;
	}

	
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

		if (masterPort > 0)
		{
			LOG_WARN<<"SLAVE OF would result into synchronization with the master we are already connected with. No operation performed.";
			addReplySds(session->sendBuf,sdsnew("+OK Already connected to specified master\r\n"));
			return false;
		}	

		repli.replicationSetMaster(this,obj[0],port);
		LOG_INFO<<"SLAVE OF "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
	}
	


	addReply(session->sendBuf,shared.ok);
	return false;
}



bool xRedis::commandCommond(const std::deque <rObj*> & obj,xSession * session)
{
	for(auto it = obj.begin(); it != obj.end(); it++)
	{
		zfree(*it);
	}
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
	{
		MutexLockGuard lk(slaveMutex);
		auto it = repliTimers.find(session->conn->getSockfd());
		if(it != repliTimers.end())
		{
			LOG_WARN<<"Client repeat send sync ";
			session->conn->forceClose();
			return false;
		}

		int32_t sockfd = session->conn->getSockfd();
		timer = session->conn->getLoop()->runAfter(REPLI_TIME_OUT,(void *)&sockfd,
				false,std::bind(&xRedis::handleSalveRepliTimeOut,this,std::placeholders::_1));
		repliTimers.insert(std::make_pair(session->conn->getSockfd(),timer));
		salvetcpconnMaps.insert(std::make_pair(session->conn->getSockfd(),session->conn));
		repliEnabled = true;
	}

	if(!save(session))
	{
		repli.isreconnect =false;
		session->conn->forceClose();
		return false;
	}


	bool mark  = true;
	char rdb_filename[] = "dump.rdb";
	{
		{
			MutexLockGuard lk(mutex);
			mark = rdbReplication(rdb_filename,session);
		}

		if(!mark)
		{
			MutexLockGuard lk(slaveMutex);
			if(timer)
			{
				session->conn->getLoop()->cancelAfter(timer);
			}
			session->conn->forceClose();
			LOG_INFO<<"master sync send failure";
			return false;
		}
	}

	LOG_INFO<<"master sync send success ";
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


size_t xRedis::getDbsize()
{
	size_t size = 0;
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


	{
		for(auto it = setShards.begin(); it != setShards.end(); it++)
		{
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lk(mu);
			size+=(*it).set.size();
		}
	}



	{
		for(auto it = sortSetShards.begin(); it != sortSetShards.end(); it++)
		{
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lk(mu);
			size+=(*it).set.size();
		}
	}


	{
		for(auto it = pubSubShards.begin(); it != pubSubShards.end(); it++)
		{
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lk(mu);
			size+=(*it).pubSub.size();
		}
	}

	return size;
}

bool xRedis::dbsizeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown dbsize error");
		return false;
	}

	addReplyLongLong(session->sendBuf,getDbsize());

	return true;
}



int  xRedis::removeCommond(rObj * obj,int &count)
{
	
	{
		MutexLock &mu = setMapShards[obj->hash% kShards].mutex;
		auto &setMap = setMapShards[obj->hash% kShards].setMap;
		{
			MutexLockGuard lock(mu);
			auto it = setMap.find(obj);
			if(it != setMap.end())
			{
				auto iter = expireTimers.find(obj);
				if(iter != expireTimers.end())
				{
					expireTimers.erase(iter);
					loop.cancelAfter(iter->second);
				}

				count ++;
				setMap.erase(it);
				zfree(it->first);
				zfree(it->second);
			}

		}
	}

	{
		MutexLock &mu = hsetMapShards[obj->hash% kShards].mutex;
		auto &hsetMap = hsetMapShards[obj->hash% kShards].hsetMap;
		{
			MutexLockGuard lock(mu);
			auto hmap = hsetMap.find(obj);
			if(hmap != hsetMap.end())
			{
				count ++;
				for(auto iter = hmap->second.begin(); iter != hmap->second.end(); iter++)
				{
					zfree(iter->first);
					zfree(iter->second);
				}

				hsetMap.erase(hmap);
				zfree(hmap->first);
			}


		}
	}

	{
		MutexLock &mu = setShards[obj->hash% kShards].mutex;
		auto &set = setShards[obj->hash% kShards].set;
		{
			MutexLockGuard lock(mu);
			auto it = set.find(obj);
			if(it != set.end())
			{
				count ++;
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
		MutexLock &mu = sortSetShards[obj->hash% kShards].mutex;
		auto &set = sortSetShards[obj->hash% kShards].set;
		auto &sset = sortSetShards[obj->hash% kShards].sset;
		{
			MutexLockGuard lock(mu);
			auto it = set.find(obj);
			if(it != set.end())
			{
				auto itt = sset.find(obj);
				if(itt == sset.end())
				{
					assert(false);
				}
				
				count  ++;
				for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
				{
					zfree(iter->first);
					zfree(iter->second);
				}

				set.erase(it);
				zfree(it->first);
				sset.erase(itt);

			}
		}
	}


	{
		MutexLock &mu = pubSubShards[obj->hash% kShards].mutex;
		auto &pubSub = pubSubShards[obj->hash% kShards].pubSub;
		{
			MutexLockGuard lock(mu);
			auto it = pubSub.find(obj);
			if(it != pubSub.end())
			{
				count  ++;
				pubSub.erase(it);
				zfree(it->first);
			}
		}
	}

	return  count;
}


bool xRedis::delCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  del error");
	}

	int count = 0;
	for(int i = 0 ; i < obj.size(); i ++)
	{
		obj[i]->calHash();		
		removeCommond(obj[i],count);
		zfree(obj[i]);
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

	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}
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
		auto it = set.find(obj[0]);
		if(it == set.end())
		{
			std::unordered_set<rObj*,Hash,Equal> uset;
			for(int i = 1; i < obj.size(); i ++)
			{
				obj[i]->calHash();
				auto iter = uset.find(obj[i]);
				if(iter == uset.end())
				{
					count++;
					uset.insert(obj[i]);
				}
				else
				{
					zfree(obj[i]);
				}
				set.insert(std::make_pair(obj[0],std::move(uset)));
			}
		}
		else
		{
			zfree(obj[0]);
			for(int i = 1; i < obj.size(); i ++)
			{
				obj[i]->calHash();
				auto iter = it->second.find(obj[i]);
				if(iter == it->second.end())
				{
					count++;
					it->second.insert(obj[i]);
				}
				else
				{
					zfree(obj[i]);
				}
			}
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

	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
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
			return false;
		}

		addReplyMultiBulkLen(session->sendBuf,it->second.size());

		for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
		{
			addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdsllen(iter->first->ptr));
		}
	}

	return false;

}


bool xRedis::ppingCommond(const std::deque <rObj*> & obj, xSession * session)
{
	std::deque<rObj*>  robjs;
	robjs.push_back(shared.ppong);
	structureRedisProtocol(session->sendBuf,robjs);
	return true;
}

bool xRedis::ppongCommond(const std::deque <rObj*> & obj, xSession * session)
{
	pingPong = true;
	return true;
}


bool xRedis::pongCommond(const std::deque <rObj*> & obj,xSession * session)
{
	return false;
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
			return false;
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
				zfree(obj[1]);
				zfree(iter->second);
				iter->second = obj[2];
				update = true;
			}
		}
	}
	
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
			addReply(session->sendBuf,shared.nullbulk);
			return false;
		}

		auto iter = it->second.find(obj[1]);
		if(iter == it->second.end())
		{
			addReply(session->sendBuf,shared.nullbulk);
			return false;
		}
		addReplyBulk(session->sendBuf,iter->second);
	}

	zfree(obj[0]);
	zfree(obj[1]);
	return true;
}



void xRedis::clearCommond()
{
	{
		for(auto it = setMapShards.begin(); it != setMapShards.end(); it++)
		{
			auto &map = (*it).setMap;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter !=map.end(); iter++)
			{
				auto iterr = expireTimers.find(iter->first);
				if(iterr != expireTimers.end())
				{
					expireTimers.erase(iterr);
					loop.cancelAfter(iterr->second);
				}
			
				zfree(iter->first);
				zfree(iter->second);
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
			for(auto iter = map.begin(); iter!=map.end(); iter++)
			{
				auto  &mmap = iter->second;
				for(auto iterr = mmap.begin(); iterr!=mmap.end(); iterr++)
				{
					zfree(iterr->first);
					zfree(iterr->second);
				}
				zfree(iter->first);
			}
			map.clear();

		}

	}

	{
		for(auto it = setShards.begin(); it != setShards.end(); it++)
		{
			auto &map = (*it).set;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter != map.end(); iter ++)
			{
				for(auto iterr = iter->second.begin(); iterr != iter->second.end() ;iterr ++)
				{
					zfree(*iterr);
				}
				zfree(iter->first);
			}
			map.clear();

		}
	}
	


	{
		for(auto it = sortSetShards.begin(); it != sortSetShards.end(); it++)
		{
			auto &map = (*it).set;
			auto &mmap = (*it).sset;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter != map.end(); iter ++)
			{
				for(auto iterr = iter->second.begin(); iterr != iter->second.end() ;iterr ++)
				{
						zfree(iterr->first);
						zfree(iterr->second);
				}

				zfree(iter->first);
			}

			mmap.clear();
			map.clear();

		}
	}


	{
		for(auto it = pubSubShards.begin(); it != pubSubShards.end(); it++)
		{
			auto &map = (*it).pubSub;
			MutexLock &mu =  (*it).mutex;
			MutexLockGuard lock(mu);
			for(auto iter = map.begin(); iter != map.end(); iter ++)
			{
				zfree(iter->first);
			}
			map.clear();

		}
	}

}


bool xRedis::flushdbCommond(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  flushdb error");
		return false;
	}

	clearCommond();

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
	if(obj.size() <  2 || obj.size() > 8 )
	{
		addReplyErrorFormat(session->sendBuf,"unknown  param error");
		return false;
	}

    int j;
    rObj *expire = NULL;
    int unit = UNIT_SECONDS;	
    int flags = OBJ_SET_NO_FLAGS;

	for (j = 2; j < obj.size();j++)
	{
		const char *a = obj[j]->ptr;
		rObj *next = (j == obj.size() - 2) ? NULL : obj[j + 1];

		if ((a[0] == 'n' || a[0] == 'N') &&
		(a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
		!(flags & OBJ_SET_XX))
		{
			flags |= OBJ_SET_NX;
		}
		else if ((a[0] == 'x' || a[0] == 'X') &&
		       (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
		       !(flags & OBJ_SET_NX))
		{
			flags |= OBJ_SET_XX;
		}
		else if ((a[0] == 'e' || a[0] == 'E') &&
		       (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
		       !(flags & OBJ_SET_PX) && next)
		{
			flags |= OBJ_SET_EX;
			unit = UNIT_SECONDS;
			expire = next;
			j++;
		}
		else if ((a[0] == 'p' || a[0] == 'P') &&
		       (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
		       !(flags & OBJ_SET_EX) && next)
		{
			flags |= OBJ_SET_PX;
			unit = UNIT_MILLISECONDS;
			expire = next;
			j++;
		}
		else
		{
			addReply(session->sendBuf,shared.syntaxerr);
			return false;
		}
	}
	

	long long milliseconds = 0; /* initialized to avoid any harmness warning */

	if (expire)
	{
		if (getLongLongFromObjectOrReply(session->sendBuf, expire, &milliseconds, NULL) != REDIS_OK)
		   return false;
		if (milliseconds <= 0)
		{
		    addReplyErrorFormat(session->sendBuf,"invalid expire time in");
		    return false;
		}
		if (unit == UNIT_SECONDS) milliseconds *= 1000;
	}
    
	obj[0]->calHash();
	size_t hash= obj[0]->hash;
	int index = hash  % kShards;
	MutexLock &mu = setMapShards[index].mutex;
	auto & setMap = setMapShards[index].setMap;
	{
		MutexLockGuard lock(mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			if(flags & OBJ_SET_XX)
			{
				addReply(session->sendBuf,shared.nullbulk);
				return false;
			}
			setMap.insert(std::make_pair(obj[0],obj[1]));
		}
		else
		{
			if(flags & OBJ_SET_NX)
			{
				addReply(session->sendBuf,shared.nullbulk);
				return false;
			}

			zfree(obj[0]);
			zfree(it->second);
			it->second = obj[1];
		}
		
		if (expire)
		{
			LOG_INFO<<"expire";
			auto iter = expireTimers.find(obj[0]);
			if(iter != expireTimers.end())
			{
				expireTimers.erase(iter);
				loop.cancelAfter(iter->second);
			}

			xTimer * timer = loop.runAfter(milliseconds / 1000,(void *)(obj[0]),false,std::bind(&xRedis::handleSetExpire,this,std::placeholders::_1));
			expireTimers.insert(std::make_pair(obj[0],timer));
		}
	}

	for(int i = 2; i < obj.size(); i ++)
	{
		zfree(obj[i]);
	}
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
	int index = hash  % kShards;
	MutexLock &mu = setMapShards[index].mutex;
	SetMap & setMap = setMapShards[index].setMap;
	{
		MutexLockGuard lock(mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			addReply(session->sendBuf,shared.nullbulk);
			return false;
		}
		
		addReplyBulk(session->sendBuf,it->second);
	}

	zfree(obj[0]);
	
	return true;
}


void xRedis::flush()
{

}



void xRedis::init()
{
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
	obj = createStringObject("bgsave",6);
	handlerCommondMap[obj] = std::bind(&xRedis::bgsaveCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("memory",6);
	handlerCommondMap[obj] = std::bind(&xRedis::memoryCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("ppong",6);
	handlerCommondMap[obj] = std::bind(&xRedis::ppongCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("pping", 6);
	handlerCommondMap[obj] = std::bind(&xRedis::ppingCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("sentinel", 8);
	handlerCommondMap[obj] = std::bind(&xRedis::ppingCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("cluster", 7);
	handlerCommondMap[obj] = std::bind(&xRedis::clusterCommond, this, std::placeholders::_1, std::placeholders::_2);
	obj = createStringObject("set",3);
	unorderedmapCommonds.insert(obj);
	obj = createStringObject("hset",4);
	unorderedmapCommonds.insert(obj);
	obj = createStringObject("zadd",4);
	unorderedmapCommonds.insert(obj);
	obj = createStringObject("sadd",4);
	unorderedmapCommonds.insert(obj);
	obj = createStringObject("del",3);
	unorderedmapCommonds.insert(obj);
	obj = createStringObject("flushdb",7);
	unorderedmapCommonds.insert(obj);
	
}


