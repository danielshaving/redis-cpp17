#include "xRedis.h"

xRedis::xRedis(const char * ip, int16_t port, int16_t threadCount,bool enbaledCluster)
:server(&loop, ip, port,nullptr),
ip(ip),
port(port),
threadCount(1),
masterPort(0),
clusterEnabled(enbaledCluster),
slaveEnabled(false),
authEnabled(false),
repliEnabled(false),
salveCount(0),
clusterSlotEnabled(false),
clusterRepliMigratEnabled(false),
clusterRepliImportEnabeld(false),
object(this),
repli(this),
senti(this),
clus(this),
rdb(this),
forkEnabled(false),
forkCondWaitCount(0),
rdbChildPid(-1),
slavefd(-1)
{
	initConfig();
	loadDataFromDisk();
	server.setConnectionCallback(std::bind(&xRedis::connCallBack, this, std::placeholders::_1));
	server.setThreadNum(threadCount);
	if(threadCount > 1)
	{
	    this->threadCount = threadCount;
	}
	server.start();
	loop.runAfter(1.0,nullptr,true,std::bind(&xRedis::serverCron,this,std::placeholders::_1));

	sentiThreads =  std::unique_ptr<std::thread>(new std::thread(std::bind(&xSentinel::connectSentinel,&senti)));
	sentiThreads->detach();

	repliThreads = std::unique_ptr<std::thread>(new std::thread(std::bind(&xReplication::connectMaster,&repli)));
	repliThreads->detach();

	clusterThreads = std::unique_ptr<std::thread>(new std::thread(std::bind(&xCluster::connectCluster, &clus)));
	clusterThreads->detach();
}


xRedis::~xRedis()
{
	clear();
	clearCommand();

}

bool xRedis::clearClusterMigradeCommand(void * data)
{
	return true;
}

void xRedis::replyCheck()
{

}

void xRedis::serverCron(const std::any &context)
{
    if(rdbChildPid != -1)
    {
        pid_t pid;
        int32_t statloc;

		if ((pid = wait3(&statloc,WNOHANG,nullptr)) != 0)
		{
			 int32_t exitcode = WEXITSTATUS(statloc);
			 int32_t bysignal = 0;

			 if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

			 if (pid == rdbChildPid)
			 {
				if (!bysignal && exitcode == 0)
				{
					LOG_INFO<<"background saving terminated with success";
					if(slavefd != -1)
					{

						std::unique_lock <std::mutex> lck(slaveMutex);
						auto it = salvetcpconnMaps.find(slavefd);
						if(it == salvetcpconnMaps.end())
						{
							LOG_WARN<<"master sync send failure";
						}
						else
						{
							if(!rdb.rdbReplication("dump.rdb",it->second))
							{
								it->second->forceClose();
								LOG_WARN<<"master sync send failure";
							}
							else
							{
								LOG_INFO<<"master sync send success ";
							}
						}

						slavefd = -1;
					}
				}
				else if (!bysignal && exitcode != 0)
				{
					LOG_INFO<<"background saving error";
				}
				else
				{
					LOG_WARN<<"background saving terminated by signal "<< bysignal;
					char tmpfile[256];
					snprintf(tmpfile,256,"temp-%d.rdb", (int32_t) rdbChildPid);
					unlink(tmpfile);

					if (bysignal != SIGUSR1)
					{

					}

				 }
			 }
			 else
			 {
				 LOG_WARN<<"Warning, detected child with unmatched pid: "<<pid;
			 }

			 rdbChildPid = -1;

		}


	}
}

void xRedis::handleTimeOut(const std::any & context)
{
	loop.quit();
}

void xRedis::handleSetExpire(const std::any & context)
{
	rObj * obj = std::any_cast<rObj*>(context);
	int32_t c;
	removeCommand(obj,c);
}


void xRedis::handleSalveRepliTimeOut(const std::any & context)
{
	std::unique_lock <std::mutex> lck(slaveMutex);
	auto it = salvetcpconnMaps.find(std::any_cast<int32_t>(context));
	if(it != salvetcpconnMaps.end())
	{
		it->second->forceClose();
	}
	LOG_INFO<<"sync connect repli  timeout ";
}




void xRedis::clearDeques(std::deque<rObj*> & robj)
{
	for (auto it = robj.begin(); it != robj.end(); ++it)
	{
	    (*it)->calHash();
		auto iter = replyCommandMap.find(*it);
		if (iter == replyCommandMap.end())
		{
			zfree(*it);
		}

	}

}



void xRedis::clearClusterState(int32_t sockfd)
{
	
}


void xRedis::clearRepliState(int32_t sockfd)
{
	{
		std::unique_lock <std::mutex> lck(slaveMutex);
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
void xRedis::connCallBack(const xTcpconnectionPtr& conn)
{
	if(conn->connected())
	{
		//socket.setTcpNoDelay(conn->getSockfd(),true);
		socket.getpeerName(conn->getSockfd(),&(conn->ip),conn->port);
		std::shared_ptr<xSession> session (new xSession(this,conn));
		std::unique_lock <std::mutex> lck(mtx);;
		sessions[conn->getSockfd()] = session;
		LOG_INFO<<"Client connect success";
	}
	else
	{
		{
			clearRepliState(conn->getSockfd());
		}

		{
			clearClusterState(conn->getSockfd());
		}
	
		{
			std::unique_lock <std::mutex> lck(mtx);
			sessions.erase(conn->getSockfd());
		}

		LOG_INFO<<"Client disconnect";
	}
}


bool xRedis::deCodePacket(const xTcpconnectionPtr& conn,xBuffer *recvBuf)
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

	xTimestamp start (xTimestamp::now());
	if(rdb.rdbLoad(rdb_filename) == REDIS_OK)
	{
		xTimestamp end(xTimestamp::now());
		LOG_INFO<<"db loaded from disk sec: "<<timeDifference(end, start);
	}
	else if (errno != ENOENT)
	{
       	LOG_WARN<<"fatal error loading the DB:  Exiting."<<strerror(errno);
 	}

}


bool xRedis::scardCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() != 1 )
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  scard error");
		return false;
	}

	size_t hash= obj[0]->hash;
	int32_t count = 0;
	std::mutex &mu = setShards[hash% kShards].mtx;
	auto &set = setShards[hash% kShards].set;
	{
		std::unique_lock <std::mutex> lck(mtx);
		auto it = set.find(obj[0]);
		if(it != set.end())
		{
			count = it->second.size();
		}
	}

	object.addReplyLongLong(session->sendBuf,count);

	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}

bool xRedis::saddCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() < 2)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  sadd error");
		return false;
	}


	size_t hash= obj[0]->hash;

	int32_t count = 0;
	std::mutex &mu = setShards[hash% kShards].mtx;
	auto &set = setShards[hash% kShards].set;
	{
		std::unique_lock <std::mutex> lck(mtx);
		auto it = set.find(obj[0]);
		if(it == set.end())
		{
			std::unordered_set<rObj*,Hash,Equal> uset;
			for(int32_t i = 1; i < obj.size(); i ++)
			{
			
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
			for(int32_t i = 1; i < obj.size(); i ++)
			{
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

	object.addReplyLongLong(session->sendBuf,count);

	return true;
}



bool xRedis::sentinelCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	return false;
}

bool xRedis::memoryCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size()  > 2)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown memory  error");
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
				object.addReply(session->sendBuf, object.ok);
				return false;
			}
	}

#endif

	object.addReply(session->sendBuf,object.ok);
	return false;
}

bool xRedis::infoCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size()  < 0)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown info  error");
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
	ip.c_str(),
	port,
	threadCount);

	{
		std::unique_lock <std::mutex> lck(slaveMutex);
		for(auto it = salvetcpconnMaps.begin(); it != salvetcpconnMaps.end(); ++it )
		{
			info = sdscat(info,"\r\n");
			info = sdscatprintf(info,
			"# SlaveInfo \r\n"
			"slave_ip:%s\r\n"
			"slave_port:%d\r\n",
			it->second->ip.c_str(),
			it->second->port);

		}

	}
	
	object.addReplyBulkSds(session->sendBuf, info);
	
	return false ;
}


bool xRedis::clientCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() > 1)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown client  error");
		return false;
	}

	object.addReply(session->sendBuf,object.ok);

	return false;
}


bool xRedis::echoCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() > 1)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown echo  error");
		return false;
	}

	object.addReplyBulk(session->sendBuf,obj[0]);
	return false;
}




bool xRedis::authCommand(const std::deque <rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size() > 1)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown auth  error");
		return false;
	}

	if (password.c_str() == nullptr)
	{
		object.addReplyError(session->sendBuf, "client sent auth, but no password is set");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr, password.c_str()))
	{
		session->authEnabled = true;
		object.addReply(session->sendBuf, object.ok);
	}
	else
	{
		object.addReplyError(session->sendBuf, "invalid password");
	}


	return false;
}

bool xRedis::configCommand(const std::deque <rObj*> & obj, const xSeesionPtr &session)
{

	if (obj.size() > 3 ||  obj.size() == 0)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown config  error");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr, "set"))
	{
		if (obj.size() != 3)
		{
			object.addReplyErrorFormat(session->sendBuf, "Wrong number of arguments for CONFIG %s",(char*)obj[0]->ptr);
			return false;
		}

		if (!strcasecmp(obj[1]->ptr, "requirepass"))
		{
			password = obj[2]->ptr;
			authEnabled = true;
			session->authEnabled = false;
			object.addReply(session->sendBuf, object.ok);
		}
		else
		{
			object.addReplyErrorFormat(session->sendBuf, "Invalid argument  for CONFIG SET '%s'",
				(char*)obj[1]->ptr);
		}

	}
	else
	{
		object.addReplyError(session->sendBuf, "config subcommand must be one of GET, SET, RESETSTAT, REWRITE");
	}

	return false;

}

bool xRedis::migrateCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size() < 5)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown migrate  error");
		return false;
	}

	if (!clusterEnabled)
	{
		object.addReplyError(session->sendBuf, "this instance has cluster support disabled");
		return false;
	}

	long long port;

	if (object.getLongLongFromObject(obj[1], &port) != REDIS_OK)
	{
		object.addReplyErrorFormat(session->sendBuf, "Invalid TCP port specified: %s",
			(char*)obj[2]->ptr);
		return false;
	}

	std::string ip = obj[0]->ptr;
	if (ip == ip  &&  this->port == port)
	{
		object.addReplyErrorFormat(session->sendBuf, "migrate  self server error ");
		return false;
	}

	clus.replicationToNode(session,ip,port);
	object.addReply(session->sendBuf, object.ok);

	return false;
}

bool xRedis::clusterCommand(const std::deque <rObj*> & obj, const xSeesionPtr &session)
{
	if (!clusterEnabled)
	{
		object.addReplyError(session->sendBuf, "this instance has cluster support disabled");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr, "meet"))
	{
		if (obj.size() != 3)
		{
			object.addReplyErrorFormat(session->sendBuf, "unknown cluster  error");
			return false;
		}

		long long port;

		if (object.getLongLongFromObject(obj[2], &port) != REDIS_OK)
		{
			object.addReplyErrorFormat(session->sendBuf, "Invalid TCP port specified: %s", (char*)obj[2]->ptr);
			return false;
		}

		if (ip.c_str() && !memcmp(ip.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr)) && this->port == port)
		{
			LOG_WARN << "cluster  meet  connect self error .";
			object.addReplyErrorFormat(session->sendBuf, "Don't connect self ");
			return false;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); ++it)
			{
				if (port == it->second->port && !memcmp(it->second->ip.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr)))
				{
					LOG_WARN << "cluster  meet  already exists .";
					object.addReplyErrorFormat(session->sendBuf, "cluster  meet  already exists ");
					return false;
				}
			}
		}
		
		if(!clus.connSetCluster(obj[1]->ptr, port))
		{
			object.addReplyErrorFormat(session->sendBuf,"Invaild node address specified: %s:%s",(char*)obj[1]->ptr,(char*)obj[2]->ptr);
			return false;
		}
		
	}
	else if (!strcasecmp(obj[0]->ptr, "connect") && obj.size() == 3)
	{
		long long  port;
	
		if (object.getLongLongFromObject(obj[2], &port) != REDIS_OK)
		{
			object.addReplyError(session->sendBuf, "Invalid or out of range port");
			return  false;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); ++it)
			{
				if (port == it->second->port && !memcmp(it->second->ip.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr)))
				{
					return false;
				}
			}
		}
	
		clus.connSetCluster(obj[1]->ptr, port);
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr, "nodes") && obj.size() == 1)
	{
		rObj *o = object.createObject(OBJ_STRING, clus.showClusterNodes());
		object.addReplyBulk(session->sendBuf, o);
		object.decrRefCount(o);
		return false;
	}
	else if(!strcasecmp(obj[0]->ptr,"getkeysinslot") && obj.size() == 3)
	{
		long long maxkeys, slot;
		uint32_t numkeys, j;
		rObj **keys;

		if (object.getLongLongFromObjectOrReply(session->sendBuf,obj[1],&slot,nullptr) != REDIS_OK)
			return false;
			

		if (object.getLongLongFromObjectOrReply(session->sendBuf,obj[2],&maxkeys,nullptr)!= REDIS_OK)
			return false;
	
		if (slot < 0 || slot >= 16384 || maxkeys < 0) 
		{
			object.addReplyError(session->sendBuf,"Invalid slot or number of keys");
			return false;
		}
		
		keys = (rObj**)zmalloc(sizeof(rObj*) * maxkeys);
		
		clus.getKeyInSlot(slot,keys,maxkeys);
		object.addReplyMultiBulkLen(session->sendBuf,numkeys);
		for (j = 0; j < numkeys; j++)
			object.addReplyBulk(session->sendBuf,keys[j]);
		zfree(keys);
		return false;
		
	}
	else if (!strcasecmp(obj[0]->ptr, "slots") && obj.size() == 1)
	{

	}
	else if (!strcasecmp(obj[0]->ptr, "keyslot") && obj.size() == 2)
	{
		char * key = obj[1]->ptr;
		object.addReplyLongLong(session->sendBuf, clus.keyHashSlot((char*)key, sdslen(key)));
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr, "setslot") && obj.size() >= 3)
	{		
		if( !strcasecmp(obj[1]->ptr, "node") )
		{
			std::string imipPort = obj[2]->ptr;
			{
				std::unique_lock <std::mutex> lck(clusterMutex);
				clus.importingSlotsFrom.erase(imipPort);
				if(clus.importingSlotsFrom.size() == 0)
				{
					clusterRepliImportEnabeld = false;
				}
			}

			std::string fromIp;
			int32_t  fromPort;
			const char *start = obj[3]->ptr;
			const char *end = obj[3]->ptr + sdslen(obj[3]->ptr);
			const  char *space = std::find(start,end,':');
			if(space != end)
			{
				std::string ip(start,space);
				fromIp = ip;
				std::string port(space + 2,end);
				long long value;
				string2ll(port.c_str(), port.length(), &value);
				fromPort = value;
			}
			else
			{
				 object.addReply(session->sendBuf, object.err);
				 return false;
			}
			
				
			for(int32_t i  = 4; i < obj.size(); i++)
			{
				int32_t slot;
				if ((slot = clus.getSlotOrReply(session, obj[i])) == -1)
				{
					object.addReplyErrorFormat(session->sendBuf, "Invalid slot %d",(char*)obj[i]->ptr);
					return false;
				}
				
				std::unique_lock <std::mutex> lck(clusterMutex);
				auto it = clus.clusterSlotNodes.find(slot);
				if(it != clus.clusterSlotNodes.end())
				{
					it->second.ip = fromIp;
					it->second.port = fromPort;
				}
				else
				{
				    LOG_WARN<<"slot not found error";
				}
			}
		
			object.addReply(session->sendBuf, object.ok);
			LOG_INFO<<"cluster async replication success "<<imipPort;
			return false;

		}

		int32_t slot;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == -1)
		{
			object.addReplyErrorFormat(session->sendBuf, "Invalid slot %d" ,(char*)obj[1]->ptr);
			return false;
		}

		std::string nodeName = obj[3]->ptr;
		if (nodeName == ip + "::" + std::to_string(port))
		{
			object.addReplyErrorFormat(session->sendBuf, "setslot self server error ");
			return false;
		}
		
		if( !strcasecmp(obj[2]->ptr, "importing") && obj.size() == 4)
		{
			bool mark = false;
			{
				std::unique_lock <std::mutex> lck(clusterMutex);
				for (auto it = clus.clusterSlotNodes.begin(); it != clus.clusterSlotNodes.end(); ++it)
				{
					std::string node = it->second.ip + "::" + std::to_string(it->second.port);
					if (node == nodeName && slot == it->first)
					{
						mark = true;
						break;
					}
				}
			}

			if (!mark)
			{
				object.addReplyErrorFormat(session->sendBuf, "setslot slot node no found error ");
				return false;
			}
		
			std::unique_lock <std::mutex> lck(clusterMutex);
			auto it = clus.importingSlotsFrom.find(nodeName);
			if (it == clus.importingSlotsFrom.end())
			{
				std::unordered_set<int32_t> uset;
				uset.insert(slot);
				clus.importingSlotsFrom.insert(std::make_pair(std::move(nodeName), std::move(uset)));
			}
			else
			{
				auto iter = it->second.find(slot);
				if (iter == it->second.end())
				{
					it->second.insert(slot);
				}
				else
				{
					object.addReplyErrorFormat(session->sendBuf, "repeat importing slot :%d", slot);
					return false;
				}
			}

			clusterRepliImportEnabeld = true;
		}
		else if (!strcasecmp(obj[2]->ptr, "migrating") && obj.size() == 4)
		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			auto it = clus.migratingSlosTos.find(nodeName);
			if (it == clus.migratingSlosTos.end())
			{
				std::unordered_set<int32_t> uset;
				uset.insert(slot);
				clus.migratingSlosTos.insert(std::make_pair(std::move(nodeName), std::move(uset)));
			}
			else
			{
				auto iter = it->second.find(slot);
				if (iter == it->second.end())
				{
					it->second.insert(slot);
				}
				else
				{
					object.addReplyErrorFormat(session->sendBuf, "repeat migrating slot :%d", slot);
					return false;
				}
			}
		}
		else
		{
			object.addReplyErrorFormat(session->sendBuf, "Invalid  param ");
			return false;
		}

		
	}
	else if(!strcasecmp(obj[0]->ptr, "delimport"))
	{
		std::unique_lock <std::mutex> lck(clusterMutex);
		clus.importingSlotsFrom.clear();
		clusterRepliImportEnabeld = false;
		LOG_INFO << "delimport success:";
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr, "delsync"))
	{
		int32_t slot;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == 0)
		{
			LOG_INFO << "getSlotOrReply error ";
			return false;
		}

        {
            std::unique_lock <std::mutex> lck(clusterMutex);
            clus.clusterSlotNodes.erase(slot);

            if (clus.clusterSlotNodes.size() == 0)
            {
                clusterSlotEnabled = false;
            }
		}

		LOG_INFO << "delsync success:" << slot;
		return false;

	}
	else if (!strcasecmp(obj[0]->ptr, "addsync"))
	{
		int32_t slot;
		long long  port;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == 0)
		{
			LOG_INFO << "getSlotOrReply error ";
			return false;
		}

		if (object.getLongLongFromObject(obj[3], &port) != REDIS_OK)
		{
			object.addReplyError(session->sendBuf, "Invalid or out of range port");
			return  REDIS_ERR;
		}

		std::unique_lock <std::mutex> lck(clusterMutex);
		auto it = clus.clusterSlotNodes.find(slot);
		if (it == clus.clusterSlotNodes.end())
		{
			clusterSlotEnabled = true;
			LOG_INFO << "addsync success:" << slot;
			clus.cretateClusterNode(slot,obj[2]->ptr,port);
		}
		else
		{
			LOG_INFO << "clusterSlotNodes insert error ";
		}

		return false;
		
	}
	else if (!strcasecmp(obj[0]->ptr, "delslots") &&  obj.size() == 2)
	{		
		std::unique_lock <std::mutex> lck(clusterMutex);
		if (clustertcpconnMaps.size() == 0)
		{
			object.addReplyErrorFormat(session->sendBuf, "execute cluster meet ip:port");
			return false;
		}

		int32_t slot;
		int32_t j;
		for (j = 1; j < obj.size(); j++)
		{
			if ((slot = clus.getSlotOrReply(session, obj[j])) == 0)
			{
				return false;
			}

			if(slot < 0 || slot > 16384 )
			{
				object.addReplyErrorFormat(session->sendBuf, "cluster delslots range error %d:",slot);
				return false;
			}

			auto it = clus.clusterSlotNodes.find(slot);
			if (it != clus.clusterSlotNodes.end())
			{
				clus.deques.push_back(object.cluster);
				clus.deques.push_back(object.delsync);
				clus.deques.push_back(object.createStringObject(obj[j]->ptr, sdslen(obj[j]->ptr)));
				clus.syncClusterSlot();
				clus.clusterSlotNodes.erase(slot);
				LOG_INFO << "deslots success " << slot;
			}
		}

		if (clus.clusterSlotNodes.size() == 0)
		{
			clusterSlotEnabled = false;
		}

	}
	else if (!strcasecmp(obj[0]->ptr, "addslots") && obj.size() == 2)
	{
		int32_t  j, slot;
		for (j = 1; j < obj.size(); j++)
		{
			if(slot < 0 || slot > 16384 )
			{
				object.addReplyErrorFormat(session->sendBuf, "cluster delslots range error %d:",slot);
				return false;
			}

			if ((slot = clus.getSlotOrReply(session, obj[j])) == 0)
			{
				return false;
			}

			std::unique_lock <std::mutex> lck(clusterMutex);
			if (clustertcpconnMaps.size() == 0)
			{
				object.addReplyErrorFormat(session->sendBuf, "execute cluster meet ip:port");
				return false;
			}

			auto it = clus.clusterSlotNodes.find(slot);
			if (it == clus.clusterSlotNodes.end())
			{
				clus.deques.push_back(object.cluster);
				clus.deques.push_back(object.addsync);
				clus.deques.push_back(object.createStringObject(obj[j]->ptr, sdslen(obj[j]->ptr)));
				clus.deques.push_back(object.rIp);
				clus.deques.push_back(object.rPort);
				clus.cretateClusterNode(slot,this->ip,this->port);
				clus.syncClusterSlot();
				LOG_INFO << "addslots success " << slot;
			}
			else
			{
				object.addReplyErrorFormat(session->sendBuf, "Slot %d specified multiple times", slot);
				return false;
			}
		}
		clusterSlotEnabled = true;
	}
	else
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown param error");
		return false;
	}
	
	object.addReply(session->sendBuf, object.ok);
	return false;
	
}

void xRedis::structureRedisProtocol(xBuffer &  sendBuf, std::deque<rObj*> &robjs)
{
	int32_t len, j;
	char buf[8];
	buf[0] = '*';
	len = 1 + ll2string(buf + 1, sizeof(buf) - 1, robjs.size());
	buf[len++] = '\r';
	buf[len++] = '\n';
	sendBuf.append(buf, len);

	for (int32_t i = 0; i < robjs.size(); i++)
	{
		buf[0] = '$';
		len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdslen(robjs[i]->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		sendBuf.append(buf, len);
		sendBuf.append(robjs[i]->ptr, sdslen(robjs[i]->ptr));
		sendBuf.append("\r\n", 2);
	}
}



bool xRedis::bgsave(const xSeesionPtr &session,bool enabled)
{
	if(rdbChildPid != -1)
	{
		if(!enabled)
		{
			object.addReplyError(session->sendBuf, "Background save already in progress");
			LOG_WARN << "rdbChildPid == -1";
		}
		return false;
	}


	if(rdbSaveBackground(session, enabled) == REDIS_OK)
	{
		if(!enabled)
		{
			object.addReplyStatus(session->sendBuf, "Background saving started");
		}
	}
	else
	{
		if(!enabled)
		{
			object.addReply(session->sendBuf, object.err);
		}
		return false;
	}

	return true;
}

bool  xRedis::save(const xSeesionPtr &session)
{
	if(rdbChildPid != -1)
	{
		return false;
	}

	xTimestamp start(xTimestamp::now());
	{
		char filename[] = "dump.rdb";
		if(rdb.rdbSave(filename) == REDIS_OK)
		{
			xTimestamp end(xTimestamp::now());
			LOG_INFO<<"DB saved on disk sec: "<<timeDifference(end, start);
		}
		else
		{
			LOG_INFO<<"DB saved on disk error";
			return false;
		}
	}

	return true;
}



void xRedis::forkClear()
{
     for(auto it = sessions.begin(); it != sessions.end(); ++it)
     {
         it->second->conn->forceClose();
     }

     sessions.clear();

     for (auto it = salvetcpconnMaps.begin(); it != salvetcpconnMaps.end(); ++it)
     {
         it->second->forceClose();
     }

     salvetcpconnMaps.clear();

     for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); ++it)
     {
         it->second->forceClose();
     }

     clustertcpconnMaps.clear();
}
int32_t xRedis::rdbSaveBackground(const xSeesionPtr &session, bool enabled)
{
	if(rdbChildPid != -1)
	{
		return REDIS_ERR;
	}

	pid_t childpid;
	if ((childpid = fork()) == 0)
	{
	     forkClear();
		 int32_t retval;
		 rdb.setBlockEnable(enabled);
		 retval = rdb.rdbSave("dump.rdb");
		 if(retval == REDIS_OK)
		 {
			 size_t privateDirty = zmalloc_get_private_dirty();
			 if (privateDirty)
			 {
				 LOG_INFO<<"RDB: "<< privateDirty/(1024*1024)<<"MB of memory used by copy-on-write";
			 }
		 }
		 else
		 {
			 LOG_WARN << "rdbSave failure";
		 }
		 exit((retval == REDIS_OK) ? 0 : 1);
	}
	else
	{
		if (childpid == -1)
		{
			LOG_WARN << "childpid error";
			if (!enabled)
			{
				object.addReply(session->sendBuf, object.err);
			}
			return REDIS_ERR;
		}

		rdbChildPid = childpid;
	}

	return REDIS_OK; /* unreached */
}

bool xRedis::bgsaveCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() > 0)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown bgsave error");
		return false;
	}
	
	bgsave(session);

	return true;
	
}

bool xRedis::saveCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() > 0)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown save error");
		return false;
	}

	if(save(session))
	{
		object.addReply(session->sendBuf,object.ok);
	}
	else
	{
		object.addReply(session->sendBuf,object.err);
	}

	return true;
}


bool xRedis::slaveofCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() !=  2)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown slaveof error");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,"no") &&!strcasecmp(obj[1]->ptr,"one")) 
	{
		if (masterHost.c_str() && masterPort) 
		{
			LOG_WARN<<"master mode enabled (user request from "<<masterHost.c_str()<<":"<<masterPort;
			repli.disconnect();
		}
	}
	else
	{
		long   port;
		if ((object.getLongFromObjectOrReply(session->sendBuf, obj[1], &port, nullptr) != REDIS_OK))
			return false;

		if (ip.c_str() && !memcmp(ip.c_str(), obj[0]->ptr,sdslen(obj[0]->ptr))
		&& this->port == port)
		{
			LOG_WARN<<"slave of  connect self error .";
			object.addReplySds(session->sendBuf,sdsnew("don't connect master self \r\n"));
			return false;
		}

		if (masterPort > 0)
		{
			LOG_WARN<<"slave of would result into synchronization with the master we are already connected with. no operation performed.";
			object.addReplySds(session->sendBuf,sdsnew("+ok already connected to specified master\r\n"));
			return false;
		}	

		repli.replicationSetMaster(obj[0],port);
		LOG_INFO<<"slave of "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
	}
	
	object.addReply(session->sendBuf,object.ok);
	return false;
}



bool xRedis::commandCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{	
	object.addReply(session->sendBuf,object.ok);
	return false;
}



 void xRedis::handleForkTimeOut()
 {
    forkCondWaitCount++;
    expireCondition.notify_one();

    {
        std::unique_lock <std::mutex> lck(forkMutex);
        forkCondition.wait(lck);
    }
 }



bool xRedis::syncCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() >  0)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown sync  error");
		return false;
	}

	xTimer * timer = nullptr;
	{
		std::unique_lock <std::mutex> lck(slaveMutex);
		auto it = repliTimers.find(session->conn->getSockfd());
		if(it != repliTimers.end())
		{
			LOG_WARN<<"client repeat send sync ";
			session->conn->forceClose();
			return false;
		}
		
		slavefd = session->conn->getSockfd();
		timer = session->conn->getLoop()->runAfter(REPLI_TIME_OUT,(void *)&slavefd,
				false,std::bind(&xRedis::handleSalveRepliTimeOut,this,std::placeholders::_1));
		repliTimers.insert(std::make_pair(session->conn->getSockfd(),timer));
		salvetcpconnMaps.insert(std::make_pair(session->conn->getSockfd(),session->conn));

	}

	
    auto threadPoolVec = server.getThreadPool()->getAllLoops();
    for(auto it = threadPoolVec.begin(); it != threadPoolVec.end(); ++it)
    {
        if(session->conn->getLoop()->getThreadId() == (*it)->getThreadId())
        {
            continue;
        }

		(*it)->runInLoop(std::bind(&xRedis::handleForkTimeOut, this));
    }
    
    if(threadCount > 1)
    {
        std::unique_lock <std::mutex> lck(forkMutex);
        while(forkCondWaitCount < threadCount)
        {
            expireCondition.wait(lck);
        }
    }
    

    repliEnabled = true;
    forkCondWaitCount = 0;
	session->conn->setMessageCallback(std::bind(&xReplication::slaveCallBack, &repli, std::placeholders::_1, std::placeholders::_2));

	if(!bgsave(session,true))
	{
		LOG_WARN << "bgsave failure";
		{
			std::unique_lock <std::mutex> lck(slaveMutex);
			repliTimers.erase(session->conn->getSockfd());
			session->conn->getLoop()->cancelAfter(timer);
			salvetcpconnMaps.erase(session->conn->getSockfd());
		}
		slavefd = -1;
		session->conn->forceClose();
	}

    if(threadCount > 1)
    {
        forkCondition.notify_all();
    }


	return true;
}


bool xRedis::psyncCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() >  0)
	{
		LOG_WARN<<"unknown psync  error";
		object.addReplyErrorFormat(session->sendBuf,"unknown psync  error");
		return false;
	}

	return true;
}


size_t xRedis::getDbsize()
{
	size_t size = 0;
	{
		for(auto it = setMapShards.begin(); it != setMapShards.end(); ++it)
		{
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			size+=(*it).setMap.size();
		}
	}

	{
		for(auto it = hsetMapShards.begin(); it != hsetMapShards.end(); ++it)
		{
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			size+=(*it).hsetMap.size();
		}
	}

	{
		for (auto it = listMapShards.begin(); it != listMapShards.end(); ++it)
		{
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			size += (*it).listMap.size();
		}
	}


	{
		for (auto it = setShards.begin(); it != setShards.end(); ++it)
		{
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			size += (*it).set.size();
		}
	}


	{
		for (auto it = sortShards.begin(); it != sortShards.end(); ++it)
		{
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			size += (*it).set.size();
		}
	}




	return size;
}

bool xRedis::dbsizeCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() > 0)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown dbsize error");
		return false;
	}

	object.addReplyLongLong(session->sendBuf,getDbsize());

	return true;
}



int32_t  xRedis::removeCommand(rObj * obj,int32_t &count)
{
	
	{
		std::mutex &mu = setMapShards[obj->hash% kShards].mtx;
		auto &setMap = setMapShards[obj->hash% kShards].setMap;
		{
			std::unique_lock <std::mutex> lck(mu);
			auto it = setMap.find(obj);
			if(it != setMap.end())
			{
				auto iter = expireTimers.find(obj);
				if(iter != expireTimers.end())
				{
					zfree(iter->first);
					loop.cancelAfter(iter->second);
					expireTimers.erase(iter);
				}

				count ++;
				zfree(it->first);
				zfree(it->second);
				setMap.erase(it);
			}

		}
	}

	{
		std::mutex &mu = hsetMapShards[obj->hash% kShards].mtx;
		auto &hsetMap = hsetMapShards[obj->hash% kShards].hsetMap;
		{
			std::unique_lock <std::mutex> lck(mu);
			auto it = hsetMap.find(obj);
			if(it != hsetMap.end())
			{
				count ++;
				for(auto iter = it->second.begin(); iter != it->second.end(); ++iter)
				{
					zfree(iter->first);
					zfree(iter->second);
				}

				zfree(it->first);
				hsetMap.erase(it);

			}
		}
	}


	{
		std::mutex &mu = listMapShards[obj->hash% kShards].mtx;
		auto &listMap = listMapShards[obj->hash% kShards].listMap;
		{
			std::unique_lock <std::mutex> lck(mu);
			auto it = listMap.find(obj);
			if(it != listMap.end())
			{
				count ++;
				for(auto iter = it->second.begin(); iter != it->second.end(); ++iter)
				{
					zfree(*iter);
				}

				zfree(it->first);
				listMap.erase(it);
			}

		}
	}


	{
		std::mutex &mu = setShards[obj->hash% kShards].mtx;
		auto &set = setShards[obj->hash% kShards].set;
		{
			std::unique_lock <std::mutex> lck(mu);
			auto it = set.find(obj);
			if(it != set.end())
			{
				count ++;
				for(auto iter = it->second.begin(); iter != it->second.end(); ++iter)
				{
					zfree(*iter);
				}

				zfree(it->first);
				set.erase(it);
			}
		}
	}



	return  count;
}


bool xRedis::delCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() < 1)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  del error");
		return false;
	}

	int32_t count = 0;
	for(int32_t i = 0 ; i < obj.size(); i ++)
	{	
		removeCommand(obj[i],count);
		zfree(obj[i]);
	}

	object.addReplyLongLong(session->sendBuf,count);
	
	return true;
}



bool xRedis::hkeysCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() != 1)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  hkeys error");
		return false;
	}


	size_t hash= obj[0]->hash;


    std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);

		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			object.addReply(session->sendBuf,object.emptymultibulk);
			return false;
		}

		object.addReplyMultiBulkLen(session->sendBuf,it->second.size());

		for(auto iter = it->second.begin(); iter != it->second.end(); ++iter)
		{
			object.addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
		}
	}

	return false;

}

bool xRedis::pingCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() > 0)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown ping error");
		return false;
	}
	
	object.addReply(session->sendBuf,object.pong);
	return false;
}


bool xRedis::hgetallCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() != 1)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  hgetall error");
		return false;
	}


	size_t hash= obj[0]->hash;
	
	std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		
		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			object.addReply(session->sendBuf,object.emptymultibulk);
			return false;
		}

		object.addReplyMultiBulkLen(session->sendBuf,it->second.size() * 2);

		for(auto iter = it->second.begin(); iter != it->second.end(); ++iter)
		{
			object.addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
			object.addReplyBulkCBuffer(session->sendBuf,iter->second->ptr,sdslen(iter->second->ptr));
		}
	}

	zfree(obj[0]);

	return true;
}

bool xRedis::hlenCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() != 1)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  hlen error");
		return false;
	}


	size_t hash= obj[0]->hash;
	bool update = false;

	int32_t count = 0;
	std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = hsetMap.find(obj[0]);
		if(it != hsetMap.end())
		{
			count = it->second.size();
		}
	}

	zfree(obj[0]);

	object.addReplyLongLong(session->sendBuf,count);
	return  true;
}

bool xRedis::hsetCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() != 3)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  hset error");
		return false;
	}

	size_t hash= obj[0]->hash;
	bool update = false;
	
	std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
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
	
	object.addReply(session->sendBuf,update ? object.czero : object.cone);

	return true;
}


bool xRedis::zcardCommand(const std::deque <rObj*> & obj, const xSeesionPtr &session)
{
	if(obj.size()  != 1 )
	{
		object.addReplyError(session->sendBuf,"unknown  zcard  param error");
		return false;
	}

	
	size_t hash= obj[0]->hash;
	size_t len = 0;
	std::mutex &mu = sortShards[hash% kShards].mtx;
	auto &sortSet = sortShards[hash% kShards].set;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = sortSet.find(obj[0]);
		if(it != sortSet.end())
		{
			assert(it->second.sortMap.size() ==  it->second.sortDouble.size());
			len += it->second.sortMap.size();
		}
	}

	object.addReplyLongLong(session->sendBuf,len);
	
	return false;
}



bool xRedis::zrangeCommand(const std::deque<rObj*> &obj,const xSeesionPtr &session)
{
	return zrangeGenericCommand(obj,session,0);
}


bool xRedis::zrevrangeCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	return zrangeGenericCommand(obj,session,1);
}

bool xRedis::zrangeGenericCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session,int32_t reverse)
{
	if (obj.size()  != 4)
	{
		object.addReplyError(session->sendBuf,"unknown  zrange  param error");
		return false;
	}

	int32_t rangelen;
	int32_t withscores = 0;
	long long start;
	
	if (object.getLongLongFromObjectOrReply(session->sendBuf,obj[1],&start,nullptr) != REDIS_OK)
	{
		return false;
	}

	long long end;

	if (object.getLongLongFromObjectOrReply(session->sendBuf,obj[2],&end,nullptr) != REDIS_OK)
	{
		return false;
	}

	if ( !strcasecmp(obj[3]->ptr,"withscores"))
	{
		withscores = 1;
	}
	else if (obj.size() >= 5)
	{
		object.addReply(session->sendBuf,object.syntaxerr);
		return false;
	}

	size_t hash= obj[0]->hash;
	std::mutex &mu = sortShards[hash% kShards].mtx;
	auto &sortSet = sortShards[hash% kShards].set;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = sortSet.find(obj[0]);
		if(it == sortSet.end())
		{
			object.addReply(session->sendBuf,object.emptymultibulk);
			return false;
		}
		else
		{	
			assert(it->second.sortMap.size() ==  it->second.sortDouble.size());
			int32_t llen = it->second.sortMap.size();
			
			if (start < 0) start = llen+start;
			if (end < 0) end = llen+end;
			if (start < 0) start = 0;

			if (start > end || start >= llen) 
			{
				object.addReply(session->sendBuf,object.emptymultibulk);
				return false;
			}

			if (end >= llen) 
			{
				end = llen-1;	
			}
				
 		   	rangelen = (end-start)+1;	
			object.addReplyMultiBulkLen(session->sendBuf, withscores ? (rangelen*2) : rangelen);

			if(reverse)
			{
				int32_t count = 0;
				for (auto iter = it->second.sortMap.rbegin(); iter != it->second.sortMap.rend(); ++iter)
				{
					if (count++ >= start)
					{
						object.addReplyBulkCBuffer(session->sendBuf, iter->second->ptr, sdslen(iter->second->ptr));
						if (withscores)
						{
							object.addReplyDouble(session->sendBuf, iter->first);
						}
					}
					if (count >= end)
					{
						break;
					}
				
				}
				
			}
			else
			{
				int32_t count = 0;
				for (auto iter = it->second.sortMap.begin(); iter != it->second.sortMap.end(); ++iter)
				{
					if (count++ >= start)
					{
						object.addReplyBulkCBuffer(session->sendBuf, iter->second->ptr, sdslen(iter->second->ptr));
						if (withscores)
						{
							object.addReplyDouble(session->sendBuf, iter->first);
						}
					}
					if (count >= end)
					{
						break;
					}
				
				}
			}
			
			
		}
	}

	
	return false;
}


bool xRedis::zaddCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if (obj.size() < 3)
	{
		object.addReplyError(session->sendBuf,"unknown  zadd  param error");
		return false;
	}

	double scores = 0;
	size_t  added = 0;
	size_t hash= obj[0]->hash;
	std::mutex &mu = sortShards[hash% kShards].mtx;
	auto &sortSet = sortShards[hash% kShards].set;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = sortSet.find(obj[0]);
		if(it == sortSet.end())
		{
			if (object.getDoubleFromObjectOrReply(session->sendBuf,obj[1],&scores,nullptr) != REDIS_OK)
			{
				return false;
			}

			sort_set set;
			set.sortDouble.insert(std::make_pair(obj[2],scores));
			set.sortMap.insert(std::make_pair(scores,obj[2]));
			added++;
			zfree(obj[1]);
			for(int32_t i = 3; i < obj.size(); i += 2)
			{
				if (object.getDoubleFromObjectOrReply(session->sendBuf,obj[i],&scores,nullptr) != REDIS_OK)
				{
					return false;
				}

				auto it = set.sortDouble.find(obj[i + 1]);
				if(it == set.sortDouble.end())
				{
					set.sortDouble.insert(std::make_pair(obj[i + 1],scores));
					set.sortMap.insert(std::make_pair(scores,obj[i + 1]));
					added++;
				}
				else
				{
					if(scores != it->second)
					{
						bool mark = false;
						auto iter = set.sortMap.find(it->second);
						while(iter != set.sortMap.end())
						{
							if(!memcmp(iter->second->ptr,obj[i + 1]->ptr,sdslen(obj[i + 1]->ptr)))
							{
								rObj * v = iter->second;
								set.sortMap.erase(iter);
								set.sortMap.insert(std::make_pair(scores,v));
								mark = true;
								break;
							}
							++it;
						}

						assert(mark);
						it->second = scores;
						zfree(obj[i]);
						zfree(obj[i + 1]);
						added++;
					}
					else
					{
						zfree(obj[i + 1]);
						zfree(obj[i]);
					}

				}
			}

			sortSet.insert(std::make_pair(obj[0],std::move(set)));
			object.addReplyLongLong(session->sendBuf,added);
			return true;
		}
		else
		{
			zfree(obj[0]);
			for(int32_t i = 1; i < obj.size(); i += 2)
			{
				if (object.getDoubleFromObjectOrReply(session->sendBuf,obj[i],&scores,nullptr) != REDIS_OK)
				{
					return false;
				}

				auto iter  = it->second.sortDouble.find(obj[i + 1]);
				if(iter == it->second.sortDouble.end())
				{
					it->second.sortDouble.insert(std::make_pair(obj[i + 1],scores));
					it->second.sortMap.insert(std::make_pair(scores,obj[i + 1]));
					zfree(obj[i]);
					added++;
				}
				else
				{
					if(scores != iter->second)
					{
						bool mark = false;
						auto iterr = it->second.sortMap.find(iter->second);
						while(iterr != it->second.sortMap.end())
						{
							if(!memcmp(iterr->second->ptr,obj[i + 1]->ptr,sdslen(obj[i + 1]->ptr)))
							{
								rObj * v = iterr->second;
								it->second.sortMap.erase(iterr);
								it->second.sortMap.insert(std::make_pair(scores,v));
								mark = true;
								break;
							}
							++iterr;
						}

						assert(mark);
						iter->second = scores;
						zfree(obj[i]);
						zfree(obj[i + 1]);
						added++;
					}
					else
					{
						zfree(obj[i + 1]);
						zfree(obj[i]);
					}

				}
			}

		}

		object.addReplyLongLong(session->sendBuf,added);
	}

	return true;
}



 bool xRedis::debugCommand(const std::deque <rObj*> & obj, const xSeesionPtr &session)
 {
	if (obj.size() == 1)
	{
		object.addReplyError(session->sendBuf,"You must specify a subcommand for DEBUG. Try DEBUG HELP for info.");
	    return false;
	}

	if(!strcasecmp(obj[0]->ptr,"sleep"))
	{
		double dtime = strtod(obj[1]->ptr,nullptr);
		long long utime = dtime*1000000;
		struct timespec tv;

		tv.tv_sec = utime / 1000000;
		tv.tv_nsec = (utime % 1000000) * 1000;
		nanosleep(&tv, nullptr);
		object.addReply(session->sendBuf,object.ok);
		return false;
	}
	
	object.addReply(session->sendBuf,object.err);
    return false;
 }

bool xRedis::hgetCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() != 2)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  hget  param error");
		return false;
	}
	
	size_t hash= obj[0]->hash;
	std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			object.addReply(session->sendBuf,object.nullbulk);
			return false;
		}

		auto iter = it->second.find(obj[1]);
		if(iter == it->second.end())
		{
			object.addReply(session->sendBuf,object.nullbulk);
			return false;
		}
		object.addReplyBulk(session->sendBuf,iter->second);
	}

	zfree(obj[0]);
	zfree(obj[1]);
	return true;
}

void xRedis::clear()
{
	object.destorySharedObjects();
}

void xRedis::clearCommand()
{
	{
		for(auto it = setMapShards.begin(); it != setMapShards.end(); ++it)
		{
			auto &map = (*it).setMap;
			std::mutex &mu =  (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter !=map.end(); ++iter)
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
		for(auto it = hsetMapShards.begin(); it != hsetMapShards.end(); ++it)
		{
			auto &map = (*it).hsetMap;
			std::mutex &mu =  (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter!=map.end(); ++iter)
			{
				auto  &mmap = iter->second;
				for(auto iterr = mmap.begin(); iterr!=mmap.end(); ++iterr)
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
		for (auto it = listMapShards.begin(); it != listMapShards.end(); ++it)
		{
			auto &list = (*it).listMap;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for (auto iter = list.begin(); iter != list.end(); ++iter)
			{
				for (auto iterr = iter->second.begin(); iterr != iter->second.end(); ++iterr)
				{
					zfree(*iterr);
				}
				zfree(iter->first);
			}

			list.clear();
		}

	}


	{
		for (auto it = setShards.begin(); it != setShards.end(); ++it)
		{
			auto &set = (*it).set;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for (auto iter = set.begin(); iter != set.end(); ++iter)
			{
				for (auto iterr = iter->second.begin(); iterr != iter->second.end(); ++iterr)
				{
					zfree(*iterr);
				}
				zfree(iter->first);
			}

			set.clear();
		}

	}


	{
		for (auto it = sortShards.begin(); it != sortShards.end(); ++it)
		{
			auto  &set = (*it).set;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = set.begin(); iter != set.end(); ++iter)
			{
				for(auto iterr = iter->second.sortDouble.begin(); iterr != iter->second.sortDouble.end(); ++iterr)
				{
					zfree(iterr->first);
				}
				zfree(iter->first);
			}
			set.clear();
		}

	}

}


bool xRedis::keysCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() != 1 )
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown keys  error");
		return false;
	}

	sds pattern =obj[0]->ptr;
	int32_t plen = sdslen(pattern), allkeys;
	unsigned long numkeys = 0;

	allkeys = (pattern[0] == '*' && pattern[1] == '\0');

	{
		for(auto it = setMapShards.begin(); it != setMapShards.end(); ++it)
		{
			auto &map = (*it).setMap;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter != map.end(); ++iter)
			{
				if (allkeys || stringmatchlen(pattern,plen,iter->first->ptr,sdslen(iter->first->ptr),0))
				{
					object.addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
					numkeys++;
				}
			}
		}
	}

	{
		for(auto it = hsetMapShards.begin(); it != hsetMapShards.end(); ++it)
		{
			auto &map = (*it).hsetMap;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter!=map.end(); ++iter)
			{
				if (allkeys || stringmatchlen(pattern,plen,iter->first->ptr,sdslen(iter->first->ptr),0))
				{
					object.addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
					numkeys++;
				}
			}
		}
	}

	{
		for(auto it = listMapShards.begin(); it != listMapShards.end(); ++it)
		{
			auto &map = (*it).listMap;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter != map.end();  ++iter)
			{
				if (allkeys || stringmatchlen(pattern,plen,iter->first->ptr,sdslen(iter->first->ptr),0))
				{
					object.addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
					numkeys++;
				}
			}
		}
	}

	{
		for(auto it = setShards.begin(); it != setShards.end(); ++it)
		{
			auto &map = (*it).set;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter != map.end();  ++iter)
			{
				if (allkeys || stringmatchlen(pattern,plen,iter->first->ptr,sdslen(iter->first->ptr),0))
				{
					object.addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
					numkeys++;
				}
			}
		}
	}

	{
		for(auto it = sortShards.begin(); it != sortShards.end(); ++it)
		{
			auto &map = (*it).set;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter != map.end();  ++iter)
			{
				if (allkeys || stringmatchlen(pattern,plen,iter->first->ptr,sdslen(iter->first->ptr),0))
				{
					object.addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
					numkeys++;
				}
			}
		}
	}

	object.prePendReplyLongLongWithPrefix(session->sendBuf,numkeys);

	return false;
}


bool xRedis::flushdbCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	if(obj.size() > 0)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  flushdb  param error");
		return false;
	}

	clearCommand();

	object.addReply(session->sendBuf,object.ok);
	return true;
}

bool xRedis::quitCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{
	session->conn->forceClose();
	return true;
}


bool xRedis::setCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{	

	if(obj.size() <  2 || obj.size() > 8 )
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  set param error");
		return false;
	}

	int32_t j;
	rObj * expire = nullptr;
	rObj * ex = nullptr;
	int32_t unit = UNIT_SECONDS;
	int32_t flags = OBJ_SET_NO_FLAGS;

	for (j = 2; j < obj.size(); j++)
	{
		const char *a = obj[j]->ptr;
		rObj *next = (j == obj.size() - 1) ? nullptr : obj[j + 1];

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
			object.addReply(session->sendBuf,object.syntaxerr);
			return false;
		}
	}
	

	long long milliseconds = 0;

	if (expire)
	{
		if (object.getLongLongFromObjectOrReply(session->sendBuf, expire, &milliseconds, NULL) != REDIS_OK)
		   return false;
		if (milliseconds <= 0)
		{
		    object.addReplyErrorFormat(session->sendBuf,"invalid expire time in");
		    return false;
		}
		if (unit == UNIT_SECONDS) milliseconds *= 1000;
	}

	size_t hash= obj[0]->hash;
	int32_t index = hash  % kShards;
	std::mutex &mu = setMapShards[index].mtx;
	auto & setMap = setMapShards[index].setMap;
	{
		std::unique_lock <std::mutex>  lck (mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			if(flags & OBJ_SET_XX)
			{
				object.addReply(session->sendBuf,object.nullbulk);
				return false;
			}

			setMap.insert(std::make_pair(obj[0],obj[1]));

			if (expire)
			{
				ex  = object.createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}

		}
		else
		{
			if(flags & OBJ_SET_NX)
			{
				object.addReply(session->sendBuf,object.nullbulk);
				return false;
			}

			if (expire)
			{
				ex  = object.createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}

			zfree(obj[0]);
			zfree(it->second);
			it->second = obj[1];

		}
	}

	if (expire)
	{
		{
			std::unique_lock <std::mutex> (expireMutex);
			ex->calHash();
			auto iter = expireTimers.find(ex);
			if(iter != expireTimers.end())
			{
				expireTimers.erase(iter);
				loop.cancelAfter(iter->second);
				zfree(iter->first);
			}

			xTimer * timer = loop.runAfter(milliseconds / 1000,(void *)(ex),false,std::bind(&xRedis::handleSetExpire,this,std::placeholders::_1));
			expireTimers.insert(std::make_pair(ex,timer));
		}

		for(int32_t i = 2; i < obj.size(); i++)
		{
			zfree(obj[i]);
		}
	}

	object.addReply(session->sendBuf,object.ok);
	return true;
}

bool xRedis::getCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session)
{	
	
	if(obj.size() != 1)
	{
		object.addReplyErrorFormat(session->sendBuf,"unknown  get param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	int32_t index = hash  % kShards;
	std::mutex &mu = setMapShards[index].mtx;
	SetMap & setMap = setMapShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			object.addReply(session->sendBuf,object.nullbulk);
			return false;
		}
		
		object.addReplyBulk(session->sendBuf,it->second);
	}

	zfree(obj[0]);
	
	return true;
}



bool xRedis::ttlCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size() != 1)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown  ttl param error");
		return false;
	}

	std::unique_lock <std::mutex> lck(expireMutex);
	auto it = expireTimers.find(obj[0]);
	if (it == expireTimers.end())
	{
		object.addReplyLongLong(session->sendBuf, -2);
		return false;
	}
	
	int64_t ttl = it->second->getExpiration().getMicroSecondsSinceEpoch() - xTimestamp::now().getMicroSecondsSinceEpoch();
	
	object.addReplyLongLong(session->sendBuf, ttl / 1000000);
	return false;
}


bool xRedis::lpushCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size()  <  2)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown  lpush  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	int32_t index = hash  % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	int64_t pushed = 0;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			std::deque<rObj*> list;
			for (int32_t i = 1; i < obj.size(); ++i)
			{
				pushed++;
				list.push_back(obj[i]);
			}
			listMap.insert(std::make_pair(obj[0], std::move(list)));
		}
		else
		{
			zfree(obj[0]);
			for (int32_t i = 1; i < obj.size(); ++i)
			{
				pushed++;
				it->second.push_back(obj[i]);
			}
		}
	}

	object.addReplyLongLong(session->sendBuf, pushed);
	return true;
}


bool xRedis::lpopCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size() != 1)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown  lpop  param error");
		return false;
	}


	size_t hash = obj[0]->hash;
	int32_t index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			object.addReply(session->sendBuf, object.nullbulk);
		}
		else
		{
			object.addReplyBulk(session->sendBuf, it->second.back());
			zfree(it->second.back());
			it->second.pop_back();
			if (it->second.empty())
			{
				zfree(it->first);
				listMap.erase(it);
			}
		}
	}

	return false;
}

bool xRedis::lrangeCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size() != 3)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown  lrange  param error");
		return false;
	}

	long  start;
	long end;

	if ((object.getLongFromObjectOrReply(session->sendBuf, obj[1], &start, nullptr) != REDIS_OK) ||
		(object.getLongFromObjectOrReply(session->sendBuf, obj[2], &end,nullptr) != REDIS_OK))
	{
		return false;
	}

	size_t hash = obj[0]->hash;
	int32_t index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			object.addReply(session->sendBuf, object.nullbulk);
			return false;
		}


		int64_t listSize = it->second.size();

		if (start < 0)
		{
			start = listSize + start;
		}

		if (end < 0)
		{
			end = listSize + end;
		}

		if (start < 0)
		{
			start = 0;
		}

		if (start > end || start >= listSize)
		{
			object.addReply(session->sendBuf, object.emptymultibulk);
			return false;
		}

		if (end >= listSize)
		{
			end = listSize - 1;
		}

		int64_t rangelen =  (end - start) + 1;
		object.addReplyMultiBulkLen(session->sendBuf, rangelen);

		while(rangelen--)
		{
			object.addReplyBulkCBuffer(session->sendBuf, it->second[start]->ptr, sdslen(it->second[start]->ptr));
			start++;
		}
	}
	return false;
}

bool xRedis::rpushCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size()  != 2)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown  rpush  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	int32_t index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	int64_t pushed = 0;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			std::deque<rObj*> list;
			for (int32_t i = 1; i < obj.size(); ++i)
			{
				pushed++;
				list.push_front(obj[i]);
			}

			listMap.insert(std::make_pair(obj[0], std::move(list)));
		}
		else
		{
			zfree(obj[0]);
			for (int32_t i = 1; i < obj.size(); ++i)
			{
				pushed++;
				it->second.push_front(obj[i]);
			}
		}
	}

	object.addReplyLongLong(session->sendBuf, pushed);

	return true;
}


bool xRedis::llenCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size() != 1)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown  llen  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	int32_t index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			object.addReplyLongLong(session->sendBuf,0);
			return false;
		}

		object.addReplyLongLong(session->sendBuf, it->second.size());
	}
	return false;
}


bool xRedis::rpopCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session)
{
	if (obj.size()  !=  1)
	{
		object.addReplyErrorFormat(session->sendBuf, "unknown  rpop  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	int32_t index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			object.addReply(session->sendBuf, object.nullbulk);
		}
		else
		{
			object.addReplyBulk(session->sendBuf, it->second.front());
			zfree(it->second.front());
			it->second.pop_front();
			if (it->second.empty())
			{
				zfree(it->first);
				listMap.erase(obj[0]);
			}
		}
	}

	return false;
}

void xRedis::flush()
{

}

void xRedis::initConfig()
{
	master = "master";
	slave = "slave";
	zmalloc_enable_thread_safeness();
	ipPort = this->ip + "::" + std::to_string(port);
	object.createSharedObjects();
}






