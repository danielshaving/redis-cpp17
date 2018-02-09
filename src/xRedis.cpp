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
	removeCommand(obj);
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
	for (auto &it : robj)
	{
	    it->calHash();
		auto iter = replyCommandMap.find(it);
		if (iter == replyCommandMap.end())
		{
			zfree(it);
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
		//LOG_INFO<<"Client connect success";
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

		//LOG_INFO<<"Client disconnect";
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
	if (!je_mallctl("arenas.narenas", &narenas, &sz, nullptr, 0))
	{
		sprintf(tmp, "arena.%d.purge", narenas);
		if (!je_mallctl(tmp, nullptr, 0, nullptr, 0))
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
		for(auto &it : salvetcpconnMaps )
		{
			info = sdscat(info,"\r\n");
			info = sdscatprintf(info,
			"# SlaveInfo \r\n"
			"slave_ip:%s\r\n"
			"slave_port:%d\r\n",
			it.second->ip.c_str(),
			it.second->port);
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
			for (auto &it : clustertcpconnMaps)
			{
				if (port == it.second->port && !memcmp(it.second->ip.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr)))
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
			for (auto &it : clustertcpconnMaps)
			{
				if (port == it.second->port && !memcmp(it.second->ip.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr)))
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

		if (object.getLongLongFromObjectOrReply(session->sendBuf,obj[1],&slot,nullptr) != REDIS_OK)
			return false;
			
		if (object.getLongLongFromObjectOrReply(session->sendBuf,obj[2],&maxkeys,nullptr) != REDIS_OK)
			return false;
	
		if (slot < 0 || slot >= 16384 || maxkeys < 0) 
		{
			object.addReplyError(session->sendBuf,"Invalid slot or number of keys");
			return false;
		}
		
		std::vector<rObj*> keys;
		clus.getKeyInSlot(slot,keys,maxkeys);
		object.addReplyMultiBulkLen(session->sendBuf,numkeys);
		
		for (auto &it :  keys)
		{
			object.addReplyBulk(session->sendBuf,it);
			zfree(it);
		}
		
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
		if(!!strcasecmp(obj[1]->ptr, "stable") )
		{
			
		}
		else if( !strcasecmp(obj[1]->ptr, "node") )
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
			const char *space = std::find(start,end,':');
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
					object.addReplyErrorFormat(session->sendBuf, "Invalid slot %s",obj[i]->ptr);
					return false;
				}
				
				std::unique_lock <std::mutex> lck(clusterMutex);
				auto it = clus.checkClusterSlot(slot);
				if(it != nullptr)
				{
					it->ip = fromIp;
					it->port = fromPort;
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
		bool mark = false;
		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto  &it : clus.clusterSlotNodes)
			{
				if(slot == it.first && nodeName == it.second.name)
				{
					if(ip == it.second.ip && port == it.second.port)
					{
						object.addReplyErrorFormat(session->sendBuf, "setslot migrate slot  error  %d",slot);
						return false;
					}
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
		
	
		if( !strcasecmp(obj[2]->ptr, "importing") && obj.size() == 4)
		{		
			std::unique_lock <std::mutex> lck(clusterMutex);
			auto &map = clus.getImporting();
			auto it = map.find(nodeName);
			if (it == map.end())
			{
				std::unordered_set<int32_t> uset;
				uset.insert(slot);
				map.insert(std::make_pair(std::move(nodeName), std::move(uset)));
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
			auto &map = clus.getMigrating();
			auto it = map.find(nodeName);
			if (it == map.end())
			{
				std::unordered_set<int32_t> uset;
				uset.insert(slot);
				map.insert(std::make_pair(std::move(nodeName), std::move(uset)));
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
			clus.eraseClusterNode(slot);
		}

		LOG_INFO << "delsync success:" << slot;
		return false;

	}
	else if (!strcasecmp(obj[0]->ptr, "addsync") && obj.size() == 5)
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
		if(clus.checkClusterSlot(slot) == nullptr)
		{
			LOG_INFO << "addsync success:" << slot;
			clus.cretateClusterNode(slot,obj[2]->ptr,port,obj[4]->ptr);
		}
		else
		{
			object.addReplyErrorFormat(session->sendBuf, "cluster insert error:%d",slot);
			LOG_INFO << "cluster insert error "<<slot;
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

			if(clus.checkClusterSlot(slot) == nullptr)
			{
				clus.delSlotDeques(obj[j],slot);
				clus.syncClusterSlot();
				LOG_INFO << "deslots success " << slot;
			}
			else
			{
				object.addReplyErrorFormat(session->sendBuf, "not found deslots error %d:",slot);
				LOG_INFO << "not found deslots " << slot;
			}
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
			if (clustertcpconnMaps.empty())
			{
				object.addReplyErrorFormat(session->sendBuf, "execute cluster meet ip:port");
				return false;
			}

			if(clus.checkClusterSlot(slot) == nullptr)
			{
				char name[CLUSTER_NAMELEN];
				getRandomHexChars(name,CLUSTER_NAMELEN);
				clus.cretateClusterNode(slot,this->ip,this->port,name);
				clus.addSlotDeques(obj[j],name);
				clus.syncClusterSlot();
				LOG_INFO << "addslots success " << slot;
			}
			else
			{
				object.addReplyErrorFormat(session->sendBuf, "Slot %d specified multiple times", slot);
				return false;
			}
		}
	
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


bool xRedis::getClusterMap(rObj * command)
{
	auto it = cluterMaps.find(command);
	if(it == cluterMaps.end())
	{
		return false;
	}
	return true;
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
     for(auto &it : sessions)
     {
         it.second->conn->forceClose();
     }

     sessions.clear();

     for (auto &it : salvetcpconnMaps)
     {
         it.second->forceClose();
     }

     salvetcpconnMaps.clear();

     for (auto &it : clustertcpconnMaps)
     {
         it.second->forceClose();
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
    for(auto &it : threadPoolVec)
    {
        if(session->conn->getLoop()->getThreadId() == it->getThreadId())
        {
            continue;
        }

		it->runInLoop(std::bind(&xRedis::handleForkTimeOut, this));
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
	
	for(auto &it : redisShards)
	{
		std::unique_lock <std::mutex> lck(it.mtx);
		auto &map = it.redis;
		size += map.size();
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

bool    xRedis::removeCommand(rObj * obj)
{
	size_t index = obj->hash% kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj);
		if(it != map.end())
		{
			if((*it)->type == OBJ_STRING)
			{
				auto iter = setMap.find(obj);
			#ifdef __DEBUG__
				assert(iter != setMap.end());
			#endif
				auto iterr = expireTimers.find(obj);
				if(iterr != expireTimers.end())
				{
					zfree(iterr->first);
					loop.cancelAfter(iterr->second);
					expireTimers.erase(iterr);
				}
				zfree(iter->first);
				zfree(iter->second);
				setMap.erase(iter);
			}
			return true;
		}
	}

	return false;
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
		if(removeCommand(obj[i]))
		{
			count++;
		}
	}

	object.addReplyLongLong(session->sendBuf,count);
	
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


void xRedis::clear()
{
	object.destorySharedObjects();
}

void xRedis::clearCommand()
{
	{
		std::unique_lock <std::mutex> lck(expireMutex);
		for(auto &it : expireTimers)
		{
#ifdef __DEBUG__
			assert(it.first->type == OBJ_EXPIRE);
#endif
			zfree(it.first);
			loop.cancelAfter(it.second);
			zfree(it.second);
		}
	}

	for(auto &it : redisShards)
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
				zfree(iterr->first);
				zfree(iterr->second);
				setMap.erase(iterr);
			}
			else if(iter->type == OBJ_SET)
			{
				
			}
			else if(iter->type == OBJ_LIST)
			{
				
			}
			else if(iter->type == OBJ_HASH)
			{
				
			}
			else if(iter->type == OBJ_ZSET)
			{
				
			}
			else
			{
				LOG_ERROR<<"type unkown:"<<iter->type;
			}
		}
		map.clear();
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
		for(auto &it : redisShards)
		{
			auto &mu =  it.mtx;
			auto &map = it.redis;
			auto &setMap = it.setMap;
			std::unique_lock <std::mutex> lck(mu);
			for(auto &iter : map)
			{
				if(iter->type == OBJ_STRING)
				{
					auto iterr = setMap.find(iter);
				#ifdef __DEBUG__
					assert(iterr != setMap.end());
				#endif

					if (allkeys || stringmatchlen(pattern,plen,iterr->first->ptr,sdslen(iterr->first->ptr),0))
					{
						object.addReplyBulkCBuffer(session->sendBuf,iterr->first->ptr,sdslen(iterr->first->ptr));
						numkeys++;
					}
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
		if (object.getLongLongFromObjectOrReply(session->sendBuf, expire, &milliseconds, nullptr) != REDIS_OK)
		{
			return false;
		}
		if (milliseconds <= 0)
		{
		    object.addReplyErrorFormat(session->sendBuf,"invalid expire time in");
		    return false;
		}
		if (unit == UNIT_SECONDS) milliseconds *= 1000;
	}
	
	obj[0]->type = OBJ_STRING;
	obj[1]->type = OBJ_STRING;
	size_t hash= obj[0]->hash;
	size_t index = hash  % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if(it == map.end())
		{
			if(flags & OBJ_SET_XX)
			{
				object.addReply(session->sendBuf,object.nullbulk);
				return false;
			}

		#ifdef __DEBUG__
			auto iter = setMap.find(obj[0]);
			assert(iter == setMap.end());
		#endif

			map.insert(obj[0]);
			setMap.insert(std::make_pair(obj[0],obj[1]));
			
			if (expire)
			{
				ex = object.createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
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
				ex = object.createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}

			auto iter = setMap.find(obj[0]);
		#ifdef __DEBUG__
			assert(iter != setMap.end());
		#endif
			zfree(obj[0]);
			zfree(iter->second);
			iter->second = obj[1];
		}
	}

	if (expire)
	{
		{
			std::unique_lock <std::mutex> lck(expireMutex);
			ex->type = OBJ_EXPIRE;
			ex->calHash();
			auto iter = expireTimers.find(ex);
			if(iter != expireTimers.end())
			{
				zfree(iter->first);
				loop.cancelAfter(iter->second);
				expireTimers.erase(iter);
			}

			xTimer * timer = loop.runAfter(milliseconds / 1000,ex,false,std::bind(&xRedis::handleSetExpire,this,std::placeholders::_1));
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
	auto &map = redisShards[index].redis;
	auto &mu = redisShards[index].mtx;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			object.addReply(session->sendBuf,object.nullbulk);
			return false;
		}
	#ifdef __DEBUG__
		auto iter = map.find(obj[0]);
		assert(iter != map.end());
	#endif
		object.addReplyBulk(session->sendBuf,it->second);
	}

	return false;
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






