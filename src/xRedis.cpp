#include "xRedis.h"

xRedis::xRedis(const char * ip, int16_t port, int16_t threadCount,bool enbaledCluster)
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
clusterRepliMigratEnabled(false),
clusterRepliImportEnabeld(false),
forkEnabled(false),
forkCondWaitCount(0),
rdbChildPid(-1),
slavefd(-1),
count(0),
pingPong(false)
{
	initConfig();
	createSharedObjects();
	loadDataFromDisk();
	server.init(&loop, host, port,this);
	server.setConnectionCallback(std::bind(&xRedis::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	server.setThreadNum(threadCount);
	server.start();
	zmalloc_enable_thread_safeness();
	loop.runAfter(1.0,nullptr,true,std::bind(&xRedis::serverCron,this,std::placeholders::_1));
	
}

xRedis::~xRedis()
{
	clear();
	destorySharedObjects();
	clearCommand();

}




bool xRedis::clearClusterMigradeCommand(void * data)
{
	return true;
}

void xRedis::replyCheck()
{

}



void xRedis::serverCron(void * data)
{
	if(rdbChildPid != -1)
	{
		 pid_t pid;
		int statloc;

		if ((pid = wait3(&statloc,WNOHANG,nullptr)) != 0)
		{
				 int exitcode = WEXITSTATUS(statloc);
				 int bysignal = 0;

				 if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

				 if (pid == rdbChildPid)
				 {
						if (!bysignal && exitcode == 0)
						{
							LOG_INFO<<"background saving terminated with success";
							if(slavefd != -1)
							{
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

							snprintf(tmpfile,256,"temp-%d.rdb", (int) rdbChildPid);
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

void xRedis::handleTimeOut(void * data)
{
	loop.quit();
}

void xRedis::handleSetExpire(void * data)
{
	rObj * obj = (rObj*) data;
	int count = 0 ;
	removeCommand(obj,count);
}


void xRedis::handleSalveRepliTimeOut(void * data)
{
	int32_t *sockfd = (int32_t *)data;
	std::unique_lock <std::mutex> lck(slaveMutex);
	auto it = salvetcpconnMaps.find(*sockfd);
	if(it != salvetcpconnMaps.end())
	{
		it->second->forceClose();
	}
	LOG_INFO<<"sync connect repli  timeout ";
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
void xRedis::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		//socket.setTcpNoDelay(conn->getSockfd(),true);
		socket.getpeerName(conn->getSockfd(),&(conn->host),conn->port);
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


bool xRedis::scardCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1 )
	{
		addReplyErrorFormat(session->sendBuf,"unknown  scard error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;
	int count = 0;
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

	addReplyLongLong(session->sendBuf,count);

	for(auto it = obj.begin(); it != obj.end(); it ++)
	{
		zfree(*it);
	}

	return true;
}

bool xRedis::saddCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() < 2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  sadd error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;

	int count = 0;
	std::mutex &mu = setShards[hash% kShards].mtx;
	auto &set = setShards[hash% kShards].set;
	{
		std::unique_lock <std::mutex> lck(mtx);
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



bool xRedis::sentinelCommand(const std::deque<rObj*> & obj, xSession * session)
{
	return false;
}

bool xRedis::memoryCommand(const std::deque <rObj*> & obj,xSession * session)
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

bool xRedis::infoCommand(const std::deque <rObj*> & obj,xSession * session)
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
			it->second->host.c_str(),
			it->second->port);

		}

	}
	
	addReplyBulkSds(session->sendBuf, info);
	
	return false ;
}


bool xRedis::clientCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown client  error");
		return false;
	}

	addReply(session->sendBuf,shared.ok);

	return false;
}


bool xRedis::echoCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown echo  error");
		return false;
	}

	addReplyBulk(session->sendBuf,obj[0]);
	return false;
}




bool xRedis::authCommand(const std::deque <rObj*> & obj, xSession * session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->sendBuf, "unknown auth  error");
		return false;
	}

	if (password.c_str() == nullptr)
	{
		addReplyError(session->sendBuf, "client sent auth, but no password is set");
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

bool xRedis::configCommand(const std::deque <rObj*> & obj, xSession * session)
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
		addReplyError(session->sendBuf, "config subcommand must be one of GET, SET, RESETSTAT, REWRITE");
	}

	return false;

}

bool xRedis::migrateCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size() < 2)
	{
		addReplyErrorFormat(session->sendBuf, "unknown migrate  error");
	}

	if (!clusterEnabled)
	{
		addReplyError(session->sendBuf, "This instance has cluster support disabled");
		return false;
	}

	long long port;

	if (getLongLongFromObject(obj[1], &port) != REDIS_OK)
	{
		addReplyErrorFormat(session->sendBuf, "Invalid TCP port specified: %s",
			(char*)obj[2]->ptr);
		return false;
	}


	std::string ip = obj[0]->ptr;
	if (ip == host  &&  this->port == port)
	{
		addReplyErrorFormat(session->sendBuf, "migrate  self server error ");
		return false;
	}

	clus.asyncReplicationToNode( ip , port);

	addReply(session->sendBuf, shared.ok);
	return false;
}

bool xRedis::clusterCommand(const std::deque <rObj*> & obj, xSession * session)
{
	if (!clusterEnabled)
	{
		addReplyError(session->sendBuf, "This instance has cluster support disabled");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr, "meet"))
	{
		if (obj.size() != 3)
		{
			addReplyErrorFormat(session->sendBuf, "unknown cluster  error");
			return false;
		}

		long long port;

		if (getLongLongFromObject(obj[2], &port) != REDIS_OK)
		{
			addReplyErrorFormat(session->sendBuf, "Invalid TCP port specified: %s",
				(char*)obj[2]->ptr);
			return false;
		}

		if (host.c_str() && !memcmp(host.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr))
			&& this->port == port)
		{
			LOG_WARN << "cluster  meet  connect self error .";
			addReplyErrorFormat(session->sendBuf, "Don't connect self ");
			return false;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); ++it)
			{
				if (port == it->second->port && !memcmp(it->second->host.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr)))
				{
					LOG_WARN << "cluster  meet  already exists .";
					addReplyErrorFormat(session->sendBuf, "cluster  meet  already exists ");
					return false;
				}
			}
		}
		
		if(!clus.connSetCluster(obj[1]->ptr, port))
		{
			addReplyErrorFormat(session->sendBuf,"Invaild node address specified: %s:%s",(char*)obj[1]->ptr,(char*)obj[2]->ptr);
			return false;
		}
		
	}
	else if (!strcasecmp(obj[0]->ptr, "connect") && obj.size() == 3)
	{
		long long  port;
	
		if (getLongLongFromObject(obj[2], &port) != REDIS_OK)
		{
			addReplyError(session->sendBuf, "Invalid or out of range port");
			return  false;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); it++)
			{
				if (port == it->second->port && !memcmp(it->second->host.c_str(), obj[1]->ptr, sdslen(obj[1]->ptr)))
				{
					return false;
				}
			}
		}
	
		clus.connSetCluster(obj[1]->ptr, port);
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr, "info") && obj.size() == 1)
	{
		sds ci = sdsempty(), ni = sdsempty();

		ci = sdscatprintf(sdsempty(), "%s %s:%d ------connetc slot:",
			(host + "::" + std::to_string(port)).c_str(),
			host.c_str(),
			port);
		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto it = clus.clusterSlotNodes.begin(); it != clus.clusterSlotNodes.end(); ++it)
			{
				if (it->second.ip == host && it->second.port == port)
				{
					ni = sdscatprintf(sdsempty(), "%d ",
						it->first);
					ci = sdscatsds(ci, ni);
					sdsfree(ni);
				}
			}
			ci = sdscatlen(ci, "\n", 1);

			for (auto it = clustertcpconnMaps.begin(); it != clustertcpconnMaps.end(); ++it)
			{
				ni = sdscatprintf(sdsempty(), "%s %s:%d ------connetc slot:",
					(it->second->host +  "::" + std::to_string(it->second->port)).c_str(),
					it->second->host.c_str(),
					it->second->port);

				ci = sdscatsds(ci, ni);
				sdsfree(ni);
			
				for (auto iter = clus.clusterSlotNodes.begin(); iter != clus.clusterSlotNodes.end(); ++iter)
				{
					if (iter->second.ip == it->second->host && iter->second.port == it->second->port)
					{
						ni = sdscatprintf(sdsempty(), "%d ",
							iter->first);
						ci = sdscatsds(ci, ni);
						sdsfree(ni);
					}
					
				}
				ci = sdscatlen(ci, "\n", 1);

			}
		}

		rObj *o = createObject(OBJ_STRING, ci);
		addReplyBulk(session->sendBuf, o);
		decrRefCount(o);
		return false;
	}
	else if(!strcasecmp(obj[0]->ptr,"getkeysinslot") && obj.size() == 3)
	{
		long long maxkeys, slot;
		unsigned int numkeys, j;
		rObj **keys;

		if (getLongLongFromObjectOrReply(session->sendBuf,obj[1],&slot,nullptr) != REDIS_OK)
			return false;
			

		if (getLongLongFromObjectOrReply(session->sendBuf,obj[2],&maxkeys,nullptr)!= REDIS_OK)
			return false;
	
		if (slot < 0 || slot >= 16384 || maxkeys < 0) 
		{
			addReplyError(session->sendBuf,"Invalid slot or number of keys");
			return false;
		}
		
		keys = (rObj**)zmalloc(sizeof(rObj*) * maxkeys);
		
		clus.getKeyInSlot(slot,keys,maxkeys);
		addReplyMultiBulkLen(session->sendBuf,numkeys);
		for (j = 0; j < numkeys; j++)
			addReplyBulk(session->sendBuf,keys[j]);
		zfree(keys);
		return false;
		
	}
	else if (!strcasecmp(obj[0]->ptr, "slots") && obj.size() == 1)
	{

	}
	else if (!strcasecmp(obj[0]->ptr, "keyslot") && obj.size() == 2)
	{
		char * key = obj[1]->ptr;
		addReplyLongLong(session->sendBuf, clus.keyHashSlot((char*)key, sdslen(key)));
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr, "setslot") && obj.size() >= 4)
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
			int  fromPort;
			const char *start = obj[3]->ptr;
			const char *end = obj[3]->ptr + sdslen(obj[3]->ptr );
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
				
				 addReply(session->sendBuf, shared.err);
				 return false;
			}
			
				
			for(int i  = 4; i < obj.size(); i++)
			{
				int slot;
				if ((slot = clus.getSlotOrReply(session, obj[i])) == -1)
				{
					addReplyErrorFormat(session->sendBuf, "Invalid slot %d",
						(char*)obj[i]->ptr);
					return false;
				}
				
				std::unique_lock <std::mutex> lck(clusterMutex);
				auto it = clus.clusterSlotNodes.find(slot);
				if(it != clus.clusterSlotNodes.end())
				{
					it->second.ip = fromIp;
					it->second.port = fromPort;
				}
				
				
			}
		
			LOG_INFO<<"cluster async replication success "<<imipPort;
			addReply(session->sendBuf, shared.ok);
			return false;

		}

		
		int slot;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == -1)
		{
			addReplyErrorFormat(session->sendBuf, "Invalid slot %d",
				(char*)obj[1]->ptr);
			return false;
		}


		std::string nodeName = obj[3]->ptr;
		if (nodeName == host + "::" + std::to_string(port))
		{
			addReplyErrorFormat(session->sendBuf, "setslot self server error ");
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
				addReplyErrorFormat(session->sendBuf, "setslot slot node no found error ");
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
					addReplyErrorFormat(session->sendBuf, "repeat importing slot :%d", slot);
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
					addReplyErrorFormat(session->sendBuf, "repeat migrating slot :%d", slot);
					return false;
				}
			}
		}
		else
		{
			addReplyErrorFormat(session->sendBuf, "Invalid  param ");
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
		int slot;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == 0)
		{
			LOG_INFO << "getSlotOrReply error ";
			return false;
		}

		std::unique_lock <std::mutex> lck(clusterMutex);
		clus.clusterSlotNodes.erase(slot);
		LOG_INFO << "delsync success:" << slot;
		return false;

	}
	else if (!strcasecmp(obj[0]->ptr, "addsync"))
	{
		int slot;
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

		std::unique_lock <std::mutex> lck(clusterMutex);
		auto it = clus.clusterSlotNodes.find(slot);
		if (it == clus.clusterSlotNodes.end())
		{
			clusterSlotEnabled = true;
			LOG_INFO << "addsync success:" << slot;
			xClusterNode node;
			node.ip = obj[2]->ptr;
			node.port = (int32_t)p;
			clus.clusterSlotNodes.insert(std::make_pair(slot, std::move(node)));

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
			addReplyErrorFormat(session->sendBuf, "execute cluster meet ip:port");
			return false;
		}

		int slot;
		int j;
		for (j = 1; j < obj.size(); j++)
		{
			if ((slot = clus.getSlotOrReply(session, obj[j])) == 0)
			{
				return false;
			}

			if(slot < 0 || slot > 16384 )
			{
				addReplyErrorFormat(session->sendBuf, "cluster delslots range error %d:",slot);
				return false;
			}

			auto it = clus.clusterSlotNodes.find(slot);
			if (it != clus.clusterSlotNodes.end())
			{
				std::deque<rObj*> robj;
				rObj * c = createStringObject("cluster", 7);
				rObj * d = createStringObject("delsync", 7);
				robj.push_back(c);
				robj.push_back(d);
				rObj * o = createStringObject(obj[j]->ptr, sdslen(obj[j]->ptr));
				robj.push_back(o);
				clus.syncClusterSlot(robj);
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
				addReplyErrorFormat(session->sendBuf, "cluster delslots range error %d:",slot);
				return false;
			}

			if ((slot = clus.getSlotOrReply(session, obj[j])) == 0)
			{
				return false;
			}

			std::unique_lock <std::mutex> lck(clusterMutex);
			if (clustertcpconnMaps.size() == 0)
			{
				addReplyErrorFormat(session->sendBuf, "execute cluster meet ip:port");
				return false;
			}

			auto it = clus.clusterSlotNodes.find(slot);
			if (it == clus.clusterSlotNodes.end())
			{
				xClusterNode  node;
				node.ip = host;
				node.port = this->port;
				rObj * i = createStringObject((char*)(host.c_str()), host.length());
				char buf[32];
				int32_t len = ll2string(buf, sizeof(buf), this->port);
				rObj * p = createStringObject(buf, len);

				std::deque<rObj*> robj;
				rObj * c = createStringObject("cluster", 7);
				rObj * a = createStringObject("addsync", 7);
			
				robj.push_back(c);
				robj.push_back(a);
				rObj * o = createStringObject(obj[j]->ptr, sdslen(obj[j]->ptr));
				robj.push_back(o);
				robj.push_back(i);
				robj.push_back(p);

				clus.syncClusterSlot(robj);
				clus.clusterSlotNodes.insert(std::make_pair(slot, std::move(node)));
				LOG_INFO << "addslots success " << slot;
			}
			else
			{
				addReplyErrorFormat(session->sendBuf, "Slot %d specified multiple times", slot);
				return false;
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
		len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdslen(robjs[i]->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		sendBuf.append(buf, len);
		sendBuf.append(robjs[i]->ptr, sdslen(robjs[i]->ptr));
		sendBuf.append("\r\n", 2);
	}
}



bool xRedis::bgsave(xSession * session,bool enabled)
{
	if(rdbChildPid != -1)
	{
		if(!enabled)
		{
			addReplyError(session->sendBuf, "Background save already in progress");
			LOG_WARN << "rdbChildPid == -1";
		}

		return false;
	}


	if(rdbSaveBackground(session, enabled) == REDIS_OK)
	{
		if(!enabled)
		{
			addReplyStatus(session->sendBuf, "Background saving started");
		}
	}
	else
	{
		if(!enabled)
		{
			addReply(session->sendBuf, shared.err);
		}
		return false;
	}

	return true;
}

bool  xRedis::save(xSession * session)
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
int xRedis::rdbSaveBackground(xSession * session, bool enabled)
{
	if(rdbChildPid != -1)
	{
		return REDIS_ERR;
	}

	pid_t childpid;
	if ((childpid = fork()) == 0)
	{
	     forkClear();
		 int retval;
		 retval = rdb.rdbSave("dump.rdb",!enabled);
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
				addReply(session->sendBuf, shared.err);
			}
			return REDIS_ERR;
		}

		rdbChildPid = childpid;
	}

	return REDIS_OK; /* unreached */
}

bool xRedis::bgsaveCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown bgsave error");
		return false;
	}
	

	bgsave(session);


	return true;
	
}

bool xRedis::saveCommand(const std::deque <rObj*> & obj,xSession * session)
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


bool xRedis::slaveofCommand(const std::deque <rObj*> & obj,xSession * session)
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
			LOG_WARN<<"master mode enabled (user request from "<<masterHost.c_str()<<":"<<masterPort;
			repli.disconnect();
		}

	}
	else
	{
		long   port;
		if ((getLongFromObjectOrReply(session->sendBuf, obj[1], &port, nullptr) != REDIS_OK))
			return false;

		if (host.c_str() && !memcmp(host.c_str(), obj[0]->ptr,sdslen(obj[0]->ptr))
		&& this->port == port)
		{
			LOG_WARN<<"slave of  connect self error .";
			addReplySds(session->sendBuf,sdsnew("don't connect master self \r\n"));
			return false;
		}

		if (masterPort > 0)
		{
			LOG_WARN<<"slave of would result into synchronization with the master we are already connected with. no operation performed.";
			addReplySds(session->sendBuf,sdsnew("+ok already connected to specified master\r\n"));
			return false;
		}	

		repli.replicationSetMaster(this,obj[0],port);
		LOG_INFO<<"slave of "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
	}
	


	addReply(session->sendBuf,shared.ok);
	return false;
}



bool xRedis::commandCommand(const std::deque <rObj*> & obj,xSession * session)
{	
	addReply(session->sendBuf,shared.ok);
	return false;
}

 void xRedis::handleForkTimeOut(void *data)
 {
    forkCondWaitCount++;
    expireCondition.notify_one();

    {
        std::unique_lock <std::mutex> lck(forkMutex);
        forkCondition.wait(lck);
    }
 }

bool xRedis::syncCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() >  0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown sync  error");
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

	{
       auto threadPoolVec = server.getThreadPool()->getAllLoops();
       for(auto it = threadPoolVec.begin(); it != threadPoolVec.end(); ++it)
       {
            if(session->conn->getLoop()->getThreadId() == (*it)->getThreadId())
            {
                continue;
            }

            std::unique_lock <std::mutex> lck(expireMutex);
            (*it)->runAfter(0.1,nullptr,false,std::bind(&xRedis::handleForkTimeOut,this,std::placeholders::_1));
       }

	}

    {
        if(threadCount  > 0)
        {
            std::unique_lock <std::mutex> lck(forkMutex);
            while(forkCondWaitCount < threadCount - 1)
            {
                expireCondition.wait(lck);
            }
        }
    }

    repliEnabled = true;
    forkCondWaitCount = 0;

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

	if(threadCount  >  0 )
	{
		std::unique_lock <std::mutex> lck(forkMutex);
		forkCondition.notify_all();
	}

	return true;
}


bool xRedis::psyncCommand(const std::deque <rObj*> & obj,xSession * session)
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



	return size;
}

bool xRedis::dbsizeCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown dbsize error");
		return false;
	}

	addReplyLongLong(session->sendBuf,getDbsize());

	return true;
}



int  xRedis::removeCommand(rObj * obj,int &count)
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
					for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
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


bool xRedis::delCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() < 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  del error");
		return false;
	}

	int count = 0;
	for(int i = 0 ; i < obj.size(); i ++)
	{
		obj[i]->calHash();		
		removeCommand(obj[i],count);
		zfree(obj[i]);
	}

	addReplyLongLong(session->sendBuf,count);
	
	return true;
}



bool xRedis::hkeysCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hkeys error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;


    std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);

		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			addReply(session->sendBuf,shared.emptymultibulk);
			return false;
		}

		addReplyMultiBulkLen(session->sendBuf,it->second.size());

		for(auto iter = it->second.begin(); iter != it->second.end(); ++iter)
		{
			addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
		}
	}

	return false;

}

bool xRedis::pingCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown ping error");
		return false;
	}
	
	addReply(session->sendBuf,shared.pong);
	return false;
}


bool xRedis::hgetallCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hgetall error");
		return false;
	}

	obj[0]->calHash();
	size_t hash= obj[0]->hash;
	
	std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		
		auto it = hsetMap.find(obj[0]);
		if(it == hsetMap.end())
		{
			addReply(session->sendBuf,shared.emptymultibulk);
			return false;
		}

		addReplyMultiBulkLen(session->sendBuf,it->second.size() * 2);

		for(auto iter = it->second.begin(); iter != it->second.end(); ++iter)
		{
			addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
			addReplyBulkCBuffer(session->sendBuf,iter->second->ptr,sdslen(iter->second->ptr));
		}
	}

	zfree(obj[0]);

	return true;
}

bool xRedis::hlenCommand(const std::deque <rObj*> & obj,xSession * session)
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

	addReplyLongLong(session->sendBuf,count);
	return  true;
}

bool xRedis::hsetCommand(const std::deque <rObj*> & obj,xSession * session)
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
	
	addReply(session->sendBuf,update ? shared.czero : shared.cone);

	return true;
}


 bool xRedis::debugCommand(const std::deque <rObj*> & obj, xSession * session)
 {
	if (obj.size() == 1)
	{
	    addReplyError(session->sendBuf,"You must specify a subcommand for DEBUG. Try DEBUG HELP for info.");
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
		addReply(session->sendBuf,shared.ok);
		return false;
	}
	
	addReply(session->sendBuf,shared.err);
    return false;
 }

bool xRedis::hgetCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() != 2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hget  param error");
		return false;
	}
	
	obj[0]->calHash();
	obj[1]->calHash();
	size_t hash= obj[0]->hash;
	std::mutex &mu = hsetMapShards[hash% kShards].mtx;
	auto &hsetMap = hsetMapShards[hash% kShards].hsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
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




void xRedis::clear()
{
	for(auto it = handlerCommandMap.begin(); it != handlerCommandMap.end(); ++it)
	{
		zfree(it->first);
	}

	handlerCommandMap.clear();


	for(auto it = unorderedmapCommands.begin(); it != unorderedmapCommands.end(); ++it)
	{
		zfree(*it);
	}

	unorderedmapCommands.clear();

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




	
}


bool xRedis::keysCommand(const std::deque <rObj*> & obj,xSession * session)
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
		for(auto it = setMapShards.begin(); it != setMapShards.end(); ++it)
		{
			auto &map = (*it).setMap;
			std::mutex &mu = (*it).mtx;
			std::unique_lock <std::mutex> lck(mu);
			for(auto iter = map.begin(); iter != map.end(); ++iter)
			{
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
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
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
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
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
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
				addReplyBulkCBuffer(session->sendBuf,iter->first->ptr,sdslen(iter->first->ptr));
			}

		}
	}



	return false;
}


bool xRedis::flushdbCommand(const std::deque <rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  flushdb  param error");
		return false;
	}

	clearCommand();

	addReply(session->sendBuf,shared.ok);	
	return true;
}

bool xRedis::quitCommand(const std::deque <rObj*> & obj,xSession * session)
{
	session->conn->forceClose();
	return true;
}


bool xRedis::setCommand(const std::deque <rObj*> & obj,xSession * session)
{	

	if(obj.size() <  2 || obj.size() > 8 )
	{
		addReplyErrorFormat(session->sendBuf,"unknown  set param error");
		return false;
	}

	int j;
	rObj * expire = nullptr;
	rObj * ex = nullptr;
	int unit = UNIT_SECONDS;	
	int flags = OBJ_SET_NO_FLAGS;

	for (j = 2; j < obj.size();j++)
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
	std::mutex &mu = setMapShards[index].mtx;
	auto & setMap = setMapShards[index].setMap;
	{
		std::unique_lock <std::mutex>  lck (mu);
		auto it = setMap.find(obj[0]);
		if(it == setMap.end())
		{
			if(flags & OBJ_SET_XX)
			{
				addReply(session->sendBuf,shared.nullbulk);
				return false;
			}

			setMap.insert(std::make_pair(obj[0],obj[1]));

			if (expire)
			{
				ex  = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}

		}
		else
		{
			if(flags & OBJ_SET_NX)
			{
				addReply(session->sendBuf,shared.nullbulk);
				return false;
			}

			if (expire)
			{
				ex  = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
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

		for(int i = 2; i < obj.size(); i++)
		{
			zfree(obj[i]);
		}
	}

	addReply(session->sendBuf,shared.ok);
	return true;
}

bool xRedis::getCommand(const std::deque <rObj*> & obj,xSession * session)
{	
	
	if(obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  get param error");
		return false;
	}
	
	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	int index = hash  % kShards;
	std::mutex &mu = setMapShards[index].mtx;
	SetMap & setMap = setMapShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
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



bool xRedis::ttlCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf, "unknown  ttl param error");
		return false;
	}

	obj[0]->calHash();
	std::unique_lock <std::mutex> lck(expireMutex);
	auto it = expireTimers.find(obj[0]);
	if (it == expireTimers.end())
	{
		addReplyLongLong(session->sendBuf, -2);
		return false;
	}
	
	int64_t ttl = it->second->getExpiration().getMicroSecondsSinceEpoch() - xTimestamp::now().getMicroSecondsSinceEpoch();
	
	addReplyLongLong(session->sendBuf, ttl / 1000000);
	return false;
}


bool xRedis::lpushCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size()  <  2)
	{
		addReplyErrorFormat(session->sendBuf, "unknown  lpush  param error");
		return false;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	int index = hash  % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	int64_t pushed = 0;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			std::deque<rObj*> list;
			for (int i = 1; i < obj.size(); ++i)
			{
				pushed++;
				list.push_back(obj[i]);
			}
			listMap.insert(std::make_pair(obj[0], std::move(list)));
		}
		else
		{
			zfree(obj[0]);
			for (int i = 1; i < obj.size(); ++i)
			{
				pushed++;
				it->second.push_back(obj[i]);
			}
		}
	}

	addReplyLongLong(session->sendBuf, pushed);
	return true;
}


bool xRedis::lpopCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf, "unknown  lpop  param error");
		return false;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	int index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			addReply(session->sendBuf, shared.nullbulk);
		}
		else
		{
			addReplyBulk(session->sendBuf, it->second.back());
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


bool xRedis::lrangeCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size() != 3)
	{
		addReplyErrorFormat(session->sendBuf, "unknown  lrange  param error");
		return false;
	}

	long  start;
	long end;
	if ((getLongFromObjectOrReply(session->sendBuf, obj[1], &start, nullptr) != REDIS_OK) || 
		(getLongFromObjectOrReply(session->sendBuf, obj[2], &end,nullptr) != REDIS_OK))
	{
		return false;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	int index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			addReply(session->sendBuf, shared.nullbulk);
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
			addReply(session->sendBuf, shared.emptymultibulk);
			return false;
		}

		if (end >= listSize)
		{
			end = listSize - 1;
		}

		int64_t rangelen =  (end - start) + 1;
		addReplyMultiBulkLen(session->sendBuf, rangelen);

		while(rangelen--)
		{
			addReplyBulkCBuffer(session->sendBuf, it->second[start]->ptr, sdslen(it->second[start]->ptr));
			start++;
		}

	}
	

	return false;
}

bool xRedis::rpushCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size()  != 2)
	{
		addReplyErrorFormat(session->sendBuf, "unknown  rpush  param error");
		return false;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	int index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	int64_t pushed = 0;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			std::deque<rObj*> list;
			for (int i = 1; i < obj.size(); ++i)
			{
				pushed++;
				list.push_front(obj[i]);
			}

			listMap.insert(std::make_pair(obj[0], std::move(list)));
		}
		else
		{
			zfree(obj[0]);
			for (int i = 1; i < obj.size(); ++i)
			{
				pushed++;
				it->second.push_front(obj[i]);
			}
		}
	}

	addReplyLongLong(session->sendBuf, pushed);

	return true;
}


bool xRedis::llenCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->sendBuf, "unknown  llen  param error");
		return false;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	int index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			addReplyLongLong(session->sendBuf,0);
			return false;
		}

		addReplyLongLong(session->sendBuf, it->second.size());
	}
	return false;
}


bool xRedis::rpopCommand(const std::deque<rObj*> & obj, xSession * session)
{
	if (obj.size()  !=  1)
	{
		addReplyErrorFormat(session->sendBuf, "unknown  rpop  param error");
		return false;
	}

	obj[0]->calHash();
	size_t hash = obj[0]->hash;
	int index = hash % kShards;
	std::mutex &mu = listMapShards[index].mtx;
	auto & listMap = listMapShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(obj[0]);
		if (it == listMap.end())
		{
			addReply(session->sendBuf, shared.nullbulk);
		}
		else
		{
			addReplyBulk(session->sendBuf, it->second.front());
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
#define REGISTER_REDIS_COMMAND(msgId, func) \
	handlerCommandMap[msgId] = std::bind(&xRedis::func, this, std::placeholders::_1, std::placeholders::_2);
	REGISTER_REDIS_COMMAND(createStringObject("set",3),setCommand);
	REGISTER_REDIS_COMMAND(createStringObject("get",3),getCommand);
	REGISTER_REDIS_COMMAND(createStringObject("flushdb",7),flushdbCommand);
	REGISTER_REDIS_COMMAND(createStringObject("dbsize",6),dbsizeCommand);
	REGISTER_REDIS_COMMAND(createStringObject("hset",4),hsetCommand);
	REGISTER_REDIS_COMMAND(createStringObject("hget",4),hgetCommand);
	REGISTER_REDIS_COMMAND(createStringObject("hgetall",7),hgetallCommand);
	REGISTER_REDIS_COMMAND(createStringObject("ping",4),pingCommand);
	REGISTER_REDIS_COMMAND(createStringObject("save",4),saveCommand);
	REGISTER_REDIS_COMMAND(createStringObject("slaveof",7),slaveofCommand);
	REGISTER_REDIS_COMMAND(createStringObject("sync",4),syncCommand);
	REGISTER_REDIS_COMMAND(createStringObject("command",7),commandCommand);
	REGISTER_REDIS_COMMAND(createStringObject("config",6),configCommand);
	REGISTER_REDIS_COMMAND(createStringObject("auth",4),authCommand);
	REGISTER_REDIS_COMMAND(createStringObject("info",4),infoCommand);
	REGISTER_REDIS_COMMAND(createStringObject("echo",4),echoCommand);
	REGISTER_REDIS_COMMAND(createStringObject("client",5),clientCommand);
	REGISTER_REDIS_COMMAND(createStringObject("hkeys",5),hkeysCommand);
	REGISTER_REDIS_COMMAND(createStringObject("del",3),delCommand);
	REGISTER_REDIS_COMMAND(createStringObject("hlen",4),hlenCommand);
	REGISTER_REDIS_COMMAND(createStringObject("keys",4),keysCommand);
	REGISTER_REDIS_COMMAND(createStringObject("bgsave",6),bgsaveCommand);
	REGISTER_REDIS_COMMAND(createStringObject("memory",6),memoryCommand);
	REGISTER_REDIS_COMMAND(createStringObject("cluster",7),clusterCommand);
	REGISTER_REDIS_COMMAND(createStringObject("migrate",7),migrateCommand);
	REGISTER_REDIS_COMMAND(createStringObject("debug",5),debugCommand);
	REGISTER_REDIS_COMMAND(createStringObject("ttl", 3), ttlCommand);
	REGISTER_REDIS_COMMAND(createStringObject("lpush", 5), lpushCommand);
	REGISTER_REDIS_COMMAND(createStringObject("lpop", 4), lpopCommand);
	REGISTER_REDIS_COMMAND(createStringObject("rpush",5 ), rpushCommand);
	REGISTER_REDIS_COMMAND(createStringObject("lrange", 6), lrangeCommand);
	REGISTER_REDIS_COMMAND(createStringObject("rpush", 5), rpushCommand);
	REGISTER_REDIS_COMMAND(createStringObject("rpop", 4), rpopCommand);
	REGISTER_REDIS_COMMAND(createStringObject("llen", 4), llenCommand);
	REGISTER_REDIS_COMMAND(createStringObject("sadd", 4), saddCommand);
	REGISTER_REDIS_COMMAND(createStringObject("scard", 5), scardCommand);

#define REGISTER_REDIS_CHECK_COMMAND(msgId) \
	unorderedmapCommands.insert(msgId);
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("set",3));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("hset",4));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("lpush",5));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("rpush",5));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("sadd",4));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("lpop",4));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("rpop",4));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("del",3));
	REGISTER_REDIS_CHECK_COMMAND(createStringObject("flushdb",7));

#define REGISTER_REDIS_CLUSTER_CHECK_COMMAND(msgId) \
	cluterMaps.insert(msgId);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(createStringObject("cluster",7));
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(createStringObject("migrate",7));
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(createStringObject("command",7));
	
	sentiThreads =  std::shared_ptr<std::thread>(new std::thread(std::bind(&xSentinel::connectSentinel,&senti)));
	sentiThreads->detach();
	repliThreads = std::shared_ptr<std::thread>(new std::thread(std::bind(&xReplication::connectMaster,&repli)));
	repliThreads->detach();
	clusterThreads = std::shared_ptr<std::thread>(new std::thread(std::bind(&xCluster::connectCluster, &clus)));
	clusterThreads->detach();
	
	clus.init(this);
	rdb.init(this);

}


