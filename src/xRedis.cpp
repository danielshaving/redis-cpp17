#include "xRedis.h"

xRedis::xRedis(const char *ip,int16_t port,int16_t threadCount,bool enbaledCluster)
:server(&loop,ip,port,nullptr),
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
repli(this),
clus(this),
rdb(this),
forkEnabled(false),
forkCondWaitCount(0),
rdbChildPid(-1),
slavefd(-1),
dbnum(1)
{
	initConfig();
	loadDataFromDisk();
	server.setConnectionCallback(std::bind(&xRedis::connCallBack,this,std::placeholders::_1));
	server.setThreadNum(threadCount);
	if (threadCount > 1)
	{
		this->threadCount = threadCount;
		zmalloc_enable_thread_safeness();
	}

	server.start();
	loop.runAfter(1.0,true,std::bind(&xRedis::serverCron,this));
	loop.runAfter(120.0,true,std::bind(&xRedis::bgsaveCron,this));

	{
		std::thread thread(std::bind(&xReplication::connectMaster,&repli));
		thread.detach();
	}

	{
		std::thread thread(std::bind(&xCluster::connectCluster,&clus));
		thread.detach();
	}

	LOG_INFO<<"Ready to accept connections";
}


xRedis::~xRedis()
{
	clear();
	clearCommand();
}

bool xRedis::clearClusterMigradeCommand()
{
	return true;
}

void xRedis::replyCheck()
{

}

void xRedis::bgsaveCron()
{
	rdbSaveBackground();
}

void xRedis::serverCron()
{
	if (rdbChildPid != -1)
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
					 LOG_INFO <<"background saving terminated with success";
					if (slavefd != -1)
					{
						std::unique_lock <std::mutex> lck(slaveMutex);
						auto it = slaveConns.find(slavefd);
						if (it == slaveConns.end())
						{
							LOG_WARN<<"master sync send failure";
						}
						else
						{
							if (!rdb.rdbReplication("dump.rdb",it->second))
							{
								it->second->forceClose();
								LOG_WARN<<"master sync send failure";
							}
							else
							{
								 LOG_INFO <<"master sync send success ";
							}
						}

						slavefd = -1;
					}
				}
				else if (!bysignal && exitcode != 0)
				{
					 LOG_INFO <<"background saving error";
				}
				else
				{
					LOG_WARN<<"background saving terminated by signal "<< bysignal;
					char tmpfile[256];
					snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)rdbChildPid);
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

void xRedis::slaveRepliTimeOut(int32_t context)
{
	std::unique_lock <std::mutex> lck(slaveMutex);
	auto it = slaveConns.find(context);
	if (it != slaveConns.end())
	{
		it->second->forceClose();
	}
	LOG_INFO <<"sync connect repli timeout ";
}

void xRedis::clearCommand(std::deque<rObj*> &commands)
{
	for (auto &it : commands)
	{
		it->calHash();
		auto iter = replyCommands.find(it);
		if (iter == replyCommands.end())
		{
			zfree(it);
		}
	}
}

void xRedis::clearPubSubState(int32_t sockfd)
{
	{
		std::unique_lock <std::mutex> lck(pubsubMutex);
		for (auto &it : pubsubs)
		{
			for (auto iter = it.second.begin(); iter != it.second.end();)
			{
				if (iter->first == sockfd)
				{
					it.second.erase(iter++);
					continue;
				}
				++iter;
			}
		}
	}
}

void xRedis::setExpire(rObj *key,double when)
{
	{
		std::unique_lock <std::mutex> lck(expireMutex);
		auto it = expireTimers.find(key);
		assert(it == expireTimers.end());
	}

	auto timer = loop.runAfter(when,false,std::bind(&xRedis::setExpireTimeOut,this,key));

	{
		std::unique_lock <std::mutex> lck(expireMutex);
		expireTimers.insert(std::make_pair(key,timer));
	}
}

void xRedis::clearClusterState(int32_t sockfd)
{
	
}

void xRedis::clearRepliState(int32_t sockfd)
{
	{
		std::unique_lock <std::mutex> lck(slaveMutex);
		auto it = slaveConns.find(sockfd);
		if (it != slaveConns.end())
		{
			salveCount--;
			slaveConns.erase(sockfd);
			if (slaveConns.size() == 0)
			{
				repliEnabled = false;
				xBuffer buffer;
				slaveCached.swap(buffer);
			}
		}

		auto iter = repliTimers.find(sockfd);
		if (iter != repliTimers.end())
		{
			loop.cancelAfter(iter->second);
			repliTimers.erase(iter);
		}
	}
}

void xRedis::connCallBack(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		//socket.setTcpNoDelay(conn->getSockfd(),true);

		char buf[64] = "";
		uint16_t port = 0;
		auto addr = socket.getPeerAddr(conn->getSockfd());
		socket.toIp(buf,sizeof(buf),(const  struct sockaddr *)&addr);
		socket.toPort(&port,(const struct sockaddr *)&addr);
		conn->setip(buf);
		conn->setport(port);

		std::shared_ptr<xSession> session (new xSession(this,conn));
		std::unique_lock <std::mutex> lck(mtx);
		auto it = sessions.find(conn->getSockfd());
		assert(it == sessions.end());
		sessions[conn->getSockfd()] = session;
		// LOG_INFO <<"Client connect success";
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
			auto it = sessions.find(conn->getSockfd());
			assert(it != sessions.end());
			sessions.erase(conn->getSockfd());
		}

		// LOG_INFO <<"Client disconnect";
	}
}

void xRedis::loadDataFromDisk()
{
	int64_t start = mstime();
	if (rdb.rdbLoad("dump.rdb") == REDIS_OK)
	{
		int64_t end = mstime();
		LOG_INFO <<"db loaded from disk milseconds: "<<end - start;
	}
	else if (errno != ENOENT)
	{
       	LOG_WARN <<"fatal error loading the DB: Exiting."<<strerror(errno);
 	}
}

bool xRedis::subscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() < 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown subscribe error");
		return false;
	}

	bool retval;
	int sub = 0;
	for (int i = 0; i < obj.size(); i++)
	{
		{
			std::unique_lock <std::mutex> lck(pubsubMutex);
			auto it = pubsubs.find(obj[i]);
			if (it == pubsubs.end())
			{
				std::unordered_map<int32_t,TcpConnectionPtr> maps;
				maps[session->clientConn->getSockfd()] = session->clientConn;
				pubsubs[obj[i]] = std::move(maps);
				retval = true;
				sub++;
			}
			else
			{
				retval = false;
				auto iter = it->second.find(session->clientConn->getSockfd());
				if (iter == it->second.end())
				{
					sub++;
				}
				it->second[session->clientConn->getSockfd()] = session->clientConn;
			}
		}

		addReply(session->clientBuffer,shared.mbulkhdr[3]);
		addReply(session->clientBuffer,shared.subscribebulk);
		addReplyBulk(session->clientBuffer,obj[i]);
		addReplyLongLong(session->clientBuffer,sub);

		if (!retval)
		{
			zfree(obj[i]);
		}
	}

	return true;
}

bool xRedis::unsubscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	return true;
}

bool xRedis::psubscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	return true;
}

bool xRedis::punsubscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	return true;
}

bool xRedis::publishCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	return true;
}

bool xRedis::pubsubCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	return true;
}

bool xRedis::sentinelCommand(const std::deque<rObj*> &obj, const SessionPtr &session)
{
	return false;
}

bool xRedis::memoryCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size()  > 2)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown memory error");
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
			addReply(session->clientBuffer, ok);
			return false;
		}
	}

#endif

	addReply(session->clientBuffer,shared.ok);
	return false;
}

bool xRedis::infoCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size()  < 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown info error");
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
	size_t total_system_mem = zmalloc_get_memory_size();
	bytesToHuman(hmem,zmalloc_used);
	bytesToHuman(peak_hmem,zmalloc_used);
	bytesToHuman(total_system_hmem,total_system_mem);
	bytesToHuman(used_memory_rss_hmem,zmalloc_get_rss());
	bytesToHuman(maxmemory_hmem,8);

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
	(unsigned int32_t)total_system_mem,
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
		for (auto &it : slaveConns )
		{
			info = sdscat(info,"\r\n");
			info = sdscatprintf(info,
			"# SlaveInfo \r\n"
			"slave_ip:%s\r\n"
			"slave_port:%d\r\n",
			it.second->getip(),
			it.second->getport());
		}
	}
	
	addReplyBulkSds(session->clientBuffer,info);
	
	return false ;
}

bool xRedis::clientCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown client error");
		return false;
	}

	addReply(session->clientBuffer,shared.ok);

	return false;
}

bool xRedis::echoCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown echo error");
		return false;
	}

	addReplyBulk(session->clientBuffer,obj[0]);
	return false;
}

bool xRedis::authCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown auth error");
		return false;
	}

	if (password.c_str() == nullptr)
	{
		addReplyError(session->clientBuffer,"client sent auth, but no password is set");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,password.c_str()))
	{
		session->authEnabled = true;
		addReply(session->clientBuffer,shared.ok);
	}
	else
	{
		addReplyError(session->clientBuffer,"invalid password");
	}

	return false;
}

bool xRedis::configCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 3 || obj.size() == 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown config error");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,"set"))
	{
		if (obj.size() != 3)
		{
			addReplyErrorFormat(session->clientBuffer,"Wrong number of arguments for CONFIG %s",(char*)obj[0]->ptr);
			return false;
		}

		if (!strcasecmp(obj[1]->ptr,"requirepass"))
		{
			password = obj[2]->ptr;
			authEnabled = true;
			session->authEnabled = false;
			addReply(session->clientBuffer,shared.ok);
		}
		else
		{
			addReplyErrorFormat(session->clientBuffer,"Invalid argument for CONFIG SET '%s'",
				(char*)obj[1]->ptr);
		}

	}
	else
	{
		addReplyError(session->clientBuffer,"config subcommand must be one of GET, SET, RESETSTAT, REWRITE");
	}

	return false;

}

bool xRedis::migrateCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() < 5)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown migrate error");
		return false;
	}

	if (!clusterEnabled)
	{
		addReplyError(session->clientBuffer,"this instance has cluster support disabled");
		return false;
	}

	int32_t firstKey = 3; /* Argument index of the first key. */
	int32_t numKeys = 1;  /* By default only migrate the 'key' argument. */

	int8_t copy = 0,replace = 0;
	char *password = nullptr;

	for (int i = 5; i < obj.size(); i++)
	{
		int moreargs = i < obj.size() - 1;
		if (!strcasecmp(obj[i]->ptr,"copy"))
		{
			copy = 1;
		}
		else if (!strcasecmp(obj[i]->ptr,"replace"))
		{
			replace = 1;
		}
		else if (!strcasecmp(obj[i]->ptr,"auth"))
		{
			if (!moreargs) 
			{
				addReply(session->clientBuffer,shared.syntaxerr);
				return false;
			}
			i++;
			password = obj[i]->ptr;
		}
		else if (!strcasecmp(obj[i]->ptr,"keys"))
		{
			if (sdslen(obj[2]->ptr) != 0)
			{
				addReplyError(session->clientBuffer,
					"When using MIGRATE KEYS option, the key argument"
					" must be set to the empty string");
				return false;
			}
			firstKey = i + 1;
			numKeys = obj.size() - i - 1;
			break;
		}
		else
		{
			addReply(session->clientBuffer,shared.syntaxerr);
			return false;
		}
	}

	int64_t port;
	int32_t timeout;
	int32_t dbid;
	/* Sanity check */
	if (getLongFromObjectOrReply(session->clientBuffer,obj[4],&timeout,nullptr) != REDIS_OK ||
		getLongFromObjectOrReply(session->clientBuffer,obj[3],&dbid,nullptr) != REDIS_OK)
	{
		return false;
	}

	if (timeout <= 0) timeout = 1000;

	if (getLongLongFromObject(obj[1], &port) != REDIS_OK)
	{
		addReplyErrorFormat(session->clientBuffer,"Invalid TCP port specified: %s",
			(char*)obj[2]->ptr);
		return false;
	}

	std::string ip = obj[0]->ptr;
	if (this->ip == ip && this->port == port)
	{
		addReplyErrorFormat(session->clientBuffer,"migrate  self server error ");
		return false;
	}
	
	clus.replicationToNode(obj,session,ip,port,copy,replace,numKeys,firstKey);
	addReply(session->clientBuffer,shared.ok);

	return false;
}

bool xRedis::clusterCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (!clusterEnabled)
	{
		addReplyError(session->clientBuffer,"this instance has cluster support disabled");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,"meet"))
	{
		if (obj.size() != 3)
		{
			addReplyErrorFormat(session->clientBuffer,"unknown cluster error");
			return false;
		}

		int64_t port;

		if (getLongLongFromObject(obj[2],&port) != REDIS_OK)
		{
			addReplyErrorFormat(session->clientBuffer,"Invalid TCP port specified: %s",(char*)obj[2]->ptr);
			return false;
		}

		if (ip.c_str() && !memcmp(ip.c_str(),obj[1]->ptr,sdslen(obj[1]->ptr)) && this->port == port)
		{
			LOG_WARN << "cluster meet connect self error .";
			addReplyErrorFormat(session->clientBuffer,"Don't connect self ");
			return false;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto &it : clusterConns)
			{
				if (port == it.second->getport() && !memcmp(it.second->getip(),obj[1]->ptr,sdslen(obj[1]->ptr)))
				{
					LOG_WARN << "cluster  meet  already exists .";
					addReplyErrorFormat(session->clientBuffer,"cluster  meet  already exists ");
					return false;
				}
			}
		}
		
		if (!clus.connSetCluster(obj[1]->ptr,port))
		{
			addReplyErrorFormat(session->clientBuffer,"Invaild node address specified: %s:%s",(char*)obj[1]->ptr,(char*)obj[2]->ptr);
			return false;
		}
		
	}
	else if (!strcasecmp(obj[0]->ptr,"connect") && obj.size() == 3)
	{
		int64_t  port;
	
		if (getLongLongFromObject(obj[2],&port) != REDIS_OK)
		{
			addReplyError(session->clientBuffer,"Invalid or out of range port");
			return  false;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto &it : clusterConns)
			{
				if (port == it.second->getport() && !memcmp(it.second->getip(),obj[1]->ptr,sdslen(obj[1]->ptr)))
				{
					return false;
				}
			}
		}
	
		clus.connSetCluster(obj[1]->ptr,port);
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"info") && obj.size() == 1)
	{
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"flushslots") && obj.size() == 1)
	{
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"saveconfig") && obj.size() == 1)
	{
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"countkeysinslot") && obj.size() == 2)
	{
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"forget") && obj.size() == 2)
	{
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"slaves") && obj.size() == 2)
	{
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"nodes") && obj.size() == 1)
	{
		rObj *o = createObject(OBJ_STRING,clus.showClusterNodes());
		addReplyBulk(session->clientBuffer,o);
		decrRefCount(o);
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"getkeysinslot") && obj.size() == 3)
	{
		int64_t maxkeys, slot;
		uint32_t numkeys, j;

		if (getLongLongFromObjectOrReply(session->clientBuffer,obj[1],&slot,nullptr) != REDIS_OK)
			return false;
			
		if (getLongLongFromObjectOrReply(session->clientBuffer,obj[2],&maxkeys,nullptr) != REDIS_OK)
			return false;
	
		if (slot < 0 || slot >= 16384 || maxkeys < 0) 
		{
			addReplyError(session->clientBuffer,"Invalid slot or number of keys");
			return false;
		}
		
		std::vector<rObj*> keys;
		clus.getKeyInSlot(slot,keys,maxkeys);
		addReplyMultiBulkLen(session->clientBuffer,numkeys);
		
		for (auto &it : keys)
		{
			addReplyBulk(session->clientBuffer,it);
			zfree(it);
		}
		
		return false;
		
	}
	else if (!strcasecmp(obj[0]->ptr,"slots") && obj.size() == 1)
	{

	}
	else if (!strcasecmp(obj[0]->ptr,"keyslot") && obj.size() == 2)
	{
		char * key = obj[1]->ptr;
		addReplyLongLong(session->clientBuffer,clus.keyHashSlot((char*)key,sdslen(key)));
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"setslot") && obj.size() >= 3)
	{		
		if (!!strcasecmp(obj[1]->ptr,"stable") )
		{
			
		}
		else if ( !strcasecmp(obj[1]->ptr,"node") )
		{
			std::string imipPort = obj[2]->ptr;
			{
				std::unique_lock <std::mutex> lck(clusterMutex);
				clus.eraseImportingSlot(imipPort);
				if (clus.getImportSlotSize() == 0)
				{
					clusterRepliImportEnabeld = false;
				}
			}

			std::string fromIp;
			int32_t fromPort;
			const char *start = obj[3]->ptr;
			const char *end = obj[3]->ptr + sdslen(obj[3]->ptr);
			const char *space = std::find(start,end,':');
			if (space != end)
			{
				std::string ip(start,space);
				fromIp = ip;
				std::string port(space + 2,end);
				int64_t value;
				string2ll(port.c_str(),port.length(),&value);
				fromPort = value;
			}
			else
			{
				 addReply(session->clientBuffer,shared.err);
				 return false;
			}
			
				
			for (int32_t i  = 4; i < obj.size(); i++)
			{
				int32_t slot;
				if ((slot = clus.getSlotOrReply(session,obj[i])) == -1)
				{
					addReplyErrorFormat(session->clientBuffer,"Invalid slot %s",obj[i]->ptr);
					return false;
				}
				
				std::unique_lock <std::mutex> lck(clusterMutex);
				auto it = clus.checkClusterSlot(slot);
				if (it != nullptr)
				{
					it->ip = fromIp;
					it->port = fromPort;
				}
				else
				{
				    LOG_WARN<<"slot not found error";
				}
			}
		
			addReply(session->clientBuffer,shared.ok);
			 LOG_INFO <<"cluster async replication success "<<imipPort;
			
			return false;
		}

		int32_t slot;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == -1)
		{
			addReplyErrorFormat(session->clientBuffer, "Invalid slot %d" ,(char*)obj[1]->ptr);
			return false;
		}

		std::string nodeName = obj[3]->ptr;
		bool mark = false;
		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			auto &clusterNode = clus.getClusterNode();
			for (auto  &it : clusterNode)
			{
				if (slot == it.first && nodeName == it.second.name)
				{
					if (ip == it.second.ip && port == it.second.port)
					{
						addReplyErrorFormat(session->clientBuffer,"setslot migrate slot  error  %d",slot);
						return false;
					}
					mark = true;
					break;
				}
			}
		}

		if (!mark)
		{
			addReplyErrorFormat(session->clientBuffer,"setslot slot node no found error ");
			return false;
		}
		
	
		if ( !strcasecmp(obj[2]->ptr,"importing") && obj.size() == 4)
		{		
			std::unique_lock <std::mutex> lck(clusterMutex);
			auto &map = clus.getImporting();
			auto it = map.find(nodeName);
			if (it == map.end())
			{
				std::unordered_set<int32_t> uset;
				uset.insert(slot);
				map.insert(std::make_pair(std::move(nodeName),std::move(uset)));
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
					addReplyErrorFormat(session->clientBuffer,"repeat importing slot :%d", slot);
					return false;
				}
			}

			clusterRepliImportEnabeld = true;
		}
		else if (!strcasecmp(obj[2]->ptr,"migrating") && obj.size() == 4)
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
					addReplyErrorFormat(session->clientBuffer,"repeat migrating slot :%d", slot);
					return false;
				}
			}
		}
		else
		{
			addReplyErrorFormat(session->clientBuffer,"Invalid  param ");
			return false;
		}
	}
	else if (!strcasecmp(obj[0]->ptr,"delsync"))
	{
		int32_t slot;
		if ((slot = clus.getSlotOrReply(session,obj[1])) == 0)
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
	else if (!strcasecmp(obj[0]->ptr,"addsync") && obj.size() == 5)
	{
		int32_t slot;
		int64_t  port;
		if ((slot = clus.getSlotOrReply(session,obj[1])) == 0)
		{
			 LOG_INFO <<"getSlotOrReply error ";
			return false;
		}

		if (getLongLongFromObject(obj[3],&port) != REDIS_OK)
		{
			addReplyError(session->clientBuffer,"Invalid or out of range port");
			return  REDIS_ERR;
		}

		std::unique_lock <std::mutex> lck(clusterMutex);
		if (clus.checkClusterSlot(slot) == nullptr)
		{
			 LOG_INFO <<"addsync success:"<< slot;
			clus.cretateClusterNode(slot,obj[2]->ptr,port,obj[4]->ptr);
		}
		else
		{
			addReplyErrorFormat(session->clientBuffer,"cluster insert error:%d",slot);
			LOG_INFO << "cluster insert error "<<slot;
		}

		return false;
		
	}
	else if (!strcasecmp(obj[0]->ptr,"delslots") && obj.size() == 2)
	{		
		std::unique_lock <std::mutex> lck(clusterMutex);
		if (clusterConns.size() == 0)
		{
			addReplyErrorFormat(session->clientBuffer,"execute cluster meet ip:port");
			return false;
		}

		int32_t slot;
		int32_t j;
		for (j = 1; j < obj.size(); j++)
		{
			if ((slot = clus.getSlotOrReply(session,obj[j])) == 0)
			{
				return false;
			}

			if (slot < 0 || slot > 16384 )
			{
				addReplyErrorFormat(session->clientBuffer,"cluster delslots range error %d:",slot);
				return false;
			}

			if (clus.checkClusterSlot(slot) == nullptr)
			{
				clus.delSlotDeques(obj[j],slot);
				clus.syncClusterSlot();
				LOG_INFO << "deslots success " << slot;
			}
			else
			{
				addReplyErrorFormat(session->clientBuffer, "not found deslots error %d:",slot);
				LOG_INFO << "not found deslots " << slot;
			}
		}

	}
	else if (!strcasecmp(obj[0]->ptr, "addslots") && obj.size() == 2)
	{
		int32_t  j, slot;
		for (j = 1; j < obj.size(); j++)
		{
			if (slot < 0 || slot > 16384 )
			{
				addReplyErrorFormat(session->clientBuffer, "cluster delslots range error %d:",slot);
				return false;
			}

			if ((slot = clus.getSlotOrReply(session, obj[j])) == 0)
			{
				return false;
			}

			std::unique_lock <std::mutex> lck(clusterMutex);
			if (clusterConns.empty())
			{
				addReplyErrorFormat(session->clientBuffer, "execute cluster meet ip:port");
				return false;
			}

			if (clus.checkClusterSlot(slot) == nullptr)
			{
				char name[CLUSTER_NAMELEN];
				getRandomHexChars(name,CLUSTER_NAMELEN);
				clus.cretateClusterNode(slot,this->ip,this->port,name);
				clus.addSlotDeques(obj[j],name);
				clus.syncClusterSlot();
				LOG_INFO << "addslots success "<< slot;
			}
			else
			{
				addReplyErrorFormat(session->clientBuffer,"Slot %d specified multiple times", slot);
				return false;
			}
		}
	
	}
	else
	{
		addReplyErrorFormat(session->clientBuffer,"unknown param error");
		return false;
	}
	
	addReply(session->clientBuffer,shared.ok);
	return false;
	
}

void xRedis::structureRedisProtocol(xBuffer &buffer,std::deque<rObj*> &robjs)
{
	int32_t len, j;
	char buf[32];
	buf[0] = '*';
	len = 1 + ll2string(buf + 1, sizeof(buf) - 1, robjs.size());
	buf[len++] = '\r';
	buf[len++] = '\n';
	buffer.append(buf, len);

	for (int32_t i = 0; i < robjs.size(); i++)
	{
		buf[0] = '$';
		len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdslen(robjs[i]->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		buffer.append(buf, len);
		buffer.append(robjs[i]->ptr, sdslen(robjs[i]->ptr));
		buffer.append("\r\n", 2);
	}
}

bool xRedis::getClusterMap(rObj *command)
{
	auto it = cluterCommands.find(command);
	if (it == cluterCommands.end())
	{
		return false;
	}
	return true;
}

bool xRedis::bgsave(const SessionPtr &session,bool enabled)
{
	if (rdbChildPid != -1)
	{
		if (!enabled)
		{
			addReplyError(session->clientBuffer,"Background save already in progress");
		}
		return false;
	}

	if (rdbSaveBackground(enabled) == REDIS_OK)
	{
		if (!enabled)
		{
			addReplyStatus(session->clientBuffer,"Background saving started");
		}
	}
	else
	{
		if (!enabled)
		{
			addReply(session->clientBuffer,shared.err);
		}
		return false;
	}
	return true;
}

bool xRedis::save(const SessionPtr &session)
{
	int64_t now = mstime();
	{
		if (rdb.rdbSave("dump.rdb") == REDIS_OK)
		{
			int64_t end = mstime();
			 LOG_INFO <<"DB saved on disk milliseconds: "<< (end - now);
		}
		else
		{
			 LOG_INFO <<"DB saved on disk error";
			return false;
		}
	}
	return true;
}

void xRedis::clearFork()
{
	for (auto &it : sessions)
	{
		it.second->clientConn->forceClose();
	}

	sessions.clear();

	for (auto &it : slaveConns)
	{
		it.second->forceClose();
	}

	slaveConns.clear();

	for (auto &it : clusterConns)
	{
		it.second->forceClose();
	}

	clusterConns.clear();
}


int32_t xRedis::rdbSaveBackground(bool enabled)
{
	if (rdbChildPid != -1) return REDIS_ERR;

	pid_t childpid;
	if ((childpid = fork()) == 0)
	{
	     clearFork();
		 int32_t retval;
		 rdb.setBlockEnable(enabled);
		 retval = rdb.rdbSave("dump.rdb");
		 if (retval == REDIS_OK)
		 {
			 size_t privateDirty = zmalloc_get_private_dirty();
			 if (privateDirty)
			 {
				  LOG_INFO <<"RDB: "<< privateDirty/(1024*1024)<<"MB of memory used by copy-on-write";
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
			return REDIS_ERR;
		}

		rdbChildPid = childpid;
	}

	return REDIS_OK; /* unreached */
}

bool xRedis::bgsaveCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown bgsave error");
		return false;
	}
	
	bgsave(session);
	return true;
}

bool xRedis::saveCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown save error");
		return false;
	}

	if (rdbChildPid != -1)
	{
		addReplyError(session->clientBuffer,"Background save already in progress");
		return false;
	}


	if (save(session))
	{
		addReply(session->clientBuffer,shared.ok);
	}
	else
	{
		addReply(session->clientBuffer,shared.err);
	}

	return true;
}

bool xRedis::slaveofCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() !=  2)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown slaveof error");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,"no") &&!strcasecmp(obj[1]->ptr,"one")) 
	{
		if (masterHost.c_str() && masterPort) 
		{
			LOG_WARN<<"master mode enabled (user request from "<<
					masterHost.c_str()<<":"<<masterPort;
			repli.disConnect();
		}
	}
	else
	{
		int32_t port;
		if ((getLongFromObjectOrReply(session->clientBuffer, obj[1], &port, nullptr) != REDIS_OK))
			return false;

		if (ip.c_str() && !memcmp(ip.c_str(),obj[0]->ptr,sdslen(obj[0]->ptr))
		&& this->port == port)
		{
			LOG_WARN<<"slave of  connect self error .";
			addReplySds(session->clientBuffer,sdsnew("don't connect master self \r\n"));
			return false;
		}

		if (masterPort > 0)
		{
			LOG_WARN<<"slave of would result into synchronization with "
					"<<the master we are already connected with. no operation performed.";
			addReplySds(session->clientBuffer,sdsnew("+ok already connected to specified master\r\n"));
			return false;
		}	

		repli.replicationSetMaster(obj[0],port);
		 LOG_INFO <<"slave of "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
	}
	
	addReply(session->clientBuffer,shared.ok);
	return false;
}

bool xRedis::commandCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{	
	addReply(session->clientBuffer,shared.ok);
	return false;
}

 void xRedis::forkWait()
 {
    forkCondWaitCount++;
    expireCondition.notify_one();

    {
        std::unique_lock <std::mutex> lck(forkMutex);
        forkCondition.wait(lck);
    }
 }

bool xRedis::lpushCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() < 2)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown lpush param error");
		return false;
	}

	obj[0]->type = OBJ_LIST;
	size_t pushed = 0;
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = listMap.find(obj[0]);
			assert(iter == listMap.end());
			std::deque<rObj*> list;
			for (int32_t i = 1; i < obj.size(); i++)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				list.push_back(obj[i]);
			}
			map.insert(obj[0]);
			listMap.insert(std::make_pair(obj[0],std::move(list)));
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert(iter->first->type == (*it)->type);

			zfree(obj[0]);

			for (int32_t i = 1; i < obj.size(); i++)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				iter->second.push_back(obj[i]);
			}
		}
	}
	addReplyLongLong(session->clientBuffer,pushed);
	return true;
}

bool xRedis::lpopCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer, "unknown  lpop  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = listMap.find(obj[0]);
			assert(iter == listMap.end());
			addReply(session->clientBuffer,shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert((*it)->type == iter->first->type);
			addReplyBulk(session->clientBuffer, iter ->second.back());
			zfree(iter ->second.back());
			iter ->second.pop_back();
			if (iter ->second.empty())
			{
				zfree(iter ->first);
				listMap.erase(iter );
				map.erase(it);
			}

		}
	}

	return false;
}

bool xRedis::lrangeCommand(const std::deque<rObj*> &obj, const SessionPtr &session)
{
	if (obj.size() != 3)
	{
		addReplyErrorFormat(session->clientBuffer, "unknown  lrange  param error");
		return false;
	}

	int32_t  start;
	int32_t end;
	if ((getLongFromObjectOrReply(session->clientBuffer, obj[1], &start, nullptr) != REDIS_OK) ||
		(getLongFromObjectOrReply(session->clientBuffer, obj[2], &end,nullptr) != REDIS_OK))
	{
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = listMap.find(obj[0]);
			assert(iter == listMap.end());

			addReply(session->clientBuffer,shared.nullbulk);
			return false;
		}

		if ((*it)->type != OBJ_LIST)
		{
			addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
			return false;
		}
		
		auto iter = listMap.find(obj[0]);
		assert(iter != listMap.end());
		assert((*it)->type == iter->first->type);
		size_t size = iter->second.size();
		if (start < 0)
		{
			start = size + start;
		}

		if (end < 0)
		{
			end = size + end;
		}

		if (start < 0)
		{
			start = 0;
		}

		if (start > end || start >= size)
		{
			addReply(session->clientBuffer,shared.emptymultibulk);
			return false;
		}

		if (end >= size)
		{
			end = size - 1;
		}

		size_t rangelen =  (end - start) + 1;
		addReplyMultiBulkLen(session->clientBuffer,rangelen);

		while(rangelen--)
		{
			addReplyBulkCBuffer(session->clientBuffer,iter->second[start]->ptr,sdslen(iter->second[start]->ptr));
			start++;
		}
	}
	return false;
}

bool xRedis::rpushCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size()  <  2)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown rpush param error");
		return false;
	}

	size_t pushed = 0;
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = listMap.find(obj[0]);
			assert(iter == listMap.end());
			obj[0]->type = OBJ_LIST;
			std::deque<rObj*> list;
			for (int64_t i = 1; i < obj.size(); i++)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				list.push_front(obj[i]);
			}
			map.insert(obj[0]);
			listMap.insert(std::make_pair(obj[0],std::move(list)));
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			

			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert(iter->first->type == (*it)->type);
			zfree(obj[0]);

			for (int32_t i = 1; i < obj.size(); ++i)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				iter->second.push_front(obj[i]);
			}
		}
	}
	
	addReplyLongLong(session->clientBuffer, pushed);
	return true;
}

bool xRedis::rpopCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size()  !=  1)
	{
		addReplyErrorFormat(session->clientBuffer, "unknown  rpop  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = listMap.find(obj[0]);
			assert(iter == listMap.end());
			addReply(session->clientBuffer,shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(session->clientBuffer,
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert((*it)->type == iter->first->type);
			addReplyBulk(session->clientBuffer, iter->second.front());
			zfree(iter->second.front());
			iter->second.pop_front();
			if (iter->second.empty())
			{
				zfree(iter->first);
				listMap.erase(iter);
				map.erase(it);
			}
		}
	}

	return false;
}

bool xRedis::llenCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer, "unknown  llen  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = listMap.find(obj[0]);
			assert(iter == listMap.end());
			addReplyLongLong(session->clientBuffer, 0);
			return false;
		}

		if ((*it)->type != OBJ_LIST)
		{
			addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
			return false;
		}

		auto iter = listMap.find(obj[0]);
		assert(iter != listMap.end());
		assert((*it)->type == iter->first->type);
		addReplyLongLong(session->clientBuffer, iter->second.size());
	}

	return false;
}

bool xRedis::syncCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() >  0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown sync  error");
		return false;
	}

	xTimer *timer = nullptr;
	{
		std::unique_lock <std::mutex> lck(slaveMutex);
		auto it = repliTimers.find(session->clientConn->getSockfd());
		if (it != repliTimers.end())
		{
			LOG_WARN<<"client repeat send sync ";
			session->clientConn->forceClose();
			return false;
		}
		
		slavefd = session->clientConn->getSockfd();
		timer = session->clientConn->getLoop()->runAfter(REPLI_TIME_OUT,
				false,std::bind(&xRedis::slaveRepliTimeOut,this,slavefd));
		repliTimers.insert(std::make_pair(session->clientConn->getSockfd(),timer));
		slaveConns.insert(std::make_pair(session->clientConn->getSockfd(),session->clientConn));

	}

	
	auto threadPoolVec = server.getThreadPool()->getAllLoops();
	for (auto &it : threadPoolVec)
	{
		if (session->clientConn->getLoop()->getThreadId() == it->getThreadId())
		{
		    continue;
		}

		it->runInLoop(std::bind(&xRedis::forkWait,this));
	}

	if (threadCount > 1)
	{
		std::unique_lock <std::mutex> lck(forkMutex);
		while(forkCondWaitCount < threadCount)
		{
		    expireCondition.wait(lck);
		}
	}
    

	repliEnabled = true;
	forkCondWaitCount = 0;
	session->clientConn->setMessageCallback(std::bind(&xReplication::slaveCallBack,
		&repli,std::placeholders::_1,std::placeholders::_2));

	if (!bgsave(session,true))
	{
		LOG_WARN << "bgsave failure";
		{
			std::unique_lock <std::mutex> lck(slaveMutex);
			repliTimers.erase(session->clientConn->getSockfd());
			session->clientConn->getLoop()->cancelAfter(timer);
			slaveConns.erase(session->clientConn->getSockfd());
		}
		slavefd = -1;
		session->clientConn->forceClose();
	}

	if (threadCount > 1)
	{
	    forkCondition.notify_all();
	}

	return true;
}


bool xRedis::psyncCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() >  0)
	{
		LOG_WARN<<"unknown psync  error";
		addReplyErrorFormat(session->clientBuffer,"unknown psync  error");
		return false;
	}

	return true;
}


int64_t xRedis::getExpire(rObj *obj)
{
	std::unique_lock <std::mutex> lck(expireMutex);
	auto it = expireTimers.find(obj);
	if(it == expireTimers.end())
	{
		return -1;
	}

	assert(it->first->type == OBJ_EXPIRE);
	return it->second->getWhen();
}

size_t xRedis::getExpireSize()
{
	std::unique_lock <std::mutex> lck(expireMutex);
	return expireTimers.size();
}

size_t xRedis::getDbsize()
{
	size_t size = 0;
	
	for (auto &it : redisShards)
	{
		std::unique_lock <std::mutex> lck(it.mtx);
		auto &map = it.redisMap;
		size += map.size();
	}

	return size;
}

bool xRedis::dbsizeCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown dbsize error");
		return false;
	}

	addReplyLongLong(session->clientBuffer,getDbsize());

	return true;
}

bool xRedis::removeCommand(rObj *obj)
{
	size_t hash = obj->hash;
	int32_t index = hash % kShards;
	auto &map = redisShards[index].redisMap;
	auto &mu = redisShards[index].mtx;
	auto &stringMap = redisShards[index].stringMap;
	auto &hashMap = redisShards[index].hashMap;
	auto &listMap = redisShards[index].listMap;
	auto &zsetMap = redisShards[index].zsetMap;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj);
		if (it != map.end())
		{
			if ((*it)->type == OBJ_STRING)
			{
				auto iter = stringMap.find(obj);
				assert(iter != stringMap.end());
				assert(iter->first->type == OBJ_STRING);
				std::unique_lock <std::mutex> lck(expireMutex);
				auto iterr = expireTimers.find(obj);
				if (iterr != expireTimers.end())
				{
					zfree(iterr->first);
					loop.cancelAfter(iterr->second);
					expireTimers.erase(iterr);
				}
				
				zfree(iter->first);
				zfree(iter->second);
				stringMap.erase(iter);
			}
			else if ((*it)->type == OBJ_HASH)
			{
				auto iter = hashMap.find(obj);
				assert(iter != hashMap.end());
				assert(iter->first->type == OBJ_HASH);
				zfree(iter->first);
				
				for (auto &iterr : iter->second)
				{
					zfree(iterr.first);
					zfree(iterr.second);
				}
				
				hashMap.erase(iter);				
			}
			else if ((*it)->type == OBJ_LIST)
			{
				auto iter = listMap.find(obj);
				assert(iter != listMap.end());
				assert(iter->first->type == OBJ_LIST);
				zfree(iter->first);

				for (auto &iterr : iter->second)
				{
					zfree(iterr);
				}
				
				listMap.erase(iter);
			}
			else if ((*it)->type == OBJ_ZSET)
			{
				auto iter = zsetMap.find(obj);
				assert(iter != zsetMap.end());
				assert(iter->first->type == OBJ_ZSET);
				assert(iter->second.keyMap.size() == iter->second.sortMap.size());
				zfree(iter->first);

				for (auto &iterr : iter->second.keyMap)
				{
					zfree(iterr.first);
				}
				
				zsetMap.erase(iter);
			}
			else if ((*it)->type == OBJ_SET)
			{
				auto iter = setMap.find(obj);
				assert(iter != setMap.end());
				assert(iter->first->type == OBJ_SET);
				zfree(iter->first);

				for (auto &iterr : iter->second)
				{
					zfree(iterr);
				}
				
				setMap.erase(iter);
			}
			else
			{
				LOG_WARN << "unkown type error " << (*it)->type;
				assert(false);
			}

			map.erase(it);
			return true;
		}
	}

	return false;
}

bool xRedis::delCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() < 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  del error");
		return false;
	}

	size_t count = 0;
	for (auto &it : obj)
	{	
		if (removeCommand(it))
		{
			count++;
		}
	}

	addReplyLongLong(session->clientBuffer,count);
	
	return false;
}

bool xRedis::pingCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown ping error");
		return false;
	}
	
	addReply(session->clientBuffer,shared.pong);
	return false;
}

bool xRedis::debugCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() == 1)
	{
		addReplyError(session->clientBuffer,"You must specify a subcommand for DEBUG. Try DEBUG HELP for info.");
	    return false;
	}

	if (!strcasecmp(obj[0]->ptr,"sleep"))
	{
		double dtime = strtod(obj[1]->ptr,nullptr);
		int64_t utime = dtime*1000000;
		struct timespec tv;

		tv.tv_sec = utime / 1000000;
		tv.tv_nsec = (utime % 1000000) * 1000;
		nanosleep(&tv,nullptr);
		addReply(session->clientBuffer,shared.ok);
		return false;
	}
	
	addReply(session->clientBuffer,shared.err);
      return false;
}


void xRedis::clear()
{
	destorySharedObjects();
}

void xRedis::clearCommand()
{
	{
		std::unique_lock <std::mutex> lck(expireMutex);
		for (auto &it : expireTimers)
		{
			assert(it.first->type == OBJ_EXPIRE);
			zfree(it.first);
			loop.cancelAfter(it.second);
			zfree(it.second);
		}
	}

	for (auto &it : redisShards)
	{
		auto &mu = it.mtx;
		auto &map = it.redisMap;
		auto &stringMap = it.stringMap;
		auto &hashMap = it.hashMap;
		auto &listMap = it.listMap;
		auto &zsetMap = it.zsetMap;
		auto &setMap = it.setMap;
		
		std::unique_lock <std::mutex> lck(mu);
		for (auto &iter : map)
		{
			if (iter->type == OBJ_STRING)
			{
				auto iterr = stringMap.find(iter);
				assert(iterr != stringMap.end());
				assert(iterr->first->type == OBJ_STRING);
				zfree(iterr->first);
				zfree(iterr->second);
				stringMap.erase(iterr);
			}
			else if (iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);
				zfree(iterr->first);
				for (auto &iterrr : iterr->second)
				{
					zfree(iterrr);
				}

				listMap.erase(iterr);
			}
			else if (iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);
				zfree(iterr->first);
			
				for (auto &iterrr : iterr->second)
				{
					zfree(iterrr.first);
					zfree(iterrr.second);
				}
				
				hashMap.erase(iterr);
			}
			else if (iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.keyMap.size() == iterr->second.sortMap.size());
				zfree(iterr->first);

				for (auto &iterrr : iterr->second.keyMap)
				{
					zfree(iterrr.first);
				}
				zsetMap.erase(iterr);
			
			}
			else if (iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);
				zfree(iterr->first);
				for (auto &iterrr : iterr->second)
				{
					zfree(iterrr);
				}
				setMap.erase(iterr);
				
			}
			else
			{
				LOG_ERROR<<"type unkown:"<<iter->type;
			}
		}
		map.clear();
	}
}

bool xRedis::keysCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1 )
	{
		addReplyErrorFormat(session->clientBuffer,"unknown keys  error");
		return false;
	}

	sds pattern =obj[0]->ptr;
	int32_t plen = sdslen(pattern),allkeys;
	uint32_t numkeys = 0;

	allkeys = (pattern[0] == '*' && pattern[1] == '\0');

	{
		for (auto &it : redisShards)
		{
			auto &mu = it.mtx;
			auto &map = it.redisMap;
			auto &stringMap = it.stringMap;
			auto &hashMap = it.hashMap;
			auto &listMap = it.listMap;
			auto &zsetMap = it.zsetMap;
			auto &setMap = it.setMap;
			std::unique_lock <std::mutex> lck(mu);
			for (auto &iter : map)
			{
				if (iter->type == OBJ_STRING)
				{
					auto iterr = stringMap.find(iter);
					assert(iterr != stringMap.end());
				}
				else if (iter->type == OBJ_LIST)
				{
					auto iterr = listMap.find(iter);
					assert(iterr != listMap.end());
				}
				else if (iter->type == OBJ_SET)
				{
					auto iterr = setMap.find(iter);
					assert(iterr != setMap.end());
				}
				else if (iter->type == OBJ_ZSET)
				{
					auto iterr = zsetMap.find(iter);
					assert(iterr != zsetMap.end());
				}
				else if (iter->type == OBJ_HASH)
				{
					auto iterr = hashMap.find(iter);
					assert(iterr != hashMap.end());
				}
				else
				{
					assert(false);
				}
				
				if (allkeys || stringmatchlen(pattern,plen,iter->ptr,sdslen(iter->ptr),0))
				{
					addReplyBulkCBuffer(session->clientBuffer,iter->ptr,sdslen(iter->ptr));
					numkeys++;
				}
			}
		}
	}
	prePendReplyLongLongWithPrefix(session->clientBuffer,numkeys);
	return false;
}

bool xRedis::flushdbCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown flushdb param error");
		return false;
	}

	clearCommand();

	addReply(session->clientBuffer,shared.ok);
	return true;
}

bool xRedis::quitCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	session->clientConn->forceClose();
	return true;
}


bool xRedis::zaddCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() < 3)
	{
		addReplyError(session->clientBuffer,"unknown  zadd  param error");
		return false;
	}

	obj[0]->type = OBJ_ZSET;

	double scores = 0;
	size_t added = 0;
	
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &zsetMap = redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			{
				auto iter = zsetMap.find(obj[0]);	
				assert(iter == zsetMap.end());
			}

			if (getDoubleFromObjectOrReply(session->clientBuffer,obj[1],&scores,nullptr) != REDIS_OK)
			{
				addReplyError(session->clientBuffer,"unknown double  param error");
				return false;
			}

			obj[2]->type = OBJ_ZSET;
			
			SortSet sortSet;
			sortSet.keyMap.insert(std::make_pair(obj[2],scores));
			sortSet.sortMap.insert(std::make_pair(scores,obj[2]));
			
			added++;
			zfree(obj[1]);

			for (int i = 3; i < obj.size(); i += 2)
			{
				obj[i + 1]->type = OBJ_ZSET;
				if (getDoubleFromObjectOrReply(session->clientBuffer,obj[1],&scores,nullptr) != REDIS_OK)
				{
					addReplyError(session->clientBuffer,"unknown double  param error");
					return false;
				}
				
				auto iter  = sortSet.keyMap.find(obj[i + 1]);
				if (iter == sortSet.keyMap.end())
				{
					sortSet.keyMap.insert(std::make_pair(obj[i + 1],scores));
					sortSet.sortMap.insert(std::make_pair(scores,obj[i + 1]));
					added++;
				}
				else
				{
					if (scores != iter->second)
					{
						bool mark = false;
						auto iterr = sortSet.sortMap.find(iter->second);
						while(iterr != sortSet.sortMap.end())
						{
							if (!memcmp(iterr->second->ptr,obj[i + 1]->ptr,sdslen(obj[i + 1]->ptr)))
							{
								rObj * v = iterr->second;
								sortSet.sortMap.erase(iterr);
								sortSet.sortMap.insert(std::make_pair(scores,v));
								mark = true;
								break;
							}
							++it;
						}

						assert(mark);
						iter->second = scores;
						added++;
					}
					
					zfree(obj[i]);
					zfree(obj[i + 1]);
						
				}
			}


			map.insert(obj[0]);
			zsetMap.insert(std::make_pair(obj[0],std::move(sortSet)));
			addReplyLongLong(session->clientBuffer,added);
			
			return true;	
		}
		else
		{
			if ((*it)->type != OBJ_ZSET)
			{
				addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = zsetMap.find(obj[0]);
			assert(iter != zsetMap.end());
			assert(iter->first->type == (*it)->type);
			
			zfree(obj[0]);
			for (int i = 1; i < obj.size(); i += 2)
			{
				obj[i + 1]->type = OBJ_ZSET;
				if (getDoubleFromObjectOrReply(session->clientBuffer,obj[i],&scores,nullptr) != REDIS_OK)
				{
					addReplyError(session->clientBuffer,"unknown double  param error");
					return false;
				}

				auto iterr = iter->second.keyMap.find(obj[i + 1]);
				if (iterr == iter->second.keyMap.end())
				{
					iter->second.keyMap.insert(std::make_pair(obj[i + 1],scores));
					iter->second.sortMap.insert(std::make_pair(scores,obj[i + 1]));
					zfree(obj[i]);
					added++;
				}
				else
				{
					if (scores != iterr->second)
					{
						bool mark = false;
						auto iterrr = iter->second.sortMap.find(iterr->second);
						while(iterrr != iter->second.sortMap.end())
						{
							if (!memcmp(iterrr->second->ptr,obj[i + 1]->ptr,sdslen(obj[i + 1]->ptr)))
							{
								rObj * v = iterrr->second;
								iter->second.sortMap.erase(iterrr);
								iter->second.sortMap.insert(std::make_pair(scores,v));
								mark = true;
								break;
							}
							++iterrr;
						}

						assert(mark);
						iterr->second = scores;
						added++;
					}
					
					zfree(obj[i + 1]);
					zfree(obj[i]);
				}
			}
		}

		addReplyLongLong(session->clientBuffer,added);
	}
	
	return true;
}

bool xRedis::zrangeCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	return zrangeGenericCommand(obj,session,0);
}

bool xRedis::zcardCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size()  != 1 )
	{
		addReplyError(session->clientBuffer,"unknown  zcard  param error");
		return false;
	}

	size_t len = 0;
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &zsetMap = redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it != map.end())
		{
			if ((*it)->type != OBJ_ZSET)
			{
				addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			auto iter = zsetMap.find(obj[0]);
			assert(iter != zsetMap.end());
			assert((*it)->type == iter->first->type);
			assert(iter->second.sortMap.size() == iter->second.keyMap.size());
			len += iter->second.sortMap.size();
		}
		else
		{
			auto iter = zsetMap.find(obj[0]);
			assert(iter == zsetMap.end());
		}
	}

	addReplyLongLong(session->clientBuffer,len);
	
	return false;
}

bool xRedis::zrevrangeCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	return zrangeGenericCommand(obj,session,1);
}

bool xRedis::scardCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1 )
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  scard error");
		return false;
	}

	size_t len = 0;
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it != map.end())
		{
			if ((*it)->type != OBJ_SET)
			{
				addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = setMap.find(obj[0]);
			assert(iter != setMap.end());
			assert(iter->first->type == (*it)->type);
			len = iter->second.size();
		}
		addReplyLongLong(session->clientBuffer,len);
	}
	return false;
}

rObj* xRedis::createDumpPayload(rObj *dump)
{
	rObj *o,*dumpobj;
	xRio payload;
	unsigned char buf[2];
	uint64_t crc;
	rdb.rioInitWithBuffer(&payload, sdsempty());
	buf[0] = REDIS_RDB_VERSION & 0xff;
	buf[1] = (REDIS_RDB_VERSION >> 8) & 0xff;
	if (rdb.createDumpPayload(&payload, dump) == REDIS_ERR)
	{
		LOG_ERROR << "RDB dump error";
		return nullptr;
	}

	payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr,buf,2);
	crc = crc64(0, (unsigned char*)payload.io.buffer.ptr,sdslen(payload.io.buffer.ptr));
	memrev64ifbe(&crc);
	payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr,&crc,8);

	dumpobj = createObject(OBJ_STRING,payload.io.buffer.ptr);
	return dumpobj;
}

bool xRedis::dumpCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  dump error");
		return false;
	}

	rObj *dumpobj = createDumpPayload(obj[0]);
	if (dumpobj == nullptr)
	{
		addReplyErrorFormat(session->clientBuffer, "RDB dump error");
		return false;
	}

	addReplyBulk(session->clientBuffer,dumpobj);
	decrRefCount(dumpobj);

	return false;
}

bool xRedis::restoreCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() < 3 || obj.size()  > 4 )
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  retore  error");
		return false;
	}

	int64_t ttl;
	int type, replace = 0;

	for (int i  = 3; i < obj.size(); i++)
	{
		if (!strcasecmp(obj[i]->ptr,"replace")) 
		{
			replace = 1;
		}
		else
		{
			addReply(session->clientBuffer,shared.syntaxerr);
        	return false;
		}
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end() && !replace)
		{
			addReply(session->clientBuffer,shared.busykeyerr);
			return false;
		}
	}

	if (getLongLongFromObjectOrReply(session->clientBuffer,obj[1],&ttl,nullptr) != REDIS_OK)
	{
		return false;
	} 
	else if (ttl < 0) 
	{
		addReplyError(session->clientBuffer,"Invalid TTL value, must be >= 0");
		return false;
	}


	if (replace)
	{
		removeCommand(obj[0]);
	}

	xRio payload;
	size_t len = sdslen(obj[2]->ptr);
	unsigned char *p = (unsigned char *)obj[2]->ptr;
	unsigned char *footer;
	uint16_t rdbver;
	uint64_t crc;

	if (len < 10) 
	{
		LOG_ERROR<<"DUMP payload version or checksum are wrong";
		return false;
	}
	
	footer = p+(len-10);
	rdbver = (footer[1] << 8) | footer[0];
	if (rdbver > REDIS_RDB_VERSION) 
	{
		LOG_ERROR<<"DUMP payload version or checksum are wrong";
		return false;
	}
	crc = crc64(0,p,len-8);
	memrev64ifbe(&crc);
	if (memcmp(&crc,footer+2,8) != 0)
	{
		LOG_ERROR<<"DUMP payload version or checksum are wrong";
		return false;
	}

	rdb.rioInitWithBuffer(&payload,obj[2]->ptr);
	rObj *key = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
	if (rdb.verifyDumpPayload(&payload,key) == REDIS_ERR)
	{
		zfree(key);
		addReplyError(session->clientBuffer,"Bad data format");
		return false;
	}

	if (ttl > 0)
	{
		rObj *ex = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
		ex->calHash();
		ex->type = OBJ_EXPIRE;
		std::unique_lock <std::mutex> lck(slaveMutex);
		xTimer *timer = loop.runAfter(ttl / 1000,false,std::bind(&xRedis::setExpireTimeOut,this,ex));
		auto it = expireTimers.find(ex);
		assert(it == expireTimers.end());
		expireTimers.insert(std::make_pair(ex,timer));	
	}
	
	addReply(session->clientBuffer,shared.ok);
	return false;

}

bool xRedis::existsCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  exists error");
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			addReplyLongLong(session->clientBuffer,0);
		}
		else
		{
			addReplyLongLong(session->clientBuffer,1);
		}
	}
	return true;
}


bool xRedis::saddCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() < 2)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  sadd error");
		return false;
	}

	obj[0]->type = OBJ_SET;
	
	size_t len = 0;
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = setMap.find(obj[0]);
			assert(iter == setMap.end());
			
			std::unordered_set<rObj*,Hash,Equal> set;
			for (int i = 1; i < obj.size(); i ++)
			{
				obj[i]->type = OBJ_SET;
				auto iterr = set.find(obj[i]);
				if (iterr == set.end())
				{
					set.insert(obj[i]);
					len++;
				}
				else
				{
					zfree(obj[i]);
				}
				setMap.insert(std::make_pair(obj[0],std::move(set)));
				map.insert(obj[0]);
			}
		}
		else
		{
			if ((*it)->type != OBJ_SET)
			{
				addReplyErrorFormat(session->clientBuffer,
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = setMap.find(obj[0]);
			assert(iter != setMap.end());
			assert(iter->first->type == (*it)->type);
			zfree(obj[0]);

			for (int i = 1; i < obj.size(); i++)
			{
				obj[i]->type = OBJ_SET;
				auto iterr = iter->second.find(obj[i]);
				if (iterr == iter->second.end())
				{
					iter->second.insert(obj[i]);
					len++;
				}
				else
				{
					zfree(obj[i]);
				}
			}
		}
	}
		
	addReplyLongLong(session->clientBuffer,len);
	return true;
}	

bool xRedis::zrangeGenericCommand(const std::deque<rObj*> &obj,const SessionPtr &session,int reverse)
{
	if (obj.size()  != 4)
	{
		addReplyError(session->clientBuffer,"unknown  zrange  param error");
		return false;
	}

	int rangelen;
	int withscores = 0;
	int64_t start;
	
	if (getLongLongFromObjectOrReply(session->clientBuffer,obj[1],&start,nullptr) != REDIS_OK)
	{
		addReplyError(session->clientBuffer,"unknown double  param error");
		return false;
	}

	int64_t end;

	if (getLongLongFromObjectOrReply(session->clientBuffer,obj[2],&end,nullptr) != REDIS_OK)
	{
		addReplyError(session->clientBuffer,"unknown double  param error");
		return false;
	}

	if ( !strcasecmp(obj[3]->ptr,"withscores"))
	{
		withscores = 1;
	}
	else if (obj.size() >= 5)
	{
		addReply(session->clientBuffer,shared.syntaxerr);
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &zsetMap = redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = zsetMap.find(obj[0]);
			assert(iter == zsetMap.end());
			addReply(session->clientBuffer,shared.emptymultibulk);
			return false;
		}
		else
		{
			if ((*it)->type != OBJ_ZSET)
			{
				addReplyErrorFormat(session->clientBuffer,
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = zsetMap.find(obj[0]);
			assert(iter != zsetMap.end());
			assert(iter->second.sortMap.size() == iter->second.keyMap.size());
			assert(iter->first->type == (*it)->type);

			size_t llen = iter->second.sortMap.size();

			if (start < 0) start = llen+start;
			if (end < 0) end = llen+end;
			if (start < 0) start = 0;

			if (start > end || start >= llen) 
			{
				addReply(session->clientBuffer,shared.emptymultibulk);
				return false;
			}

			if (end >= llen) 
			{
				end = llen-1;	
			}

			rangelen = (end-start) + 1;	
			addReplyMultiBulkLen(session->clientBuffer, withscores ? (rangelen*2) : rangelen);

			if (reverse)
			{
				int count = 0;
				for (auto iterr = iter->second.sortMap.rbegin(); iterr != iter->second.sortMap.rend(); ++iterr )
				{
					if (count++ >= start)
					{
						addReplyBulkCBuffer(session->clientBuffer,
							iterr ->second->ptr,sdslen(iterr ->second->ptr));
						if (withscores)
						{
							addReplyDouble(session->clientBuffer,iterr->first);
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
				int count = 0;
				for (auto iterr = iter->second.sortMap.begin(); iterr != iter->second.sortMap.end(); ++iterr )
				{
					if (count++ >= start)
					{
						addReplyBulkCBuffer(session->clientBuffer,
							iterr ->second->ptr, sdslen(iterr ->second->ptr));
						if (withscores)
						{
							addReplyDouble(session->clientBuffer,iterr->first);
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


bool xRedis::hgetallCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  hgetall error");
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = hashMap.find(obj[0]);
			assert(iter == hashMap.end());
			addReply(session->clientBuffer,shared.emptymultibulk);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->clientBuffer,
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			addReplyMultiBulkLen(session->clientBuffer,iter->second.size() * 2);
			for (auto &iterr : iter->second)
			{
				addReplyBulkCBuffer(session->clientBuffer,iterr.first->ptr,sdslen(iterr.first->ptr));
				addReplyBulkCBuffer(session->clientBuffer,iterr.second->ptr,sdslen(iterr.second->ptr));
			}
		}
	}
	
	return false;
}

bool xRedis::hgetCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 2)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  hget  param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = hashMap.find(obj[0]);
			assert(iter == hashMap.end());
			addReply(session->clientBuffer,shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->clientBuffer,"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			auto iterr = iter->second.find(obj[1]);
			if (iterr == iter->second.end())
			{
				addReply(session->clientBuffer,shared.nullbulk);
			}
			else
			{
				addReplyBulk(session->clientBuffer,iterr->second);
			}
		}
	}

	return false;
}

bool xRedis::hkeysCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  hkeys error");
		return false;
	}

	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = hashMap.find(obj[0]);
			assert(iter == hashMap.end());
			addReply(session->clientBuffer,shared.emptymultibulk);
			return false;
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->clientBuffer,
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			addReplyMultiBulkLen(session->clientBuffer,iter->second.size());

			for (auto &iterr : iter->second)
			{
				addReplyBulkCBuffer(session->clientBuffer,iterr.first->ptr,sdslen(iterr.first->ptr));
			}
					
		}
	}	
	
	return false;
}

bool xRedis::hlenCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  hlen error");
		return false;
	}

	size_t len = 0;
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = hashMap.find(obj[0]);
			assert(iter == hashMap.end());
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->clientBuffer,
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			assert(!iter->second.empty());
			len = iter->second.size();
		}
	}

	addReplyLongLong(session->clientBuffer,len);
	return false;
}


bool xRedis::hsetCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 3)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  hset error");
		return false;
	}

	obj[0]->type = OBJ_HASH;
	obj[1]->type = OBJ_HASH;
	obj[2]->type = OBJ_HASH;
	
	bool update = false;
	
	size_t hash = obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = hashMap.find(obj[0]);
			assert(iter == hashMap.end());
			std::unordered_map<rObj*,rObj*,Hash,Equal> rhash;
			rhash.insert(std::make_pair(obj[1],obj[2]));
			hashMap.insert(std::make_pair(obj[0],std::move(rhash)));
			map.insert(obj[0]);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->clientBuffer,
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			
			zfree(obj[0]);

			auto iterr = iter->second.find(obj[1]);
			if (iterr == iter->second.end())
			{
				iter->second.insert(std::make_pair(obj[1],obj[2]));
			}
			else
			{
				zfree(obj[1]);
				zfree(iterr->second);
				iterr->second = obj[2];
				update = true;
			}
		}
	}

	addReply(session->clientBuffer,update ? shared.czero : shared.cone);
	return true;
}

bool xRedis::setCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{	
	if (obj.size() <  2 || obj.size() > 8 )
	{
		addReplyErrorFormat(session->clientBuffer,"unknown  set param error");
		return false;
	}

	int32_t j;
	rObj *expire = nullptr;
	rObj *ex = nullptr;
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
			addReply(session->clientBuffer,shared.syntaxerr);
			return false;
		}
	}
	

	int64_t milliseconds = 0;

	if (expire)
	{
		if (getLongLongFromObjectOrReply(session->clientBuffer,
				expire,&milliseconds,nullptr) != REDIS_OK)
		{
			return false;
		}
		if (milliseconds <= 0)
		{
		    addReplyErrorFormat(session->clientBuffer,"invalid expire time in");
		    return false;
		}
		if (unit == UNIT_SECONDS) milliseconds *= 1000;
	}
	
	obj[0]->type = OBJ_STRING;
	obj[1]->type = OBJ_STRING;
	
	size_t hash= obj[0]->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &stringMap = redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			if (flags & OBJ_SET_XX)
			{
				addReply(session->clientBuffer,shared.nullbulk);
				return false;
			}


			auto iter = stringMap.find(obj[0]);
			assert(iter == stringMap.end());

			map.insert(obj[0]);
			stringMap.insert(std::make_pair(obj[0],obj[1]));
			
			if (expire)
			{
				ex = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}
		}
		else
		{
			if ((*it)->type != OBJ_STRING)
			{
				addReplyErrorFormat(session->clientBuffer,
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			if (flags & OBJ_SET_NX)
			{
				addReply(session->clientBuffer,shared.nullbulk);
				return false;
			}

			if (expire)
			{
				ex = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}

			auto iter = stringMap.find(obj[0]);
			assert(iter != stringMap.end());

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
			if (iter != expireTimers.end())
			{
				zfree(iter->first);
				loop.cancelAfter(iter->second);
				expireTimers.erase(iter);
			}

			setExpire(ex,milliseconds / 1000);
		}

		for (int32_t i = 2; i < obj.size(); i++)
		{
			zfree(obj[i]);
		}
	}

	addReply(session->clientBuffer,shared.ok);
	return true;
}

bool xRedis::getCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{	
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown get param error");
		return false;
	}

	size_t hash = obj[0]->hash;
	int32_t index = hash  % kShards;
	auto &map = redisShards[index].redisMap;
	auto &mu = redisShards[index].mtx;
	auto &stringMap = redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj[0]);
		if (it == map.end())
		{
			auto iter = stringMap.find(obj[0]);
			assert(iter == stringMap.end());
			addReply(session->clientBuffer,shared.nullbulk);
			return false;
		}

		auto iter = stringMap.find(obj[0]);
		assert(iter != stringMap.end());
		assert(iter->first->type == (*it)->type);

		if ((*it)->type != OBJ_STRING)
		{
			addReplyErrorFormat(session->clientBuffer,
				"WRONGTYPE Operation against a key holding the wrong kind of value");
			return false;
		}
		
		addReplyBulk(session->clientBuffer,iter->second);
	}

	return false;
}

bool xRedis::ttlCommand(const std::deque<rObj*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->clientBuffer,"unknown ttl param error");
		return false;
	}

	std::unique_lock <std::mutex> lck(expireMutex);
	auto it = expireTimers.find(obj[0]);
	if (it == expireTimers.end())
	{
		addReplyLongLong(session->clientBuffer,-2);
		return false;
	}
	
	int64_t ttl = it->second->getExpiration().getMicroSecondsSinceEpoch()
	 - xTimeStamp::now().getMicroSecondsSinceEpoch();
	
	addReplyLongLong(session->clientBuffer,ttl / 1000000);
	return false;
}


void xRedis::flush()
{

}

void xRedis::initConfig()
{
	LOG_INFO<<"Server initialized";
	createSharedObjects();
	char buf[32];
	int32_t len = ll2string(buf,sizeof(buf),getPort());
	shared.rPort = createStringObject(buf,len);
	shared.rIp = createStringObject((char*)(getIp().c_str()),getIp().length());

#define REGISTER_REDIS_COMMAND(msgId,func) \
	msgId->calHash(); \
	handlerCommands[msgId] = std::bind(&xRedis::func,this,std::placeholders::_1,std::placeholders::_2);
	REGISTER_REDIS_COMMAND(shared.set,setCommand);
	REGISTER_REDIS_COMMAND(shared.get,getCommand);
	REGISTER_REDIS_COMMAND(shared.hset,hsetCommand);
	REGISTER_REDIS_COMMAND(shared.hget,hgetCommand);
	REGISTER_REDIS_COMMAND(shared.hlen,hlenCommand);
	REGISTER_REDIS_COMMAND(shared.hgetall,hgetallCommand);
	REGISTER_REDIS_COMMAND(shared.lpush,lpushCommand);
	REGISTER_REDIS_COMMAND(shared.rpush,rpushCommand);
	REGISTER_REDIS_COMMAND(shared.lpop,lpopCommand);
	REGISTER_REDIS_COMMAND(shared.rpop,rpopCommand);
	REGISTER_REDIS_COMMAND(shared.lrange,lrangeCommand);
	REGISTER_REDIS_COMMAND(shared.rpop,rpopCommand);
	REGISTER_REDIS_COMMAND(shared.llen,llenCommand);
	REGISTER_REDIS_COMMAND(shared.zadd,zaddCommand);
	REGISTER_REDIS_COMMAND(shared.zrange,zrangeCommand);
	REGISTER_REDIS_COMMAND(shared.zcard,zcardCommand);
	REGISTER_REDIS_COMMAND(shared.zrevrange,zrevrangeCommand);
	REGISTER_REDIS_COMMAND(shared.scard,scardCommand);
	REGISTER_REDIS_COMMAND(shared.sadd,saddCommand);
	REGISTER_REDIS_COMMAND(shared.dump,dumpCommand);
	REGISTER_REDIS_COMMAND(shared.restore,restoreCommand);
	REGISTER_REDIS_COMMAND(shared.flushdb,flushdbCommand);
	REGISTER_REDIS_COMMAND(shared.dbsize,dbsizeCommand);
	REGISTER_REDIS_COMMAND(shared.ping,pingCommand);
	REGISTER_REDIS_COMMAND(shared.save,saveCommand);
	REGISTER_REDIS_COMMAND(shared.slaveof,slaveofCommand);
	REGISTER_REDIS_COMMAND(shared.sync,syncCommand);
	REGISTER_REDIS_COMMAND(shared.command,commandCommand);
	REGISTER_REDIS_COMMAND(shared.config,configCommand);
	REGISTER_REDIS_COMMAND(shared.auth,authCommand);
	REGISTER_REDIS_COMMAND(shared.info,infoCommand);
	REGISTER_REDIS_COMMAND(shared.echo,echoCommand);
	REGISTER_REDIS_COMMAND(shared.client,clientCommand);
	REGISTER_REDIS_COMMAND(shared.del,delCommand);
	REGISTER_REDIS_COMMAND(shared.keys,keysCommand);
	REGISTER_REDIS_COMMAND(shared.bgsave,bgsaveCommand);
	REGISTER_REDIS_COMMAND(shared.memory,memoryCommand);
	REGISTER_REDIS_COMMAND(shared.cluster,clusterCommand);
	REGISTER_REDIS_COMMAND(shared.migrate,migrateCommand);
	REGISTER_REDIS_COMMAND(shared.debug,debugCommand);
	REGISTER_REDIS_COMMAND(shared.ttl,ttlCommand);
	REGISTER_REDIS_COMMAND(shared.SET,setCommand);
	REGISTER_REDIS_COMMAND(shared.GET,getCommand);
	REGISTER_REDIS_COMMAND(shared.HSET,hsetCommand);
	REGISTER_REDIS_COMMAND(shared.HGET,hgetCommand);
	REGISTER_REDIS_COMMAND(shared.HLEN,hlenCommand);
	REGISTER_REDIS_COMMAND(shared.HGETALL,hgetallCommand);
	REGISTER_REDIS_COMMAND(shared.LPUSH,lpushCommand);
	REGISTER_REDIS_COMMAND(shared.RPUSH,rpushCommand);
	REGISTER_REDIS_COMMAND(shared.LPOP,lpopCommand);
	REGISTER_REDIS_COMMAND(shared.RPOP,rpopCommand);
	REGISTER_REDIS_COMMAND(shared.LRANGE,lrangeCommand);
	REGISTER_REDIS_COMMAND(shared.RPOP,rpopCommand);
	REGISTER_REDIS_COMMAND(shared.LLEN,llenCommand);
	REGISTER_REDIS_COMMAND(shared.ZADD,zaddCommand);
	REGISTER_REDIS_COMMAND(shared.ZRANGE,zrangeCommand);
	REGISTER_REDIS_COMMAND(shared.ZCARD,zcardCommand);
	REGISTER_REDIS_COMMAND(shared.ZREVRANGE,zrevrangeCommand);
	REGISTER_REDIS_COMMAND(shared.SCARD,scardCommand);
	REGISTER_REDIS_COMMAND(shared.SADD,saddCommand);
	REGISTER_REDIS_COMMAND(shared.DUMP,dumpCommand);
	REGISTER_REDIS_COMMAND(shared.RESTORE,restoreCommand);
	REGISTER_REDIS_COMMAND(shared.FLUSHDB,flushdbCommand);
	REGISTER_REDIS_COMMAND(shared.DBSIZE,dbsizeCommand);
	REGISTER_REDIS_COMMAND(shared.PING,pingCommand);
	REGISTER_REDIS_COMMAND(shared.SAVE,saveCommand);
	REGISTER_REDIS_COMMAND(shared.SLAVEOF,slaveofCommand);
	REGISTER_REDIS_COMMAND(shared.SYNC,syncCommand);
	REGISTER_REDIS_COMMAND(shared.COMMAND,commandCommand);
	REGISTER_REDIS_COMMAND(shared.CONFIG,configCommand);
	REGISTER_REDIS_COMMAND(shared.AUTH,authCommand);
	REGISTER_REDIS_COMMAND(shared.INFO,infoCommand);
	REGISTER_REDIS_COMMAND(shared.ECHO,echoCommand);
	REGISTER_REDIS_COMMAND(shared.CLIENT,clientCommand);
	REGISTER_REDIS_COMMAND(shared.DEL,delCommand);
	REGISTER_REDIS_COMMAND(shared.KEYS,keysCommand);
	REGISTER_REDIS_COMMAND(shared.BGSAVE,bgsaveCommand);
	REGISTER_REDIS_COMMAND(shared.MEMORY,memoryCommand);
	REGISTER_REDIS_COMMAND(shared.CLUSTER,clusterCommand);
	REGISTER_REDIS_COMMAND(shared.MIGRATE,migrateCommand);
	REGISTER_REDIS_COMMAND(shared.DEBUG,debugCommand);
	REGISTER_REDIS_COMMAND(shared.TTL,ttlCommand);

#define REGISTER_REDIS_REPLY_COMMAND(msgId) \
	msgId->calHash(); \
	replyCommands.insert(msgId);
	REGISTER_REDIS_REPLY_COMMAND(shared.addsync);
	REGISTER_REDIS_REPLY_COMMAND(shared.setslot);
	REGISTER_REDIS_REPLY_COMMAND(shared.node);
	REGISTER_REDIS_REPLY_COMMAND(shared.clusterconnect);
	REGISTER_REDIS_REPLY_COMMAND(shared.delsync);
	REGISTER_REDIS_REPLY_COMMAND(shared.cluster);
	REGISTER_REDIS_REPLY_COMMAND(shared.rIp);
	REGISTER_REDIS_REPLY_COMMAND(shared.rPort);

#define REGISTER_REDIS_CHECK_COMMAND(msgId) \
	msgId->calHash(); \
	replyCommands.insert(msgId);
	REGISTER_REDIS_CHECK_COMMAND(shared.set);
	REGISTER_REDIS_CHECK_COMMAND(shared.set);
	REGISTER_REDIS_CHECK_COMMAND(shared.lpush);
	REGISTER_REDIS_CHECK_COMMAND(shared.rpush);
	REGISTER_REDIS_CHECK_COMMAND(shared.sadd);
	REGISTER_REDIS_CHECK_COMMAND(shared.lpop);
	REGISTER_REDIS_CHECK_COMMAND(shared.rpop);
	REGISTER_REDIS_CHECK_COMMAND(shared.del);
	REGISTER_REDIS_CHECK_COMMAND(shared.flushdb);

#define REGISTER_REDIS_CLUSTER_CHECK_COMMAND(msgId) \
	msgId->calHash(); \
	cluterCommands.insert(msgId);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(shared.cluster);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(shared.migrate);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(shared.command);

	master = "master";
	slave = "slave";
	ipPort = ip + "::" + std::to_string(port);
}






