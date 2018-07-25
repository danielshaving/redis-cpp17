#include "redis.h"

Redis::Redis(const char *ip,int16_t port,int16_t threadCount,bool enbaledCluster)
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
	server.setConnectionCallback(std::bind(&Redis::connCallBack,this,std::placeholders::_1));
	server.setThreadNum(threadCount);
	if (threadCount > 1)
	{
		this->threadCount = threadCount;
		zmalloc_enable_thread_safeness();
	}

	server.start();
	loop.runAfter(1.0,true,std::bind(&Redis::serverCron,this));
	loop.runAfter(120.0,true,std::bind(&Redis::bgsaveCron,this));

	{
		std::thread thread(std::bind(&Replication::connectMaster,&repli));
		thread.detach();
	}

	{
		std::thread thread(std::bind(&Cluster::connectCluster,&clus));
		thread.detach();
	}
	LOG_INFO<<"Ready to accept connections";
}


Redis::~Redis()
{
	clear();
	clearCommand();
}

bool Redis::clearClusterMigradeCommand()
{
    return true;
}

void Redis::replyCheck()
{

}

void Redis::bgsaveCron()
{
	rdbSaveBackground();
}

void Redis::serverCron()
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

void Redis::slaveRepliTimeOut(int32_t context)
{
	std::unique_lock <std::mutex> lck(slaveMutex);
	auto it = slaveConns.find(context);
	if (it != slaveConns.end())
	{
		it->second->forceClose();
	}
	LOG_INFO <<"sync connect repli timeout ";
}

void Redis::clearCommand(std::deque<RedisObject*> &commands)
{
	for (auto &it : commands)
	{
		it->calHash();
		auto iter = replyCommands.find(it);
		if (iter == replyCommands.end())
		{
			decrRefCount(it);
		}
	}
}

void Redis::clearPubSubState(int32_t sockfd)
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

void Redis::setExpire(RedisObject *key,double when)
{
	{
		std::unique_lock <std::mutex> lck(expireMutex);
		auto it = expireTimers.find(key);
		assert(it == expireTimers.end());
	}

	auto timer = loop.runAfter(when,false,std::bind(&Redis::setExpireTimeOut,this,key));

	{
		std::unique_lock <std::mutex> lck(expireMutex);
		expireTimers.insert(std::make_pair(key,timer));
	}
}

void Redis::clearClusterState(int32_t sockfd)
{
	
}

void Redis::clearRepliState(int32_t sockfd)
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
				Buffer buffer;
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

void Redis::connCallBack(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		//socket.setTcpNoDelay(conn->getSockfd(),true);

		char buf[64] = "";
		uint16_t port = 0;
		auto addr = socket.getPeerAddr(conn->getSockfd());
		socket.toIp(buf,sizeof(buf),(const struct sockaddr *)&addr);
		socket.toPort(&port,(const struct sockaddr *)&addr);
		conn->setip(buf);
		conn->setport(port);

		//printf("%s %d\n",buf,port);

		std::shared_ptr<Session> session (new Session(this,conn));
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

void Redis::loadDataFromDisk()
{
	int64_t start = ustime();
	if (rdb.rdbLoad("dump.rdb") == REDIS_OK)
	{
		int64_t end = ustime();
		double diff = double(end - end) / (1000 * 1000);
		LOG_INFO <<"db loaded from disk seconds: "<< double(end - start) / 1000;
	}
	else if (errno != ENOENT)
	{
       	LOG_WARN <<"fatal error loading the DB: Exiting."<<strerror(errno);
 	}
}

bool Redis::subscribeCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() < 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown subscribe error");
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
				maps[session->getClientConn()->getSockfd()] = session->getClientConn();
				pubsubs[obj[i]] = std::move(maps);
				retval = true;
				sub++;
			}
			else
			{
				retval = false;
				auto iter = it->second.find(session->getClientConn()->getSockfd());
				if (iter == it->second.end())
				{
					sub++;
				}
				it->second[session->getClientConn()->getSockfd()] = session->getClientConn();
			}
		}

		addReply(session->getClientBuffer(),shared.mbulkhdr[3]);
		addReply(session->getClientBuffer(),shared.subscribebulk);
		addReplyBulk(session->getClientBuffer(),obj[i]);
		addReplyLongLong(session->getClientBuffer(),sub);

		if (!retval)
		{
			decrRefCount(obj[i]);
		}
	}
	return true;
}

bool Redis::unsubscribeCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return true;
}

bool Redis::psubscribeCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return true;
}

bool Redis::punsubscribeCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return true;
}

bool Redis::publishCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return true;
}

bool Redis::pubsubCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return true;
}

bool Redis::sentinelCommand(const std::deque<RedisObject*> &obj, const SessionPtr &session)
{
	return false;
}

bool Redis::memoryCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size()  > 2)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown memory error");
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
			addReply(session->getClientBuffer(), ok);
			return false;
		}
	}

#endif

	addReply(session->getClientBuffer(),shared.ok);
	return false;
}

bool Redis::infoCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size()  < 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown info error");
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
	
	addReplyBulkSds(session->getClientBuffer(),info);

	return false ;
}

bool Redis::clientCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown client error");
		return false;
	}
	addReply(session->getClientBuffer(),shared.ok);
	return false;
}

bool Redis::echoCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown echo error");
		return false;
	}
	addReplyBulk(session->getClientBuffer(),obj[0]);
	return false;
}

bool Redis::authCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown auth error");
		return false;
	}

	if (password.c_str() == nullptr)
	{
		addReplyError(session->getClientBuffer(),"client sent auth, but no password is set");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,password.c_str()))
	{
		session->setAuth(true);
		addReply(session->getClientBuffer(),shared.ok);
	}
	else
	{
		addReplyError(session->getClientBuffer(),"invalid password");
	}
	return false;
}

bool Redis::configCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 3 || obj.size() == 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown config error");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,"set"))
	{
		if (obj.size() != 3)
		{
			addReplyErrorFormat(session->getClientBuffer(),"Wrong number of arguments for CONFIG %s",(char*)obj[0]->ptr);
			return false;
		}

		if (!strcasecmp(obj[1]->ptr,"requirepass"))
		{
			password = obj[2]->ptr;
			authEnabled = true;
			session->setAuth(false);
			addReply(session->getClientBuffer(),shared.ok);
		}
		else
		{
			addReplyErrorFormat(session->getClientBuffer(),"Invalid argument for CONFIG SET '%s'",
				(char*)obj[1]->ptr);
		}

	}
	else
	{
		addReplyError(session->getClientBuffer(),"config subcommand must be one of GET, SET, RESETSTAT, REWRITE");
	}
	return false;
}

bool Redis::migrateCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() < 5)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown migrate error");
		return false;
	}

	if (!clusterEnabled)
	{
		addReplyError(session->getClientBuffer(),"this instance has cluster support disabled");
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
				addReply(session->getClientBuffer(),shared.syntaxerr);
				return false;
			}
			i++;
			password = obj[i]->ptr;
		}
		else if (!strcasecmp(obj[i]->ptr,"keys"))
		{
			if (sdslen(obj[2]->ptr) != 0)
			{
				addReplyError(session->getClientBuffer(),
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
			addReply(session->getClientBuffer(),shared.syntaxerr);
			return false;
		}
	}

	int64_t port;
	int32_t timeout;
	int32_t dbid;
	/* Sanity check */
	if (getLongFromObjectOrReply(session->getClientBuffer(),obj[4],&timeout,nullptr) != REDIS_OK ||
		getLongFromObjectOrReply(session->getClientBuffer(),obj[3],&dbid,nullptr) != REDIS_OK)
	{
		return false;
	}

	if (timeout <= 0) timeout = 1000;

	if (getLongLongFromObject(obj[1], &port) != REDIS_OK)
	{
		addReplyErrorFormat(session->getClientBuffer(),"Invalid TCP port specified: %s",
			(char*)obj[2]->ptr);
		return false;
	}

	std::string ip = obj[0]->ptr;
	if (this->ip == ip && this->port == port)
	{
		addReplyErrorFormat(session->getClientBuffer(),"migrate  self server error ");
		return false;
	}
	
	clus.replicationToNode(obj,session,ip,port,copy,replace,numKeys,firstKey);
	addReply(session->getClientBuffer(),shared.ok);
	return false;
}

bool Redis::clusterCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (!clusterEnabled)
	{
		addReplyError(session->getClientBuffer(),"this instance has cluster support disabled");
		return false;
	}

	if (!strcasecmp(obj[0]->ptr,"meet"))
	{
		if (obj.size() != 3)
		{
			addReplyErrorFormat(session->getClientBuffer(),"unknown cluster error");
			return false;
		}

		int64_t port;

		if (getLongLongFromObject(obj[2],&port) != REDIS_OK)
		{
			addReplyErrorFormat(session->getClientBuffer(),"Invalid TCP port specified: %s",(char*)obj[2]->ptr);
			return false;
		}

		if (ip.c_str() && !memcmp(ip.c_str(),obj[1]->ptr,sdslen(obj[1]->ptr)) && this->port == port)
		{
			LOG_WARN << "cluster meet connect self error .";
			addReplyErrorFormat(session->getClientBuffer(),"Don't connect self ");
			return false;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto &it : clusterConns)
			{
				if (port == it.second->getport() && !memcmp(it.second->getip(),obj[1]->ptr,sdslen(obj[1]->ptr)))
				{
					LOG_WARN << "cluster  meet  already exists .";
					addReplyErrorFormat(session->getClientBuffer(),"cluster  meet  already exists ");
					return false;
				}
			}
		}
		
		if (!clus.connSetCluster(obj[1]->ptr,port))
		{
			addReplyErrorFormat(session->getClientBuffer(),"Invaild node address specified: %s:%s",(char*)obj[1]->ptr,(char*)obj[2]->ptr);
			return false;
		}
		
	}
	else if (!strcasecmp(obj[0]->ptr,"connect") && obj.size() == 3)
	{
		int64_t  port;
	
		if (getLongLongFromObject(obj[2],&port) != REDIS_OK)
		{
			addReplyError(session->getClientBuffer(),"Invalid or out of range port");
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
		RedisObject *o = createObject(OBJ_STRING,clus.showClusterNodes());
		addReplyBulk(session->getClientBuffer(),o);
		decrRefCount(o);
		return false;
	}
	else if (!strcasecmp(obj[0]->ptr,"getkeysinslot") && obj.size() == 3)
	{
		int64_t maxkeys, slot;
		uint32_t numkeys, j;

		if (getLongLongFromObjectOrReply(session->getClientBuffer(),obj[1],&slot,nullptr) != REDIS_OK)
			return false;
			
		if (getLongLongFromObjectOrReply(session->getClientBuffer(),obj[2],&maxkeys,nullptr) != REDIS_OK)
			return false;
	
		if (slot < 0 || slot >= 16384 || maxkeys < 0) 
		{
			addReplyError(session->getClientBuffer(),"Invalid slot or number of keys");
			return false;
		}
		
		std::vector<RedisObject*> keys;
		clus.getKeyInSlot(slot,keys,maxkeys);
		addReplyMultiBulkLen(session->getClientBuffer(),numkeys);
		
		for (auto &it : keys)
		{
			addReplyBulk(session->getClientBuffer(),it);
			decrRefCount(it);
		}
		return false;
		
	}
	else if (!strcasecmp(obj[0]->ptr,"slots") && obj.size() == 1)
	{

	}
	else if (!strcasecmp(obj[0]->ptr,"keyslot") && obj.size() == 2)
	{
		char * key = obj[1]->ptr;
		addReplyLongLong(session->getClientBuffer(),clus.keyHashSlot((char*)key,sdslen(key)));
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
				addReply(session->getClientBuffer(),shared.err);
				return false;
			}
			
				
			for (int32_t i  = 4; i < obj.size(); i++)
			{
				int32_t slot;
				if ((slot = clus.getSlotOrReply(session,obj[i])) == -1)
				{
					addReplyErrorFormat(session->getClientBuffer(),"Invalid slot %s",obj[i]->ptr);
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
		
			addReply(session->getClientBuffer(),shared.ok);
			 LOG_INFO <<"cluster async replication success "<<imipPort;
			
			return false;
		}

		int32_t slot;
		if ((slot = clus.getSlotOrReply(session, obj[1])) == -1)
		{
			addReplyErrorFormat(session->getClientBuffer(), "Invalid slot %d" ,(char*)obj[1]->ptr);
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
						addReplyErrorFormat(session->getClientBuffer(),"setslot migrate slot  error  %d",slot);
						return false;
					}
					mark = true;
					break;
				}
			}
		}

		if (!mark)
		{
			addReplyErrorFormat(session->getClientBuffer(),"setslot slot node no found error ");
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
					addReplyErrorFormat(session->getClientBuffer(),"repeat importing slot :%d", slot);
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
					addReplyErrorFormat(session->getClientBuffer(),"repeat migrating slot :%d", slot);
					return false;
				}
			}
		}
		else
		{
			addReplyErrorFormat(session->getClientBuffer(),"Invalid  param ");
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
			addReplyError(session->getClientBuffer(),"Invalid or out of range port");
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
			addReplyErrorFormat(session->getClientBuffer(),"cluster insert error:%d",slot);
			LOG_INFO << "cluster insert error "<<slot;
		}
		return false;
		
	}
	else if (!strcasecmp(obj[0]->ptr,"delslots") && obj.size() == 2)
	{		
		std::unique_lock <std::mutex> lck(clusterMutex);
		if (clusterConns.size() == 0)
		{
			addReplyErrorFormat(session->getClientBuffer(),"execute cluster meet ip:port");
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
				addReplyErrorFormat(session->getClientBuffer(),"cluster delslots range error %d:",slot);
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
				addReplyErrorFormat(session->getClientBuffer(), "not found deslots error %d:",slot);
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
				addReplyErrorFormat(session->getClientBuffer(), "cluster delslots range error %d:",slot);
				return false;
			}

			if ((slot = clus.getSlotOrReply(session, obj[j])) == 0)
			{
				return false;
			}

			std::unique_lock <std::mutex> lck(clusterMutex);
			if (clusterConns.empty())
			{
				addReplyErrorFormat(session->getClientBuffer(), "execute cluster meet ip:port");
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
				addReplyErrorFormat(session->getClientBuffer(),"Slot %d specified multiple times", slot);
				return false;
			}
		}
	
	}
	else
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown param error");
		return false;
	}
	
	addReply(session->getClientBuffer(),shared.ok);
	return false;
}

void Redis::structureRedisProtocol(Buffer &buffer,std::deque<RedisObject*> &robjs)
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

bool Redis::getClusterMap(RedisObject *command)
{
	auto it = cluterCommands.find(command);
	if (it == cluterCommands.end())
	{
		return false;
	}
	return true;
}

bool Redis::bgsave(const SessionPtr &session,bool enabled)
{
	if (rdbChildPid != -1)
	{
		if (!enabled)
		{
			addReplyError(session->getClientBuffer(),"Background save already in progress");
		}
		return false;
	}

	if (rdbSaveBackground(enabled) == REDIS_OK)
	{
		if (!enabled)
		{
			addReplyStatus(session->getClientBuffer(),"Background saving started");
		}
	}
	else
	{
		if (!enabled)
		{
			addReply(session->getClientBuffer(),shared.err);
		}
		return false;
	}
	return true;
}

bool Redis::save(const SessionPtr &session)
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

void Redis::clearFork()
{
	for (auto &it : sessions)
	{
		it.second->getClientConn()->forceClose();
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


int32_t Redis::rdbSaveBackground(bool enabled)
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

bool Redis::bgsaveCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown bgsave error");
		return false;
	}
	
	bgsave(session);
	return true;
}

bool Redis::saveCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown save error");
		return false;
	}

	if (rdbChildPid != -1)
	{
		addReplyError(session->getClientBuffer(),"Background save already in progress");
		return false;
	}

	if (save(session))
	{
		addReply(session->getClientBuffer(),shared.ok);
	}
	else
	{
		addReply(session->getClientBuffer(),shared.err);
	}
	return true;
}

bool Redis::slaveofCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() !=  2)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown slaveof error");
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
		if ((getLongFromObjectOrReply(session->getClientBuffer(), obj[1], &port, nullptr) != REDIS_OK))
			return false;

		if (ip.c_str() && !memcmp(ip.c_str(),obj[0]->ptr,sdslen(obj[0]->ptr))
		&& this->port == port)
		{
			LOG_WARN<<"slave of connect self error .";
			addReplySds(session->getClientBuffer(),sdsnew("don't connect master self \r\n"));
			return false;
		}

		if (masterPort > 0)
		{
			LOG_WARN<<"slave of would result into synchronization with "
					"<<the master we are already connected with. no operation performed.";
			addReplySds(session->getClientBuffer(),sdsnew("+ok already connected to specified master\r\n"));
			return false;
		}	

		repli.replicationSetMaster(obj[0],port);
		LOG_INFO <<"slave of "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
	}
	
	addReply(session->getClientBuffer(),shared.ok);
	return false;
}

bool Redis::commandCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{	
	addReply(session->getClientBuffer(),shared.ok);
	return false;
}

void Redis::forkWait()
{
	forkCondWaitCount++;
	expireCondition.notify_one();

	{
	    std::unique_lock <std::mutex> lck(forkMutex);
	    forkCondition.wait(lck);
	}
}

bool Redis::lpushCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() < 2)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown lpush param error");
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
			std::deque<RedisObject*> list;
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
				addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert(iter->first->type == (*it)->type);

			decrRefCount(obj[0]);

			for (int32_t i = 1; i < obj.size(); i++)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				iter->second.push_back(obj[i]);
			}
		}
	}
	addReplyLongLong(session->getClientBuffer(),pushed);
	return true;
}

bool Redis::lpopCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(), "unknown  lpop  param error");
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
			addReply(session->getClientBuffer(),shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert((*it)->type == iter->first->type);
			addReplyBulk(session->getClientBuffer(), iter ->second.back());
			decrRefCount(iter ->second.back());
			iter ->second.pop_back();
			if (iter ->second.empty())
			{
				decrRefCount(iter ->first);
				listMap.erase(iter );
				map.erase(it);
			}
		}
	}
	return false;
}

bool Redis::lrangeCommand(const std::deque<RedisObject*> &obj, const SessionPtr &session)
{
	if (obj.size() != 3)
	{
		addReplyErrorFormat(session->getClientBuffer(), "unknown  lrange  param error");
		return false;
	}

	int32_t  start;
	int32_t end;
	if ((getLongFromObjectOrReply(session->getClientBuffer(), obj[1], &start, nullptr) != REDIS_OK) ||
		(getLongFromObjectOrReply(session->getClientBuffer(), obj[2], &end,nullptr) != REDIS_OK))
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

			addReply(session->getClientBuffer(),shared.nullbulk);
			return false;
		}

		if ((*it)->type != OBJ_LIST)
		{
			addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
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
			addReply(session->getClientBuffer(),shared.emptymultibulk);
			return false;
		}

		if (end >= size)
		{
			end = size - 1;
		}

		size_t rangelen =  (end - start) + 1;
		addReplyMultiBulkLen(session->getClientBuffer(),rangelen);

		while(rangelen--)
		{
			addReplyBulkCBuffer(session->getClientBuffer(),iter->second[start]->ptr,sdslen(iter->second[start]->ptr));
			start++;
		}
	}
	return false;
}

bool Redis::rpushCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size()  <  2)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown rpush param error");
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
			std::deque<RedisObject*> list;
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
				addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert(iter->first->type == (*it)->type);
			decrRefCount(obj[0]);

			for (int32_t i = 1; i < obj.size(); ++i)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				iter->second.push_front(obj[i]);
			}
		}
	}
	addReplyLongLong(session->getClientBuffer(), pushed);
	return true;
}

bool Redis::rpopCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size()  !=  1)
	{
		addReplyErrorFormat(session->getClientBuffer(), "unknown  rpop  param error");
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
			addReply(session->getClientBuffer(),shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(session->getClientBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert((*it)->type == iter->first->type);
			addReplyBulk(session->getClientBuffer(), iter->second.front());
			decrRefCount(iter->second.front());
			iter->second.pop_front();
			if (iter->second.empty())
			{
				decrRefCount(iter->first);
				listMap.erase(iter);
				map.erase(it);
			}
		}
	}
	return false;
}

bool Redis::llenCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(), "unknown  llen  param error");
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
			addReplyLongLong(session->getClientBuffer(), 0);
			return false;
		}

		if ((*it)->type != OBJ_LIST)
		{
			addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
			return false;
		}

		auto iter = listMap.find(obj[0]);
		assert(iter != listMap.end());
		assert((*it)->type == iter->first->type);
		addReplyLongLong(session->getClientBuffer(), iter->second.size());
	}
	return false;
}

bool Redis::syncCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() >  0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown sync  error");
		return false;
	}

	Timer *timer = nullptr;
	{
		std::unique_lock <std::mutex> lck(slaveMutex);
		auto it = repliTimers.find(session->getClientConn()->getSockfd());
		if (it != repliTimers.end())
		{
			LOG_WARN<<"client repeat send sync ";
			session->getClientConn()->forceClose();
			return false;
		}
		
		slavefd = session->getClientConn()->getSockfd();
		timer = session->getClientConn()->getLoop()->runAfter(REPLI_TIME_OUT,
				false,std::bind(&Redis::slaveRepliTimeOut,this,slavefd));
		repliTimers.insert(std::make_pair(session->getClientConn()->getSockfd(),timer));
		slaveConns.insert(std::make_pair(session->getClientConn()->getSockfd(),session->getClientConn()));
	}
	
	auto threadPoolVec = server.getThreadPool()->getAllLoops();
	for (auto &it : threadPoolVec)
	{
		if (session->getClientConn()->getLoop()->getThreadId() == it->getThreadId())
		{
		    continue;
		}

		it->runInLoop(std::bind(&Redis::forkWait,this));
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
	session->getClientConn()->setMessageCallback(std::bind(&Replication::slaveCallBack,
		&repli,std::placeholders::_1,std::placeholders::_2));

	if (!bgsave(session,true))
	{
		LOG_WARN << "bgsave failure";
		{
			std::unique_lock <std::mutex> lck(slaveMutex);
			repliTimers.erase(session->getClientConn()->getSockfd());
			session->getClientConn()->getLoop()->cancelAfter(timer);
			slaveConns.erase(session->getClientConn()->getSockfd());
		}
		slavefd = -1;
		session->getClientConn()->forceClose();
	}

	if (threadCount > 1)
	{
	    forkCondition.notify_all();
	}
	return true;
}


bool Redis::psyncCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return false;
}

int64_t Redis::getExpire(RedisObject *obj)
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

size_t Redis::getExpireSize()
{
	std::unique_lock <std::mutex> lck(expireMutex);
	return expireTimers.size();
}

size_t Redis::getDbsize()
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

bool Redis::dbsizeCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown dbsize error");
		return false;
	}
	addReplyLongLong(session->getClientBuffer(),getDbsize());
	return true;
}

bool Redis::removeCommand(RedisObject *obj)
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
					decrRefCount(iterr->first);
					loop.cancelAfter(iterr->second);
					expireTimers.erase(iterr);
				}
				
				decrRefCount(iter->first);
				decrRefCount(iter->second);
				stringMap.erase(iter);
			}
			else if ((*it)->type == OBJ_HASH)
			{
				auto iter = hashMap.find(obj);
				assert(iter != hashMap.end());
				assert(iter->first->type == OBJ_HASH);
				decrRefCount(iter->first);
				
				for (auto &iterr : iter->second)
				{
					decrRefCount(iterr.first);
					decrRefCount(iterr.second);
				}
				
				hashMap.erase(iter);				
			}
			else if ((*it)->type == OBJ_LIST)
			{
				auto iter = listMap.find(obj);
				assert(iter != listMap.end());
				assert(iter->first->type == OBJ_LIST);
				decrRefCount(iter->first);

				for (auto &iterr : iter->second)
				{
					decrRefCount(iterr);
				}
				
				listMap.erase(iter);
			}
			else if ((*it)->type == OBJ_ZSET)
			{
				auto iter = zsetMap.find(obj);
				assert(iter != zsetMap.end());
				assert(iter->first->type == OBJ_ZSET);
				assert(iter->second.first.size() == iter->second.second.size());
				decrRefCount(iter->first);

				for (auto &iterr : iter->second.first)
				{
					decrRefCount(iterr.first);
				}
				
				zsetMap.erase(iter);
			}
			else if ((*it)->type == OBJ_SET)
			{
				auto iter = setMap.find(obj);
				assert(iter != setMap.end());
				assert(iter->first->type == OBJ_SET);
				decrRefCount(iter->first);

				for (auto &iterr : iter->second)
				{
					decrRefCount(iterr);
				}
				
				setMap.erase(iter);
			}
			else
			{
				assert(false);
			}

			map.erase(it);
			return true;
		}
	}
	return false;
}

bool Redis::delCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() < 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown  del error");
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
	addReplyLongLong(session->getClientBuffer(),count);
	return false;
}

bool Redis::pingCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown ping error");
		return false;
	}
	addReply(session->getClientBuffer(),shared.pong);
	return false;
}

bool Redis::debugCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() == 1)
	{
		addReplyError(session->getClientBuffer(),"You must specify a subcommand for DEBUG. Try DEBUG HELP for info.");
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
		addReply(session->getClientBuffer(),shared.ok);
		return false;
	}
	
	addReply(session->getClientBuffer(),shared.err);
    return false;
}

void Redis::clear()
{
	destorySharedObjects();
}

void Redis::clearCommand()
{
	{
		std::unique_lock <std::mutex> lck(expireMutex);
		for (auto &it : expireTimers)
		{
			assert(it.first->type == OBJ_EXPIRE);
			decrRefCount(it.first);
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
				decrRefCount(iterr->first);
				decrRefCount(iterr->second);
				stringMap.erase(iterr);
			}
			else if (iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);
				decrRefCount(iterr->first);
				for (auto &iterrr : iterr->second)
				{
					decrRefCount(iterrr);
				}

				listMap.erase(iterr);
			}
			else if (iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);
				decrRefCount(iterr->first);
			
				for (auto &iterrr : iterr->second)
				{
					decrRefCount(iterrr.first);
					decrRefCount(iterrr.second);
				}
				
				hashMap.erase(iterr);
			}
			else if (iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.first.size() == iterr->second.second.size());
				decrRefCount(iterr->first);

				for (auto &iterrr : iterr->second.first)
				{
					decrRefCount(iterrr.first);
				}
				zsetMap.erase(iterr);
			
			}
			else if (iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);
				decrRefCount(iterr->first);
				for (auto &iterrr : iterr->second)
				{
					decrRefCount(iterrr);
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

bool Redis::keysCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1 )
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown keys error");
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
					addReplyBulkCBuffer(session->getClientBuffer(),iter->ptr,sdslen(iter->ptr));
					numkeys++;
				}
			}
		}
	}
	prePendReplyLongLongWithPrefix(session->getClientBuffer(),numkeys);
	return false;
}

bool Redis::flushdbCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown flushdb param error");
		return false;
	}

	clearCommand();

	addReply(session->getClientBuffer(),shared.ok);
	return true;
}

bool Redis::quitCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	session->getClientConn()->forceClose();
	return true;
}

bool Redis::zaddCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() < 3)
	{
		addReplyError(session->getClientBuffer(),"unknown zadd param error");
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

			if (getDoubleFromObjectOrReply(session->getClientBuffer(),obj[1],&scores,nullptr) != REDIS_OK)
			{
				addReplyError(session->getClientBuffer(),"unknown double param error");
				return false;
			}

			obj[2]->type = OBJ_ZSET;
			
			SortIndexMap indexMap;
			SortMap sortMap;
		
			indexMap.insert(std::make_pair(obj[2],scores));
			sortMap.insert(std::make_pair(scores,obj[2]));
			
			added++;
			decrRefCount(obj[1]);

			for (int i = 3; i < obj.size(); i += 2)
			{
				obj[i + 1]->type = OBJ_ZSET;
				if (getDoubleFromObjectOrReply(session->getClientBuffer(),obj[1],&scores,nullptr) != REDIS_OK)
				{
					addReplyError(session->getClientBuffer(),"unknown double param error");
					return false;
				}
				
				auto iter = indexMap.find(obj[i + 1]);
				if (iter == indexMap.end())
				{
					indexMap.insert(std::make_pair(obj[i + 1],scores));
					sortMap.insert(std::make_pair(scores,obj[i + 1]));
					added++;
				}
				else
				{
					if (scores != iter->second)
					{
						bool mark = false;
						auto iterr = sortMap.find(iter->second);
						while(iterr != sortMap.end())
						{
							if (!memcmp(iterr->second->ptr,obj[i + 1]->ptr,sdslen(obj[i + 1]->ptr)))
							{
								RedisObject *v = iterr->second;
								sortMap.erase(iterr);
								sortMap.insert(std::make_pair(scores,v));
								mark = true;
								break;
							}
							++it;
						}

						assert(mark);
						iter->second = scores;
						added++;
					}
					
					decrRefCount(obj[i]);
					decrRefCount(obj[i + 1]);
				}
			}

			map.insert(obj[0]);
			zsetMap.insert(std::make_pair(obj[0],std::make_pair(std::move(indexMap),std::move(sortMap))));
			addReplyLongLong(session->getClientBuffer(),added);
			return true;	
		}
		else
		{
			if ((*it)->type != OBJ_ZSET)
			{
				addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = zsetMap.find(obj[0]);
			assert(iter != zsetMap.end());
			assert(iter->first->type == (*it)->type);
			
			decrRefCount(obj[0]);
			for (int i = 1; i < obj.size(); i += 2)
			{
				obj[i + 1]->type = OBJ_ZSET;
				if (getDoubleFromObjectOrReply(session->getClientBuffer(),obj[i],&scores,nullptr) != REDIS_OK)
				{
					addReplyError(session->getClientBuffer(),"unknown double  param error");
					return false;
				}

				auto iterr = iter->second.first.find(obj[i + 1]);
				if (iterr == iter->second.first.end())
				{
					iter->second.first.insert(std::make_pair(obj[i + 1],scores));
					iter->second.second.insert(std::make_pair(scores,obj[i + 1]));
					decrRefCount(obj[i]);
					added++;
				}
				else
				{
					if (scores != iterr->second)
					{
						bool mark = false;
						auto iterrr = iter->second.second.find(iterr->second);
						while(iterrr != iter->second.second.end())
						{
							if (!memcmp(iterrr->second->ptr,obj[i + 1]->ptr,sdslen(obj[i + 1]->ptr)))
							{
								RedisObject * v = iterrr->second;
								iter->second.second.erase(iterrr);
								iter->second.second.insert(std::make_pair(scores,v));
								mark = true;
								break;
							}
							++iterrr;
						}

						assert(mark);
						iterr->second = scores;
						added++;
					}
					
					decrRefCount(obj[i + 1]);
					decrRefCount(obj[i]);
				}
			}
		}
		addReplyLongLong(session->getClientBuffer(),added);
	}
	return true;
}

bool Redis::zrangeCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return zrangeGenericCommand(obj,session,0);
}

bool Redis::zcardCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size()  != 1 )
	{
		addReplyError(session->getClientBuffer(),"unknown zcard param error");
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
				addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			auto iter = zsetMap.find(obj[0]);
			assert(iter != zsetMap.end());
			assert((*it)->type == iter->first->type);
			assert(iter->second.second.size() == iter->second.first.size());
			len += iter->second.second.size();
		}
		else
		{
			auto iter = zsetMap.find(obj[0]);
			assert(iter == zsetMap.end());
		}
	}

	addReplyLongLong(session->getClientBuffer(),len);
	return false;
}

bool Redis::zrevrangeCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	return zrangeGenericCommand(obj,session,1);
}

bool Redis::scardCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1 )
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown scard error");
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
				addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = setMap.find(obj[0]);
			assert(iter != setMap.end());
			assert(iter->first->type == (*it)->type);
			len = iter->second.size();
		}
		addReplyLongLong(session->getClientBuffer(),len);
	}
	return false;
}

RedisObject* Redis::createDumpPayload(RedisObject *dump)
{
	RedisObject *o,*dumpobj;
	Rio payload;
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

bool Redis::dumpCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown dump error");
		return false;
	}

	RedisObject *dumpobj = createDumpPayload(obj[0]);
	if (dumpobj == nullptr)
	{
		addReplyErrorFormat(session->getClientBuffer(),"RDB dump error");
		return false;
	}
	addReplyBulk(session->getClientBuffer(),dumpobj);
	decrRefCount(dumpobj);
	return false;
}

bool Redis::restoreCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() < 3 || obj.size()  > 4 )
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown retore error");
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
			addReply(session->getClientBuffer(),shared.syntaxerr);
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
			addReply(session->getClientBuffer(),shared.busykeyerr);
			return false;
		}
	}

	if (getLongLongFromObjectOrReply(session->getClientBuffer(),obj[1],&ttl,nullptr) != REDIS_OK)
	{
		return false;
	} 
	else if (ttl < 0) 
	{
		addReplyError(session->getClientBuffer(),"Invalid TTL value, must be >= 0");
		return false;
	}

	if (replace)
	{
		removeCommand(obj[0]);
	}

	Rio payload;
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
	RedisObject *key = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
	if (rdb.verifyDumpPayload(&payload,key) == REDIS_ERR)
	{
		decrRefCount(key);
		addReplyError(session->getClientBuffer(),"Bad data format");
		return false;
	}

	if (ttl > 0)
	{
		RedisObject *ex = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
		ex->calHash();
		ex->type = OBJ_EXPIRE;
		std::unique_lock <std::mutex> lck(slaveMutex);
		Timer *timer = loop.runAfter(ttl / 1000,false,std::bind(&Redis::setExpireTimeOut,this,ex));
		auto it = expireTimers.find(ex);
		assert(it == expireTimers.end());
		expireTimers.insert(std::make_pair(ex,timer));	
	}

	addReply(session->getClientBuffer(),shared.ok);
	return false;
}

bool Redis::existsCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() > 0)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown exists error");
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
			addReplyLongLong(session->getClientBuffer(),0);
		}
		else
		{
			addReplyLongLong(session->getClientBuffer(),1);
		}
	}
	return true;
}


bool Redis::saddCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() < 2)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown sadd error");
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
			
			std::unordered_set<RedisObject*,Hash,Equal> set;
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
					decrRefCount(obj[i]);
				}
				setMap.insert(std::make_pair(obj[0],std::move(set)));
				map.insert(obj[0]);
			}
		}
		else
		{
			if ((*it)->type != OBJ_SET)
			{
				addReplyErrorFormat(session->getClientBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = setMap.find(obj[0]);
			assert(iter != setMap.end());
			assert(iter->first->type == (*it)->type);
			decrRefCount(obj[0]);

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
					decrRefCount(obj[i]);
				}
			}
		}
	}
		
	addReplyLongLong(session->getClientBuffer(),len);
	return true;
}	

bool Redis::zrangeGenericCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session,int reverse)
{
	if (obj.size()  != 4)
	{
		addReplyError(session->getClientBuffer(),"unknown zrange param error");
		return false;
	}

	int rangelen;
	int withscores = 0;
	int64_t start;
	
	if (getLongLongFromObjectOrReply(session->getClientBuffer(),obj[1],&start,nullptr) != REDIS_OK)
	{
		addReplyError(session->getClientBuffer(),"unknown double param error");
		return false;
	}

	int64_t end;

	if (getLongLongFromObjectOrReply(session->getClientBuffer(),obj[2],&end,nullptr) != REDIS_OK)
	{
		addReplyError(session->getClientBuffer(),"unknown double param error");
		return false;
	}

	if ( !strcasecmp(obj[3]->ptr,"withscores"))
	{
		withscores = 1;
	}
	else if (obj.size() >= 5)
	{
		addReply(session->getClientBuffer(),shared.syntaxerr);
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
			addReply(session->getClientBuffer(),shared.emptymultibulk);
			return false;
		}
		else
		{
			if ((*it)->type != OBJ_ZSET)
			{
				addReplyErrorFormat(session->getClientBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = zsetMap.find(obj[0]);
			assert(iter != zsetMap.end());
			assert(iter->second.second.size() == iter->second.first.size());
			assert(iter->first->type == (*it)->type);

			size_t llen = iter->second.second.size();

			if (start < 0) start = llen+start;
			if (end < 0) end = llen+end;
			if (start < 0) start = 0;

			if (start > end || start >= llen) 
			{
				addReply(session->getClientBuffer(),shared.emptymultibulk);
				return false;
			}

			if (end >= llen) 
			{
				end = llen-1;	
			}

			rangelen = (end-start) + 1;	
			addReplyMultiBulkLen(session->getClientBuffer(),withscores ? (rangelen*2) : rangelen);

			if (reverse)
			{
				int count = 0;
				for (auto iterr = iter->second.second.rbegin(); iterr != iter->second.second.rend(); ++iterr)
				{
					if (count++ >= start)
					{
						addReplyBulkCBuffer(session->getClientBuffer(),
							iterr->second->ptr,sdslen(iterr->second->ptr));
						if (withscores)
						{
							addReplyDouble(session->getClientBuffer(),iterr->first);
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
				for (auto iterr = iter->second.second.begin(); iterr != iter->second.second.end(); ++iterr)
				{
					if (count++ >= start)
					{
						addReplyBulkCBuffer(session->getClientBuffer(),
							iterr->second->ptr, sdslen(iterr->second->ptr));
						if (withscores)
						{
							addReplyDouble(session->getClientBuffer(),iterr->first);
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


bool Redis::hgetallCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown hgetall error");
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
			addReply(session->getClientBuffer(),shared.emptymultibulk);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->getClientBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			addReplyMultiBulkLen(session->getClientBuffer(),iter->second.size() * 2);
			for (auto &iterr : iter->second)
			{
				addReplyBulkCBuffer(session->getClientBuffer(),iterr.first->ptr,sdslen(iterr.first->ptr));
				addReplyBulkCBuffer(session->getClientBuffer(),iterr.second->ptr,sdslen(iterr.second->ptr));
			}
		}
	}
	return false;
}

bool Redis::hgetCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 2)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown hget param error");
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
			addReply(session->getClientBuffer(),shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->getClientBuffer(),"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			auto iterr = iter->second.find(obj[1]);
			if (iterr == iter->second.end())
			{
				addReply(session->getClientBuffer(),shared.nullbulk);
			}
			else
			{
				addReplyBulk(session->getClientBuffer(),iterr->second);
			}
		}
	}
	return false;
}

bool Redis::hkeysCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown hkeys error");
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
			addReply(session->getClientBuffer(),shared.emptymultibulk);
			return false;
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->getClientBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			addReplyMultiBulkLen(session->getClientBuffer(),iter->second.size());

			for (auto &iterr : iter->second)
			{
				addReplyBulkCBuffer(session->getClientBuffer(),iterr.first->ptr,sdslen(iterr.first->ptr));
			}
		}
	}	
	return false;
}

bool Redis::hlenCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown hlen error");
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
				addReplyErrorFormat(session->getClientBuffer(),
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

	addReplyLongLong(session->getClientBuffer(),len);
	return false;
}

bool Redis::hsetCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 3)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown hset error");
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
			std::unordered_map<RedisObject*,RedisObject*,Hash,Equal> rhash;
			rhash.insert(std::make_pair(obj[1],obj[2]));
			hashMap.insert(std::make_pair(obj[0],std::move(rhash)));
			map.insert(obj[0]);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(session->getClientBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			
			decrRefCount(obj[0]);

			auto iterr = iter->second.find(obj[1]);
			if (iterr == iter->second.end())
			{
				iter->second.insert(std::make_pair(obj[1],obj[2]));
			}
			else
			{
				decrRefCount(obj[1]);
				decrRefCount(iterr->second);
				iterr->second = obj[2];
				update = true;
			}
		}
	}

	addReply(session->getClientBuffer(),update ? shared.czero : shared.cone);
	return true;
}

bool Redis::setCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{	
	if (obj.size() <  2 || obj.size() > 8 )
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown set param error");
		return false;
	}

	int32_t j;
	RedisObject *expire = nullptr;
	RedisObject *ex = nullptr;
	int32_t unit = UNIT_SECONDS;
	int32_t flags = OBJ_SET_NO_FLAGS;

	for (j = 2; j < obj.size(); j++)
	{
		const char *a = obj[j]->ptr;
		RedisObject *next = (j == obj.size() - 1) ? nullptr : obj[j + 1];

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
			addReply(session->getClientBuffer(),shared.syntaxerr);
			return false;
		}
	}
	
	int64_t milliseconds = 0;
	if (expire)
	{
		if (getLongLongFromObjectOrReply(session->getClientBuffer(),
				expire,&milliseconds,nullptr) != REDIS_OK)
		{
			return false;
		}
		if (milliseconds <= 0)
		{
		    addReplyErrorFormat(session->getClientBuffer(),"invalid expire time in");
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
				addReply(session->getClientBuffer(),shared.nullbulk);
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
				addReplyErrorFormat(session->getClientBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}
			
			if (flags & OBJ_SET_NX)
			{
				addReply(session->getClientBuffer(),shared.nullbulk);
				return false;
			}

			if (expire)
			{
				ex = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}

			auto iter = stringMap.find(obj[0]);
			assert(iter != stringMap.end());

			decrRefCount(obj[0]);
			decrRefCount(iter->second);
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
				decrRefCount(iter->first);
				loop.cancelAfter(iter->second);
				expireTimers.erase(iter);
			}

			setExpire(ex,milliseconds / 1000);
		}

		for (int32_t i = 2; i < obj.size(); i++)
		{
			decrRefCount(obj[i]);
		}
	}

	addReply(session->getClientBuffer(),shared.ok);
	return true;
}

bool Redis::getCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{	
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown get param error");
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
			addReply(session->getClientBuffer(),shared.nullbulk);
			return false;
		}

		auto iter = stringMap.find(obj[0]);
		assert(iter != stringMap.end());
		assert(iter->first->type == (*it)->type);

		if ((*it)->type != OBJ_STRING)
		{
			addReplyErrorFormat(session->getClientBuffer(),
				"WRONGTYPE Operation against a key holding the wrong kind of value");
			return false;
		}

		addReplyBulk(session->getClientBuffer(),iter->second);
	}
	return false;
}

bool Redis::incrCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown incr param error");
		return false;
	}
	return incrDecrCommand(obj[0],session,1);
}

bool Redis::decrCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown decr param error");
		return false;
	}
	return incrDecrCommand(obj[0],session,-1);
}

bool Redis::incrDecrCommand(RedisObject *obj,const SessionPtr &session,int64_t incr)
{
	size_t hash= obj->hash;
	size_t index = hash % kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &stringMap = redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(obj);
		if (it == map.end())
		{
			auto iter = stringMap.find(obj);
			assert(iter == stringMap.end());

			obj->type = OBJ_STRING;
			map.insert(obj);

			stringMap.insert(std::make_pair(obj,createStringObjectFromLongLong(incr)));
			addReplyLongLong(session->getClientBuffer(),incr);
			return true;
		}
		else
		{
			if ((*it)->type != OBJ_STRING)
			{
				addReplyErrorFormat(session->getClientBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return false;
			}

			auto iter = stringMap.find(obj);
			assert(iter != stringMap.end());

			int64_t value;
			if (getLongLongFromObjectOrReply(session->getClientBuffer(),
					iter->second,&value,nullptr) != REDIS_OK)
				return false;

			value += incr;
			if ((incr < 0 && value < 0 && incr < (LLONG_MIN-value)) ||
				(incr > 0 && value > 0 && incr > (LLONG_MAX-value)))
			{
				addReplyError(session->getClientBuffer(),"increment or decrement would overflow");
				return false;
			}

			decrRefCount(iter->second);
			iter->second = createStringObjectFromLongLong(value);
			addReply(session->getClientBuffer(),shared.colon);
			addReply(session->getClientBuffer(),iter->second);
			addReply(session->getClientBuffer(),shared.crlf);
			return false;
		}
	}
}

bool Redis::ttlCommand(const std::deque<RedisObject*> &obj,const SessionPtr &session)
{
	if (obj.size() != 1)
	{
		addReplyErrorFormat(session->getClientBuffer(),"unknown ttl param error");
		return false;
	}

	std::unique_lock <std::mutex> lck(expireMutex);
	auto it = expireTimers.find(obj[0]);
	if (it == expireTimers.end())
	{
		addReplyLongLong(session->getClientBuffer(),-2);
		return false;
	}
	
	int64_t ttl = it->second->getExpiration().getMicroSecondsSinceEpoch()
	 - TimeStamp::now().getMicroSecondsSinceEpoch();
	
	addReplyLongLong(session->getClientBuffer(),ttl / 1000000);
	return false;
}

void Redis::flush()
{

}

bool Redis::checkCommand(RedisObject *cmd)
{
	auto it = checkCommands.find(cmd);
	if (it == checkCommands.end())
	{
		return false;
	}
	return true;
}

void Redis::initConfig()
{
	LOG_INFO<<"Server initialized";
	createSharedObjects();
	char buf[32];
	int32_t len = ll2string(buf,sizeof(buf),getPort());
	shared.rPort = createStringObject(buf,len);
	shared.rIp = createStringObject(getIp().data(),getIp().length());

#define REGISTER_REDIS_COMMAND(msgId,func) \
	msgId->calHash(); \
	handlerCommands[msgId] = std::bind(&Redis::func,this,std::placeholders::_1,std::placeholders::_2);
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
	REGISTER_REDIS_COMMAND(shared.incr,incrCommand);
	REGISTER_REDIS_COMMAND(shared.decr,decrCommand);
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
	REGISTER_REDIS_COMMAND(shared.INCR,incrCommand);
	REGISTER_REDIS_COMMAND(shared.DECR,decrCommand);

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
	checkCommands.insert(msgId);
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






