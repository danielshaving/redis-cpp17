#include "redis.h"

Redis::Redis(const char *ip,int16_t port,int16_t threadCount,bool enbaledCluster)
:server(&loop,ip,port,nullptr),
ip(ip),
port(port),
clusterEnabled(enbaledCluster),
repli(this),
clus(this),
rdb(this)
{
	initConfig();
	loadDataFromDisk();
	server.setConnectionCallback(std::bind(&Redis::connCallBack,this,std::placeholders::_1));
	server.setThreadNum(threadCount);
	if (threadCount > 1)
	{
		this->threadCount = threadCount;
	}

	server.start();
	//loop.runAfter(1.0,true,std::bind(&Redis::serverCron,this));
	//loop.runAfter(400.0,true,std::bind(&Redis::bgsaveCron,this));

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
#ifndef _WIN32
	rdbSaveBackground();
#endif
}

void Redis::serverCron()
{
#ifndef _WIN32
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
					LOG_INFO <<"Background saving terminated with success";
					if (slavefd != -1)
					{
						std::unique_lock <std::mutex> lck(slaveMutex);
						auto it = slaveConns.find(slavefd);
						if (it == slaveConns.end())
						{
							LOG_WARN<<"Master sync send failure";
						}
						else
						{
							if (!rdb.rdbReplication("dump.rdb",it->second))
							{
								it->second->forceClose();
								LOG_WARN<<"Master sync send failure";
							}
							else
							{
								 LOG_INFO <<"Master sync send success ";
							}
						}

						slavefd = -1;
					}
				}
				else if (!bysignal && exitcode != 0)
				{
					 LOG_INFO <<"Background saving error";
				}
				else
				{
					LOG_WARN<<"Background saving terminated by signal "<< bysignal;
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
#endif
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

void Redis::clearCommand(std::deque<RedisObjectPtr> &commands)
{
	commands.clear();
}

void Redis::clearPubSubState(int32_t sockfd)
{
	{
		std::unique_lock <std::mutex> lck(pubsubMutex);
		for (auto &it : pubSubs)
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

void Redis::setExpire(const RedisObjectPtr &key,double when)
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

void Redis::clearMonitorState(int32_t sockfd)
{
	std::unique_lock <std::mutex> lck(monitorMutex);
	monitorConns.erase(sockfd);
	if (monitorConns.empty())
	{
		monitorEnabled = false;
	}
}

void Redis::clearSessionState(int32_t sockfd)
{
	std::unique_lock <std::mutex> lck(mtx);
	auto it = sessions.find(sockfd);
	assert(it != sessions.end());
	sessions.erase(it);

	auto iter = sessionConns.find(sockfd);
	assert(iter != sessionConns.end());
	sessionConns.erase(iter);
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

void Redis::setExpireTimeOut(const RedisObjectPtr &expire)
{
	removeCommand(expire);
}

void Redis::connCallBack(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		//Socket::setTcpNoDelay(conn->getSockfd(),true);

		char buf[64] = "";
		uint16_t port = 0;
		auto addr = Socket::getPeerAddr(conn->getSockfd());
		Socket::toIp(buf,sizeof(buf),(const struct sockaddr *)&addr);
		Socket::toPort(&port,(const struct sockaddr *)&addr);
		conn->setip(buf);
		conn->setport(port);

		//printf("%s %d\n",buf,port);

		SessionPtr session (new Session(this,conn));
		std::unique_lock <std::mutex> lck(mtx);
		auto it = sessions.find(conn->getSockfd());
		assert(it == sessions.end());
		sessions[conn->getSockfd()] = session;
		sessionConns[conn->getSockfd()] = conn;
		LOG_INFO <<"Client connect success";
	}
	else
	{
		clearRepliState(conn->getSockfd());
		clearClusterState(conn->getSockfd());
		clearMonitorState(conn->getSockfd());
		clearSessionState(conn->getSockfd());

		 LOG_INFO <<"Client disconnect";
	}
}

void Redis::feedMonitor(const std::deque<RedisObjectPtr> &obj,int32_t sockfd)
{
	char buf[64] = "";
	auto addr = Socket::getPeerAddr(sockfd);
	Socket::toIpPort(buf,sizeof(buf),(const struct sockaddr *)&addr);

	int j = 0;
	sds cmdrepr = sdsnew("+");
	RedisObjectPtr cmdobj;
#ifdef _WIN32
	cmdrepr = sdscatprintf(cmdrepr, "%ld.%06ld ", time(0), 0);
#else
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	cmdrepr = sdscatprintf(cmdrepr, "%ld.%06ld ", (long)tv.tv_sec, (long)tv.tv_usec);
#endif
	cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",0,buf);

	for(auto &it : obj)
	{
		j++;
		if (it->encoding == OBJ_ENCODING_INT)
		{
			cmdrepr = sdscatprintf(cmdrepr,"\"%ld\"",(long)it->ptr);
		}
		else
		{
			cmdrepr = sdscatrepr(cmdrepr,(char*)it->ptr,
						sdslen(it->ptr));
		}
		if (j != obj.size() - 1)
		{
			cmdrepr = sdscatlen(cmdrepr," ",1);
		}
	}

	cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
	cmdobj = createObject(OBJ_STRING,cmdrepr);

	std::unique_lock <std::mutex> lck(monitorMutex);
	for (auto &it : monitorConns)
	{
		it.second->sendPipe(cmdobj->ptr,sdslen(cmdobj->ptr));
	}
}

void Redis::loadDataFromDisk()
{
	int64_t start = ustime();
	if (rdb.rdbLoad("dump.rdb") == REDIS_OK)
	{
		int64_t end = ustime();
		double diff = double(end - end) / (1000 * 1000);
		LOG_INFO <<"DB loaded from disk seconds: "<< double(end - start) / 1000;
	}
	else if (errno != ENOENT)
	{
       	LOG_WARN <<"Fatal error loading the DB: Exiting."<<strerror(errno);
 	}
}

bool Redis::subscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() < 1)
	{
		return false;
	}

	bool retval;
	int sub = 0;
	for (int i = 0; i < obj.size(); i++)
	{
		{
			std::unique_lock <std::mutex> lck(pubsubMutex);
			auto it = pubSubs.find(obj[i]);
			if (it == pubSubs.end())
			{
				std::unordered_map<int32_t,TcpConnectionPtr> maps;
				maps[conn->getSockfd()] = conn;
				pubSubs[obj[i]] = std::move(maps);
				retval = true;
				sub++;
			}
			else
			{
				retval = false;
				auto iter = it->second.find(conn->getSockfd());
				if (iter == it->second.end())
				{
					sub++;
				}
				it->second[conn->getSockfd()] = conn;
			}
		}

		addReply(conn->outputBuffer(),shared.mbulkhdr[3]);
		addReply(conn->outputBuffer(),shared.subscribebulk);
		addReplyBulk(conn->outputBuffer(),obj[i]);
		addReplyLongLong(conn->outputBuffer(),sub);
	}
	return true;
}

bool Redis::unsubscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return false;
}

bool Redis::psubscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return false;
}

bool Redis::punsubscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return false;
}

bool Redis::publishCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return false;
}

bool Redis::pubsubCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return false;
}

bool Redis::sentinelCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return false;
}

bool Redis::memoryCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size()  > 2)
	{
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
			addReply(conn->outputBuffer(), ok);
			return  true;
		}
	}

#endif

	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

bool Redis::infoCommand(const std::deque<RedisObjectPtr> &obj,
	const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size()  < 0)
	{
		return false;
	}

#ifndef _WIN32
	struct rusage self_ru, c_ru;
	getrusage(RUSAGE_SELF, &self_ru);
	getrusage(RUSAGE_CHILDREN, &c_ru);
	sds info = sdsempty();

	char hmem[64];
    size_t zmallocUsed = zmalloc_used_memory();

    bytesToHuman(hmem,zmallocUsed);

    info = sdscat(info,"\r\n");
    info = sdscatprintf(info,
        "# Memory\r\n"
        "used_memory:%zu\r\n"
        "used_memory_human:%s\r\n"
        "mem_allocator:%s\r\n",
        zmallocUsed,
        hmem,
        ZMALLOC_LIB);


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
	
	addReplyBulkSds(conn->outputBuffer(),info);
#endif
	return true;
}

bool Redis::clientCommand(const std::deque<RedisObjectPtr> &obj,
	const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 1)
	{
		return false;
	}

	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

bool Redis::echoCommand(const std::deque<RedisObjectPtr> &obj,
	const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 1)
	{
		return false;
	}

	addReplyBulk(conn->outputBuffer(),obj[0]);
	return true;
}

bool Redis::authCommand(const std::deque<RedisObjectPtr> &obj,
	const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 1)
	{
		return false;
	}

	if (password.c_str() == nullptr)
	{
		addReplyError(conn->outputBuffer(),"client sent auth, but no password is set");
		return true;
	}

	if (!strcmp(obj[0]->ptr,password.c_str()))
	{
		session->setAuth(true);
		addReply(conn->outputBuffer(),shared.ok);
	}
	else
	{
		addReplyError(conn->outputBuffer(),"invalid password");
	}
	return true;
}

bool Redis::configCommand(const std::deque<RedisObjectPtr> &obj,
	const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 3 || obj.size() == 0)
	{
		return false;
	}

	if (!strcmp(obj[0]->ptr,"set"))
	{
		if (obj.size() != 3)
		{
			addReplyErrorFormat(conn->outputBuffer(),
				"Wrong number of arguments for CONFIG %s",(char*)obj[0]->ptr);
			return true;
		}

		if (!strcmp(obj[1]->ptr,"requirepass"))
		{
			password = obj[2]->ptr;
			authEnabled = true;
			session->setAuth(false);
			addReply(conn->outputBuffer(),shared.ok);
		}
		else
		{
			addReplyErrorFormat(conn->outputBuffer(),
					"Invalid argument for CONFIG SET '%s'",
				(char*)obj[1]->ptr);
		}

	}
	else
	{
		addReplyError(conn->outputBuffer(),
				"config subcommand must be one of GET, SET, RESETSTAT, REWRITE");
	}
	return true;
}

bool Redis::migrateCommand(const std::deque<RedisObjectPtr> &obj,
	const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() < 5)
	{
		return false;
	}

	if (!clusterEnabled)
	{
		addReplyError(conn->outputBuffer(),"this instance has cluster support disabled");
		return true;
	}

	int32_t firstKey = 3; /* Argument index of the first key. */
	int32_t numKeys = 1;  /* By default only migrate the 'key' argument. */

	int8_t copy = 0,replace = 0;
	char *password = nullptr;

	for (int i = 5; i < obj.size(); i++)
	{
		int moreargs = i < obj.size() - 1;
		if (!strcmp(obj[i]->ptr,"copy"))
		{
			copy = 1;
		}
		else if (!strcmp(obj[i]->ptr,"replace"))
		{
			replace = 1;
		}
		else if (!strcmp(obj[i]->ptr,"auth"))
		{
			if (!moreargs) 
			{
				addReply(conn->outputBuffer(),shared.syntaxerr);
				return true;
			}
			i++;
			password = obj[i]->ptr;
		}
		else if (!strcmp(obj[i]->ptr,"keys"))
		{
			if (sdslen(obj[2]->ptr) != 0)
			{
				addReplyError(conn->outputBuffer(),
					"When using MIGRATE KEYS option, the key argument"
					" must be set to the empty string");
				return true;
			}
			firstKey = i + 1;
			numKeys = obj.size() - i - 1;
			break;
		}
		else
		{
			addReply(conn->outputBuffer(),shared.syntaxerr);
			return true;
		}
	}

	int64_t port;
	int32_t timeout;
	int32_t dbid;
	/* Sanity check */
	if (getLongFromObjectOrReply(conn->outputBuffer(),
		obj[4],&timeout,nullptr) != REDIS_OK ||
		getLongFromObjectOrReply(conn->outputBuffer(),
			obj[3],&dbid,nullptr) != REDIS_OK)
	{
		return true;
	}

	if (timeout <= 0) timeout = 1000;

	if (getLongLongFromObject(obj[1], &port) != REDIS_OK)
	{
		addReplyErrorFormat(conn->outputBuffer(),"Invalid TCP port specified: %s",
			(char*)obj[2]->ptr);
		return true;
	}

	std::string ip = obj[0]->ptr;
	if (this->ip == ip && this->port == port)
	{
		addReplyErrorFormat(conn->outputBuffer(),"migrate self server error ");
		return true;
	}
	
	clus.replicationToNode(obj,session,ip,port,copy,replace,numKeys,firstKey);
	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

bool Redis::clusterCommand(const std::deque<RedisObjectPtr> &obj,
	const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (!clusterEnabled)
	{
		return false;
	}

	if (!strcmp(obj[0]->ptr,"meet"))
	{
		if (obj.size() != 3)
		{
			return false;
		}

		int64_t port;

		if (getLongLongFromObject(obj[2],&port) != REDIS_OK)
		{
			addReplyErrorFormat(conn->outputBuffer(),
					"Invalid TCP port specified: %s",(char*)obj[2]->ptr);
			return true;
		}

		if (ip.c_str() && !memcmp(ip.c_str(),obj[1]->ptr,
			sdslen(obj[1]->ptr)) && this->port == port)
		{
			LOG_WARN << "cluster meet connect self error .";
			addReplyErrorFormat(conn->outputBuffer(),"Don't connect self ");
			return true;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto &it : clusterConns)
			{
				if (port == it.second->getport() && 
					!memcmp(it.second->getip(),obj[1]->ptr,sdslen(obj[1]->ptr)))
				{
					LOG_WARN << "cluster meet already exists .";
					addReplyErrorFormat(conn->outputBuffer(),"cluster meet already exists ");
					return true;
				}
			}
		}
		
		if (!clus.connSetCluster(obj[1]->ptr,port))
		{
			addReplyErrorFormat(conn->outputBuffer(),
					"Invaild node address specified: %s:%s",
					(char*)obj[1]->ptr,(char*)obj[2]->ptr);
			return true;
		}
	}
	else if (!strcmp(obj[0]->ptr,"connect") && obj.size() == 3)
	{
		int64_t  port;
	
		if (getLongLongFromObject(obj[2],&port) != REDIS_OK)
		{
			addReplyError(conn->outputBuffer(),"Invalid or out of range port");
			return true;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			for (auto &it : clusterConns)
			{
				if (port == it.second->getport() && 
					!memcmp(it.second->getip(),obj[1]->ptr,sdslen(obj[1]->ptr)))
				{
					return true;
				}
			}
		}
	
		clus.connSetCluster(obj[1]->ptr,port);
		return true;
	}
	else if (!strcmp(obj[0]->ptr,"info") && obj.size() == 1)
	{
		return false;
	}
	else if (!strcmp(obj[0]->ptr,"flushslots") && obj.size() == 1)
	{
		return false;
	}
	else if (!strcmp(obj[0]->ptr,"saveconfig") && obj.size() == 1)
	{
		return false;
	}
	else if (!strcmp(obj[0]->ptr,"countkeysinslot") && obj.size() == 2)
	{
		return false;
	}
	else if (!strcmp(obj[0]->ptr,"forget") && obj.size() == 2)
	{
		return false;
	}
	else if (!strcmp(obj[0]->ptr,"slaves") && obj.size() == 2)
	{
		return false;
	}
	else if (!strcmp(obj[0]->ptr,"nodes") && obj.size() == 1)
	{
		RedisObjectPtr o = createObject(OBJ_STRING,clus.showClusterNodes());
		addReplyBulk(conn->outputBuffer(),o);
		return true;
	}
	else if (!strcmp(obj[0]->ptr,"getkeysinslot") && obj.size() == 3)
	{
		int64_t maxkeys = 0, slot = 0;
		uint32_t numkeys = 0, j = 0;

		if (getLongLongFromObjectOrReply(conn->outputBuffer(),
			obj[1],&slot,nullptr) != REDIS_OK)
			return true;
			
		if (getLongLongFromObjectOrReply(conn->outputBuffer(),
			obj[2],&maxkeys,nullptr) != REDIS_OK)
			return true;
	
		if (slot < 0 || slot >= 16384 || maxkeys < 0) 
		{
			addReplyError(conn->outputBuffer(),"Invalid slot or number of keys");
			return true;
		}
		
		std::vector<RedisObjectPtr> keys;
		clus.getKeyInSlot(slot,keys,maxkeys);
		addReplyMultiBulkLen(conn->outputBuffer(),numkeys);
		
		for (auto &it : keys)
		{
			addReplyBulk(conn->outputBuffer(),it);
		}
		return true;
	}
	else if (!strcmp(obj[0]->ptr,"slots") && obj.size() == 1)
	{

	}
	else if (!strcmp(obj[0]->ptr,"keyslot") && obj.size() == 2)
	{
		char *key = obj[1]->ptr;
		addReplyLongLong(conn->outputBuffer(),
			clus.keyHashSlot((char*)key,sdslen(key)));
		return true;
	}
	else if (!strcmp(obj[0]->ptr,"setslot") && obj.size() >= 3)
	{		
		if (!!strcmp(obj[1]->ptr,"stable") )
		{
			
		}
		else if ( !strcmp(obj[1]->ptr,"node") )
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
				addReply(conn->outputBuffer(),shared.err);
				return true;
			}
				
			for (int32_t i  = 4; i < obj.size(); i++)
			{
				int32_t slot;
				if ((slot = clus.getSlotOrReply(session,obj[i],conn)) == -1)
				{
					addReplyErrorFormat(conn->outputBuffer(),"Invalid slot %s",obj[i]->ptr);
					return true;
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
		
			addReply(conn->outputBuffer(),shared.ok);
			LOG_INFO <<"cluster async replication success "<<imipPort;
			return true;
		}

		int32_t slot;
		if ((slot = clus.getSlotOrReply(session,obj[1],conn)) == -1)
		{
			addReplyErrorFormat(conn->outputBuffer(),"Invalid slot %d",(char*)obj[1]->ptr);
			return true;
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
						addReplyErrorFormat(conn->outputBuffer(),
								"setslot migrate slot error %d",slot);
						return true;
					}

					mark = true;
					break;
				}
			}
		}

		if (!mark)
		{
			addReplyErrorFormat(conn->outputBuffer(),"setslot slot node no found error ");
			return true;
		}
		
		if ( !strcmp(obj[2]->ptr,"importing") && obj.size() == 4)
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
					addReplyErrorFormat(conn->outputBuffer(),"repeat importing slot :%d",slot);
					return true;
				}
			}
			clusterRepliImportEnabeld = true;
		}
		else if (!strcmp(obj[2]->ptr,"migrating") && obj.size() == 4)
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
					addReplyErrorFormat(conn->outputBuffer(),"repeat migrating slot :%d",slot);
					return true;
				}
			}
		}
		else
		{
			addReplyErrorFormat(conn->outputBuffer(),"Invalid param ");
			return true;
		}
	}
	else if (!strcmp(obj[0]->ptr,"delsync"))
	{
		int32_t slot;
		if ((slot = clus.getSlotOrReply(session,obj[1],conn)) == 0)
		{
			LOG_INFO <<"getSlotOrReply error ";
			return true;
		}

		{
			std::unique_lock <std::mutex> lck(clusterMutex);
			clus.eraseClusterNode(slot);
		}

		LOG_INFO << "delsync success:" << slot;
		return true;

	}
	else if (!strcmp(obj[0]->ptr,"addsync") && obj.size() == 5)
	{
		int32_t slot;
		int64_t  port;
		if ((slot = clus.getSlotOrReply(session,obj[1],conn)) == 0)
		{
			LOG_INFO <<"getSlotOrReply error ";
			return true;
		}

		if (getLongLongFromObject(obj[3],&port) != REDIS_OK)
		{
			addReplyError(conn->outputBuffer(),"Invalid or out of range port");
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
			addReplyErrorFormat(conn->outputBuffer(),"cluster insert error:%d",slot);
			LOG_INFO << "cluster insert error "<<slot;
		}
		return true;
	}
	else if (!strcmp(obj[0]->ptr,"delslots") && obj.size() == 2)
	{		
		std::unique_lock <std::mutex> lck(clusterMutex);
		if (clusterConns.size() == 0)
		{
			addReplyErrorFormat(conn->outputBuffer(),"execute cluster meet ip:port");
			return true;
		}

		int32_t slot;
		int32_t j;
		for (j = 1; j < obj.size(); j++)
		{
			if ((slot = clus.getSlotOrReply(session,obj[j],conn)) == 0)
			{
				return true;
			}

			if (slot < 0 || slot > 16384 )
			{
				addReplyErrorFormat(conn->outputBuffer(),"cluster delslots range error %d:",slot);
				return true;
			}

			if (clus.checkClusterSlot(slot) == nullptr)
			{
				clus.delSlotDeques(obj[j],slot);
				clus.syncClusterSlot();
				LOG_INFO << "deslots success " << slot;
			}
			else
			{
				addReplyErrorFormat(conn->outputBuffer(),"not found deslots error %d:",slot);
				LOG_INFO << "not found deslots " << slot;
			}
		}

	}
	else if (!strcmp(obj[0]->ptr, "addslots") && obj.size() == 2)
	{
		int32_t  j = 0,slot = 0;
		for (j = 1; j < obj.size(); j++)
		{
			if (slot < 0 || slot > 16384 )
			{
				addReplyErrorFormat(conn->outputBuffer(),"cluster delslots range error %d:",slot);
				return false;
			}

			if ((slot = clus.getSlotOrReply(session,obj[j],conn)) == 0)
			{
				return false;
			}

			std::unique_lock <std::mutex> lck(clusterMutex);
			if (clusterConns.empty())
			{
				addReplyErrorFormat(conn->outputBuffer(),"execute cluster meet ip:port");
				return true;
			}

			if (clus.checkClusterSlot(slot) == nullptr)
			{
				char name[CLUSTER_NAMELEN];
				getRandomHexChars(name,CLUSTER_NAMELEN);
				clus.cretateClusterNode(slot,this->ip,this->port,name);
				clus.addSlotDeques(obj[j],name);
				clus.syncClusterSlot();
				LOG_INFO <<"addslots success "<< slot;
			}
			else
			{
				addReplyErrorFormat(conn->outputBuffer(),"Slot %d specified multiple times",slot);
				return true;
			}
		}
	}
	else
	{
		addReplyErrorFormat(conn->outputBuffer(),"unknown param error");
		return true;
	}
	
	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

void Redis::structureRedisProtocol(Buffer &buffer,std::deque<RedisObjectPtr> &robjs)
{
	int32_t len,j;
	char buf[32];
	buf[0] = '*';
	len = 1 + ll2string(buf + 1, sizeof(buf) - 1,robjs.size());
	buf[len++] = '\r';
	buf[len++] = '\n';
	buffer.append(buf,len);

	for (int32_t i = 0; i < robjs.size(); i++)
	{
		buf[0] = '$';
		len = 1 + ll2string(buf + 1, sizeof(buf) - 1,sdslen(robjs[i]->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		buffer.append(buf, len);
		buffer.append(robjs[i]->ptr,sdslen(robjs[i]->ptr));
		buffer.append("\r\n", 2);
	}
}

bool Redis::getClusterMap(const RedisObjectPtr &command)
{
	auto it = cluterCommands.find(command);
	if (it == cluterCommands.end())
	{
		return false;
	}
	return true;
}

#ifndef _WIN32
bool Redis::bgsave(const SessionPtr &session,const TcpConnectionPtr &conn,bool enabled)
{
	if (rdbChildPid != -1)
	{
		if (!enabled)
		{
			addReplyError(conn->outputBuffer(),"Background save already in progress");
		}
		return false;
	}

	if (rdbSaveBackground(enabled) == REDIS_OK)
	{
		if (!enabled)
		{
			addReplyStatus(conn->outputBuffer(),"Background saving started");
		}
	}
	else
	{
		if (!enabled)
		{
			addReply(conn->outputBuffer(),shared.err);
		}
		return false;
	}
	return true;
}
#endif

bool Redis::save(const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (getDbsize() == 0)
	{
		return true;
	}

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
	for (auto &it : sessionConns)
	{
		it.second->forceClose();
	}

	sessionConns.clear();
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
#ifndef _WIN32
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
			 size_t privateDirty = zmalloc_get_private_dirty(getpid());
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

#endif

bool Redis::bgsaveCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 0)
	{
		return false;
	}
	
#ifndef _WIN32
	bgsave(session,conn);
#endif
	return true;
}

bool Redis::saveCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 0)
	{
		return false;
	}

	if (rdbChildPid != -1)
	{
		addReplyError(conn->outputBuffer(),"Background save already in progress");
		return true;
	}

	if (save(session,conn))
	{
		addReply(conn->outputBuffer(),shared.ok);
	}
	else
	{
		addReply(conn->outputBuffer(),shared.err);
	}
	return true;
}

bool Redis::slaveofCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 2)
	{
		return false;
	}

	if (!strcmp(obj[0]->ptr,"no") &&!strcmp(obj[1]->ptr,"one")) 
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
		if ((getLongFromObjectOrReply(conn->outputBuffer(),obj[1],&port,nullptr) != REDIS_OK))
			return false;

		if (ip.c_str() && !memcmp(ip.c_str(),obj[0]->ptr,sdslen(obj[0]->ptr))
				&& this->port == port)
		{
			LOG_WARN<<"slave of connect self error .";
			addReplySds(conn->outputBuffer(),sdsnew("don't connect master self \r\n"));
			return true;
		}

		if (masterPort > 0)
		{
			LOG_WARN<<"slave of would result into synchronization with ";
			LOG_WARN<<"the master we are already connected with. no operation performed.";
			addReplySds(conn->outputBuffer(),sdsnew("+ok already connected to specified master\r\n"));
			return true;
		}	

		repli.replicationSetMaster(obj[0],port);
		LOG_INFO <<"slave of "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
	}
	
	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

bool Redis::commandCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{	
	addReply(conn->outputBuffer(),shared.ok);
	return true;
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

bool Redis::lpushCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() < 2)
	{
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
			std::deque<RedisObjectPtr> list;
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
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert(iter->first->type == (*it)->type);

			for (int32_t i = 1; i < obj.size(); i++)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				iter->second.push_back(obj[i]);
			}
		}
	}

	addReplyLongLong(conn->outputBuffer(),pushed);
	return true;
}

bool Redis::lpopCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
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
			addReply(conn->outputBuffer(),shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert((*it)->type == iter->first->type);
			addReplyBulk(conn->outputBuffer(),iter->second.back());
			iter->second.pop_back();
			if (iter->second.empty())
			{
				listMap.erase(iter );
				map.erase(it);
			}
		}
	}
	return true;
}

bool Redis::lrangeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 3)
	{
		return false;
	}

	int32_t  start;
	int32_t end;
	if ((getLongFromObjectOrReply(conn->outputBuffer(),obj[1],&start,nullptr) != REDIS_OK) ||
		(getLongFromObjectOrReply(conn->outputBuffer(),obj[2],&end,nullptr) != REDIS_OK))
	{
		return true;
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

			addReply(conn->outputBuffer(),shared.nullbulk);
			return true;
		}

		if ((*it)->type != OBJ_LIST)
		{
			addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
			return true;
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
			addReply(conn->outputBuffer(),shared.emptymultibulk);
			return true;
		}

		if (end >= size)
		{
			end = size - 1;
		}

		size_t rangelen =  (end - start) + 1;
		addReplyMultiBulkLen(conn->outputBuffer(),rangelen);

		while(rangelen--)
		{
			addReplyBulkCBuffer(conn->outputBuffer(),
					iter->second[start]->ptr,sdslen(iter->second[start]->ptr));
			start++;
		}
	}
	return true;
}

bool Redis::rpushCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size()  <  2)
	{
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
			std::deque<RedisObjectPtr> list;
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
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}

			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert(iter->first->type == (*it)->type);

			for (int32_t i = 1; i < obj.size(); ++i)
			{
				obj[i]->type = OBJ_LIST;
				pushed++;
				iter->second.push_front(obj[i]);
			}
		}
	}

	addReplyLongLong(conn->outputBuffer(), pushed);
	return true;
}

bool Redis::rpopCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size()  !=  1)
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
			addReply(conn->outputBuffer(),shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_LIST)
			{
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}

			auto iter = listMap.find(obj[0]);
			assert(iter != listMap.end());
			assert((*it)->type == iter->first->type);
			addReplyBulk(conn->outputBuffer(), iter->second.front());
			iter->second.pop_front();
			if (iter->second.empty())
			{
				listMap.erase(iter);
				map.erase(it);
			}
		}
	}
	return false;
}

bool Redis::llenCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
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
			addReplyLongLong(conn->outputBuffer(), 0);
			return true;
		}

		if ((*it)->type != OBJ_LIST)
		{
			addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
			return true;
		}

		auto iter = listMap.find(obj[0]);
		assert(iter != listMap.end());
		assert((*it)->type == iter->first->type);
		addReplyLongLong(conn->outputBuffer(), iter->second.size());
	}
	return true;
}

bool Redis::syncCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() >  0)
	{
		return false;
	}

	TimerPtr timer;
	{
		std::unique_lock <std::mutex> lck(slaveMutex);
		auto it = repliTimers.find(conn->getSockfd());
		if (it != repliTimers.end())
		{
			LOG_WARN<<"client repeat send sync ";
			conn->forceClose();
			return true;
		}
		
		slavefd = conn->getSockfd();
		timer = conn->getLoop()->runAfter(REPLI_TIME_OUT,
				false,std::bind(&Redis::slaveRepliTimeOut,this,slavefd));
		repliTimers.insert(std::make_pair(conn->getSockfd(),timer));
		slaveConns.insert(std::make_pair(conn->getSockfd(),conn));
	}
	
	auto threadPoolVec = server.getThreadPool()->getAllLoops();
	for (auto &it : threadPoolVec)
	{
		if (conn->getLoop()->getThreadId() == it->getThreadId())
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
	conn->setMessageCallback(std::bind(&Replication::slaveCallback,
		&repli,std::placeholders::_1,std::placeholders::_2));

#ifndef _WIN32
	if (!bgsave(session,conn,true))
	{
		{
			std::unique_lock <std::mutex> lck(slaveMutex);
			repliTimers.erase(conn->getSockfd());
			conn->getLoop()->cancelAfter(timer);
			slaveConns.erase(conn->getSockfd());
		}
		slavefd = -1;
		conn->forceClose();
	}
#endif

	if (threadCount > 1)
	{
	    forkCondition.notify_all();
	}
	return true;
}

bool Redis::psyncCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return false;
}

int64_t Redis::getExpire(const RedisObjectPtr &obj)
{
	std::unique_lock <std::mutex> lck(expireMutex);
	auto it = expireTimers.find(obj);
	if (it == expireTimers.end())
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

bool Redis::dbsizeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 0)
	{
		return false;
	}

	addReplyLongLong(conn->outputBuffer(),getDbsize());
	return true;
}

bool Redis::removeCommand(const RedisObjectPtr &obj)
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
					loop.cancelAfter(iterr->second);
					expireTimers.erase(iterr);
				}
				stringMap.erase(iter);
			}
			else if ((*it)->type == OBJ_HASH)
			{
				auto iter = hashMap.find(obj);
				assert(iter != hashMap.end());
				assert(iter->first->type == OBJ_HASH);
				hashMap.erase(iter);				
			}
			else if ((*it)->type == OBJ_LIST)
			{
				auto iter = listMap.find(obj);
				assert(iter != listMap.end());
				assert(iter->first->type == OBJ_LIST);
				listMap.erase(iter);
			}
			else if ((*it)->type == OBJ_ZSET)
			{
				auto iter = zsetMap.find(obj);
				assert(iter != zsetMap.end());
				assert(iter->first->type == OBJ_ZSET);
				assert(iter->second.first.size() == iter->second.second.size());
				zsetMap.erase(iter);
			}
			else if ((*it)->type == OBJ_SET)
			{
				auto iter = setMap.find(obj);
				assert(iter != setMap.end());
				assert(iter->first->type == OBJ_SET);
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

bool Redis::delCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() < 1)
	{
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

	addReplyLongLong(conn->outputBuffer(),count);
	return true;
}

bool Redis::pingCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 0)
	{
		return false;
	}

	addReply(conn->outputBuffer(),shared.pong);
	return true;
}

bool Redis::debugCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() == 1)
	{
	    return false;
	}

	if (!strcmp(obj[0]->ptr, "sleep"))
	{
		double dtime = strtod(obj[1]->ptr,nullptr);
		int64_t utime = dtime*1000000;
		struct timespec tv;

		tv.tv_sec = utime / 1000000;
		tv.tv_nsec = (utime % 1000000) * 1000;
		std::this_thread::sleep_for(std::chrono::milliseconds(tv.tv_sec * 1000));
		addReply(conn->outputBuffer(),shared.ok);
		return true;
	}
	
	addReply(conn->outputBuffer(),shared.err);
    return true;
}

void Redis::clearCommand()
{
	{
		std::unique_lock <std::mutex> lck(expireMutex);
		for (auto &it : expireTimers)
		{
			assert(it.first->type == OBJ_EXPIRE);
			loop.cancelAfter(it.second);
		}
		expireTimers.clear();
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
				stringMap.erase(iterr);
			}
			else if (iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);
				listMap.erase(iterr);
			}
			else if (iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);
				hashMap.erase(iterr);
			}
			else if (iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.first.size() == iterr->second.second.size());
				zsetMap.erase(iterr);
			}
			else if (iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);
				setMap.erase(iterr);
			}
			else
			{
				LOG_WARN<<"type unkown:"<<iter->type;
				assert(false);
			}
		}
		map.clear();
	}
}

bool Redis::keysCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1 )
	{
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
					addReplyBulkCBuffer(conn->outputBuffer(),iter->ptr,sdslen(iter->ptr));
					numkeys++;
				}
			}
		}
	}

	prePendReplyLongLongWithPrefix(conn->outputBuffer(),numkeys);
	return true;
}

bool Redis::flushdbCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 0)
	{
		return false;
	}

	clearCommand();
	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

bool Redis::quitCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	conn->forceClose();
	return true;
}

bool Redis::zaddCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() < 3)
	{
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

			if (getDoubleFromObjectOrReply(conn->outputBuffer(),
					obj[1],&scores,nullptr) != REDIS_OK)
			{
				return false;
			}

			obj[2]->type = OBJ_ZSET;
			
			SortIndexMap indexMap;
			SortMap sortMap;
		
			indexMap.insert(std::make_pair(obj[2],scores));
			sortMap.insert(std::make_pair(scores,obj[2]));
			
			added++;

			for (int i = 3; i < obj.size(); i += 2)
			{
				obj[i + 1]->type = OBJ_ZSET;
				if (getDoubleFromObjectOrReply(conn->outputBuffer(),
						obj[1],&scores,nullptr) != REDIS_OK)
				{
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
							if (!memcmp(iterr->second->ptr,
									obj[i + 1]->ptr,sdslen(obj[i + 1]->ptr)))
							{
								const RedisObjectPtr &v = iterr->second;
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
				}
			}

			map.insert(obj[0]);
			zsetMap.insert(std::make_pair(obj[0],
					std::make_pair(std::move(indexMap),std::move(sortMap))));
			addReplyLongLong(conn->outputBuffer(),added);
			return true;	
		}
		else
		{
			if ((*it)->type != OBJ_ZSET)
			{
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}

			auto iter = zsetMap.find(obj[0]);
			assert(iter != zsetMap.end());
			assert(iter->first->type == (*it)->type);
			for (int i = 1; i < obj.size(); i += 2)
			{
				obj[i + 1]->type = OBJ_ZSET;
				if (getDoubleFromObjectOrReply(conn->outputBuffer(),
						obj[i],&scores,nullptr) != REDIS_OK)
				{
					return false;
				}

				auto iterr = iter->second.first.find(obj[i + 1]);
				if (iterr == iter->second.first.end())
				{
					iter->second.first.insert(std::make_pair(obj[i + 1],scores));
					iter->second.second.insert(std::make_pair(scores,obj[i + 1]));
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
								const RedisObjectPtr & v = iterrr->second;
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
				}
			}
		}
		addReplyLongLong(conn->outputBuffer(),added);
	}
	return true;
}

bool Redis::zrangeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return zrangeGenericCommand(obj,session,conn,0);
}

bool Redis::zcardCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size()  != 1 )
	{
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
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
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

	addReplyLongLong(conn->outputBuffer(),len);
	return true;
}

bool Redis::zrevrangeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	return zrangeGenericCommand(obj,session,conn,1);
}

bool Redis::scardCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1 )
	{
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
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}

			auto iter = setMap.find(obj[0]);
			assert(iter != setMap.end());
			assert(iter->first->type == (*it)->type);
			len = iter->second.size();
		}
		addReplyLongLong(conn->outputBuffer(),len);
	}
	return false;
}

RedisObjectPtr Redis::createDumpPayload(const RedisObjectPtr &dump)
{
	RedisObjectPtr o,dumpobj;
	Rio payload;
	unsigned char buf[2];
	uint64_t crc;
	rdb.rioInitWithBuffer(&payload,sdsempty());
	buf[0] = REDIS_RDB_VERSION & 0xff;
	buf[1] = (REDIS_RDB_VERSION >> 8) & 0xff;
	if (rdb.createDumpPayload(&payload, dump) == REDIS_ERR)
	{
		LOG_WARN << "RDB dump error";
		return nullptr;
	}

	payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr,buf,2);
	crc = crc64(0, (unsigned char*)payload.io.buffer.ptr,sdslen(payload.io.buffer.ptr));
	memrev64ifbe(&crc);
	payload.io.buffer.ptr = sdscatlen(payload.io.buffer.ptr,&crc,8);

	dumpobj = createObject(OBJ_STRING,payload.io.buffer.ptr);
	return dumpobj;
}

bool Redis::dumpCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
	{
		return false;
	}

	RedisObjectPtr dumpobj = createDumpPayload(obj[0]);
	if (dumpobj == nullptr)
	{
		addReplyErrorFormat(conn->outputBuffer(),"RDB dump error");
		return true;
	}

	addReplyBulk(conn->outputBuffer(),dumpobj);
	return true;
}

bool Redis::restoreCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() < 3 || obj.size()  > 4 )
	{
		return false;
	}

	int64_t ttl;
	int type, replace = 0;

	for (int i = 3; i < obj.size(); i++)
	{
		if (!strcmp(obj[i]->ptr,"replace")) 
		{
			replace = 1;
		}
		else
		{
			addReply(conn->outputBuffer(),shared.syntaxerr);
        	return true;
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
			addReply(conn->outputBuffer(),shared.busykeyerr);
			return true;
		}
	}

	if (getLongLongFromObjectOrReply(conn->outputBuffer(),obj[1],&ttl,nullptr) != REDIS_OK)
	{
		return true;
	} 
	else if (ttl < 0) 
	{
		addReplyError(conn->outputBuffer(),"Invalid TTL value, must be >= 0");
		return true;
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
		return false;
	}
	
	footer = p+(len-10);
	rdbver = (footer[1] << 8) | footer[0];
	if (rdbver > REDIS_RDB_VERSION) 
	{
		return false;
	}

	crc = crc64(0,p,len-8);
	memrev64ifbe(&crc);
	if (memcmp(&crc,footer+2,8) != 0)
	{
		LOG_WARN<<"DUMP payload version or checksum are wrong";
		return false;
	}

	rdb.rioInitWithBuffer(&payload,obj[2]->ptr);
	RedisObjectPtr key = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
	if (rdb.verifyDumpPayload(&payload,key) == REDIS_ERR)
	{
		addReplyError(conn->outputBuffer(),"Bad data format");
		return true;
	}

	if (ttl > 0)
	{
		RedisObjectPtr ex = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
		ex->type = OBJ_EXPIRE;
		std::unique_lock <std::mutex> lck(slaveMutex);
		TimerPtr timer = loop.runAfter(ttl / 1000,
				false,std::bind(&Redis::setExpireTimeOut,this,ex));
		auto it = expireTimers.find(ex);
		assert(it == expireTimers.end());
		expireTimers.insert(std::make_pair(ex,timer));	
	}

	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

bool Redis::existsCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 0)
	{
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
			addReplyLongLong(conn->outputBuffer(),0);
		}
		else
		{
			addReplyLongLong(conn->outputBuffer(),1);
		}
	}
	return true;
}

bool Redis::saddCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() < 2)
	{
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
			
			std::unordered_set<RedisObjectPtr,Hash,Equal> set;
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

				}
				setMap.insert(std::make_pair(obj[0],std::move(set)));
				map.insert(obj[0]);
			}
		}
		else
		{
			if ((*it)->type != OBJ_SET)
			{
				addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}

			auto iter = setMap.find(obj[0]);
			assert(iter != setMap.end());
			assert(iter->first->type == (*it)->type);

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

				}
			}
		}
	}
		
	addReplyLongLong(conn->outputBuffer(),len);
	return true;
}	

bool Redis::zrangeGenericCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn,int reverse)
{
	if (obj.size()  != 4)
	{
		return false;
	}

	int rangelen;
	int withscores = 0;
	int64_t start;
	
	if (getLongLongFromObjectOrReply(conn->outputBuffer(),
			obj[1],&start,nullptr) != REDIS_OK)
	{
		addReplyError(conn->outputBuffer(),"unknown double param error");
		return true;
	}

	int64_t end;

	if (getLongLongFromObjectOrReply(conn->outputBuffer(),
			obj[2],&end,nullptr) != REDIS_OK)
	{
		addReplyError(conn->outputBuffer(),"unknown double param error");
		return true;
	}

	if (!strcmp(obj[3]->ptr, "withscores"))
	{
		withscores = 1;
	}
	else if (obj.size() >= 5)
	{
		addReply(conn->outputBuffer(),shared.syntaxerr);
		return true;
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
			addReply(conn->outputBuffer(),shared.emptymultibulk);
			return true;
		}
		else
		{
			if ((*it)->type != OBJ_ZSET)
			{
				addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
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
				addReply(conn->outputBuffer(),shared.emptymultibulk);
				return true;
			}

			if (end >= llen) 
			{
				end = llen-1;	
			}

			rangelen = (end-start) + 1;	
			addReplyMultiBulkLen(conn->outputBuffer(),withscores ? (rangelen*2) : rangelen);

			if (reverse)
			{
				int count = 0;
				for (auto iterr = iter->second.second.rbegin();
						iterr != iter->second.second.rend(); ++iterr)
				{
					if (count++ >= start)
					{
						addReplyBulkCBuffer(conn->outputBuffer(),
							iterr->second->ptr,sdslen(iterr->second->ptr));
						if (withscores)
						{
							addReplyDouble(conn->outputBuffer(),iterr->first);
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
				for (auto iterr = iter->second.second.begin();
						iterr != iter->second.second.end(); ++iterr)
				{
					if (count++ >= start)
					{
						addReplyBulkCBuffer(conn->outputBuffer(),
							iterr->second->ptr, sdslen(iterr->second->ptr));
						if (withscores)
						{
							addReplyDouble(conn->outputBuffer(),iterr->first);
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
	return true;
}


bool Redis::hgetallCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
	{
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
			addReply(conn->outputBuffer(),shared.emptymultibulk);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			addReplyMultiBulkLen(conn->outputBuffer(),iter->second.size() * 2);
			for (auto &iterr : iter->second)
			{
				addReplyBulkCBuffer(conn->outputBuffer(),
						iterr.first->ptr,sdslen(iterr.first->ptr));
				addReplyBulkCBuffer(conn->outputBuffer(),
						iterr.second->ptr,sdslen(iterr.second->ptr));
			}
		}
	}
	return true;
}

bool Redis::hgetCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 2)
	{
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
			addReply(conn->outputBuffer(),shared.nullbulk);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			auto iterr = iter->second.find(obj[1]);
			if (iterr == iter->second.end())
			{
				addReply(conn->outputBuffer(),shared.nullbulk);
			}
			else
			{
				addReplyBulk(conn->outputBuffer(),iterr->second);
			}
		}
	}
	return true;
}

bool Redis::hkeysCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
	{
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
			addReply(conn->outputBuffer(),shared.emptymultibulk);
			return true;
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(conn->outputBuffer(),
						"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			addReplyMultiBulkLen(conn->outputBuffer(),iter->second.size());

			for (auto &iterr : iter->second)
			{
				addReplyBulkCBuffer(conn->outputBuffer(),
						iterr.first->ptr,sdslen(iterr.first->ptr));
			}
		}
	}	
	return true;
}

bool Redis::hlenCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
	{
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
				addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			assert(iter->first->type == (*it)->type);
			assert(!iter->second.empty());
			len = iter->second.size();
		}
	}

	addReplyLongLong(conn->outputBuffer(),len);
	return true;
}

bool Redis::hsetCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 3)
	{
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
			std::unordered_map<RedisObjectPtr,RedisObjectPtr,Hash,Equal> rhash;
			rhash.insert(std::make_pair(obj[1],obj[2]));
			hashMap.insert(std::make_pair(obj[0],std::move(rhash)));
			map.insert(obj[0]);
		}
		else
		{
			if ((*it)->type != OBJ_HASH)
			{
				addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			auto iter = hashMap.find(obj[0]);
			assert(iter != hashMap.end());
			
			auto iterr = iter->second.find(obj[1]);
			if (iterr == iter->second.end())
			{
				iter->second.insert(std::make_pair(obj[1],obj[2]));
			}
			else
			{
				iterr->second = obj[2];
				update = true;
			}
		}
	}

	addReply(conn->outputBuffer(),update ? shared.czero : shared.cone);
	return true;
}

bool Redis::setCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{	
	if (obj.size() <  2 || obj.size() > 8 )
	{
		return false;
	}

	int32_t j;
	RedisObjectPtr expire = nullptr;
	RedisObjectPtr ex = nullptr;
	int32_t unit = UNIT_SECONDS;
	int32_t flags = OBJ_SET_NO_FLAGS;

	for (j = 2; j < obj.size(); j++)
	{
		const char *a = obj[j]->ptr;
		const RedisObjectPtr &next = (j == obj.size() - 1) ? nullptr : obj[j + 1];

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
			addReply(conn->outputBuffer(),shared.syntaxerr);
			return true;
		}
	}
	
	int64_t milliseconds = 0;
	if (expire)
	{
		if (getLongLongFromObjectOrReply(conn->outputBuffer(),
				expire,&milliseconds,nullptr) != REDIS_OK)
		{
			return true;
		}
		if (milliseconds <= 0)
		{
		    addReplyErrorFormat(conn->outputBuffer(),"invalid expire time in");
		    return true;
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
				addReply(conn->outputBuffer(),shared.nullbulk);
				return true;
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
				addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}
			
			if (flags & OBJ_SET_NX)
			{
				addReply(conn->outputBuffer(),shared.nullbulk);
				return true;
			}

			if (expire)
			{
				ex = createStringObject(obj[0]->ptr,sdslen(obj[0]->ptr));
			}

			auto iter = stringMap.find(obj[0]);
			assert(iter != stringMap.end());
			iter->second = obj[1];
		}
	}

	if (expire)
	{
		{
			std::unique_lock <std::mutex> lck(expireMutex);
			ex->type = OBJ_EXPIRE;
			auto iter = expireTimers.find(ex);
			if (iter != expireTimers.end())
			{
				loop.cancelAfter(iter->second);
				expireTimers.erase(iter);
			}
			setExpire(ex,milliseconds / 1000);
		}
	}

	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

bool Redis::getCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{	
	if (obj.size() != 1)
	{
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
			addReply(conn->outputBuffer(),shared.nullbulk);
			return false;
		}

		auto iter = stringMap.find(obj[0]);
		assert(iter != stringMap.end());
		assert(iter->first->type == (*it)->type);

		if ((*it)->type != OBJ_STRING)
		{
			addReplyErrorFormat(conn->outputBuffer(),
				"WRONGTYPE Operation against a key holding the wrong kind of value");
			return true;
		}

		addReplyBulk(conn->outputBuffer(),iter->second);
	}
	return true;
}

bool Redis::incrCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
	{
		return false;
	}
	return incrDecrCommand(obj[0],session,conn,1);
}

bool Redis::decrCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
	{
		return false;
	}
	return incrDecrCommand(obj[0],session,conn,-1);
}

bool Redis::incrDecrCommand(const RedisObjectPtr &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn,int64_t incr)
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
			addReplyLongLong(conn->outputBuffer(),incr);
			return true;
		}
		else
		{
			if ((*it)->type != OBJ_STRING)
			{
				addReplyErrorFormat(conn->outputBuffer(),
					"WRONGTYPE Operation against a key holding the wrong kind of value");
				return true;
			}

			auto iter = stringMap.find(obj);
			assert(iter != stringMap.end());

			int64_t value;
			if (getLongLongFromObjectOrReply(conn->outputBuffer(),
					iter->second,&value,nullptr) != REDIS_OK)
				return false;

			value += incr;
			if ((incr < 0 && value < 0 && incr < (LLONG_MIN-value)) ||
				(incr > 0 && value > 0 && incr > (LLONG_MAX-value)))
			{
				addReplyError(conn->outputBuffer(),"increment or decrement would overflow");
				return true;
			}

			iter->second = createStringObjectFromLongLong(value);
			addReply(conn->outputBuffer(),shared.colon);
			addReply(conn->outputBuffer(),iter->second);
			addReply(conn->outputBuffer(),shared.crlf);
			return true;
		}
	}
}

bool Redis::ttlCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() != 1)
	{
		return false;
	}

	std::unique_lock <std::mutex> lck(expireMutex);
	auto it = expireTimers.find(obj[0]);
	if (it == expireTimers.end())
	{
		addReplyLongLong(conn->outputBuffer(),-2);
		return true;
	}
	
	int64_t ttl = it->second->getExpiration().getMicroSecondsSinceEpoch()
	 - TimeStamp::now().getMicroSecondsSinceEpoch();
	
	addReplyLongLong(conn->outputBuffer(),ttl / 1000000);
	return true;
}

bool Redis::monitorCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session,const TcpConnectionPtr &conn)
{
	if (obj.size() > 0)
	{
		return false;
	}

	monitorEnabled = true;
	std::unique_lock <std::mutex> lck(monitorMutex);
	monitorConns[conn->getSockfd()] = conn;
	addReply(conn->outputBuffer(),shared.ok);
	return true;
}

void Redis::flush()
{

}

bool Redis::checkCommand(const RedisObjectPtr &cmd)
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

	threadCount = 1;
	masterPort = 0;
	slaveEnabled = false;
	authEnabled = false;
	repliEnabled = false;
	salveCount = 0;
	clusterSlotEnabled = false;
	clusterRepliMigratEnabled = false;
	clusterRepliImportEnabeld = false;
	monitorEnabled = false;
	forkEnabled = false;
	forkCondWaitCount = 0;
	rdbChildPid = -1;
	slavefd = -1;
	masterfd = -1;
	dbnum = 1;

	createSharedObjects();
	char buf[32];
	int32_t len = ll2string(buf,sizeof(buf),getPort());
	shared.rPort = createStringObject(buf,len);
	shared.rIp = createStringObject(getIp().data(),getIp().length());

#define REGISTER_REDIS_COMMAND(msgId,func) \
	handlerCommands[msgId] = std::bind(&Redis::func,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
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
	REGISTER_REDIS_COMMAND(shared.monitor,monitorCommand);

#define REGISTER_REDIS_REPLY_COMMAND(msgId) \
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
	cluterCommands.insert(msgId);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(shared.cluster);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(shared.migrate);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(shared.command);

	master = "master";
	slave = "slave";
	ipPort = ip + "::" + std::to_string(port);
}





