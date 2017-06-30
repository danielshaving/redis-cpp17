#include "xRedis.h"



xRedis::xRedis(const char * ip,int32_t port,int32_t threadCount,bool enbaledCluster)
:host(ip),
port(port),
threadCount(threadCount),
masterPort(0),
clusterEnabled(enbaledCluster),
slaveEnabled(false),
repliEnabled(false),
authEnabled(false),
salveCount(0),
slaveRepliTimer(nullptr),
slaveRepliCacheTimer(nullptr)
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
	char filename[] = "dump.rdb";
	if(rdbSave(filename,this) == REDIS_OK)
	{
		LOG_INFO<<"Save rdb success";
	}
	else
	{
		LOG_INFO<<"Save rdb failure";
	}

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
		for(auto it = setShards.begin(); it != setShards.end(); it++)
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
		for(auto it = hsetShards.begin(); it != hsetShards.end(); it++)
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


void xRedis::handleSalveRepliTimeOut()
{
	repliEnabled = false;
	--salveCount;
	{
		MutexLockGuard mu(slaveMutex);
		slaveCached.retrieveAll();
	}
	LOG_INFO<<"handler handleSalveRepliTimeOut timeout ";
}


void xRedis::handleRepliCacheTimeOut()
{
	repliEnabled = false;
	{
		MutexLockGuard mu(slaveMutex);
		slaveCached.retrieveAll();
	}
	LOG_INFO<<"handler handleRepliCacheTimeOut timeout ";
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
		MutexLockGuard mu(mutex);
		sessions.erase(conn->getSockfd());
		auto it = tcpconnMaps.find(conn->getSockfd());
		if(it !=  tcpconnMaps.end())
		{
			if(slaveRepliCacheTimer != nullptr)
			{
				loop.cancelAfter(slaveRepliCacheTimer);
			}

			--salveCount;
			slaveRepliCacheTimer = loop.runAfter(REPLI_TIME_OUT,true,std::bind(&xRedis::handleRepliCacheTimeOut,this));
			tcpconnMaps.erase(it);
			repliEnabled = true;
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
	else
	{
		LOG_INFO<<"load rdb fail";
	}
}




bool xRedis::infoCommond(const std::deque <rObj*> & obj,xSession * session)
{
	addReply(session->sendBuf,shared.ok);
	return true;
}


bool xRedis::clientCommond(const std::deque <rObj*> & obj,xSession * session)
{
	addReply(session->sendBuf,shared.ok);
	return true;
}

bool xRedis::echoCommond(const std::deque <rObj*> & obj,xSession * session)
{
	addReply(session->sendBuf,shared.ok);
	return true;
}

bool xRedis::subscribeCommond(const std::deque <rObj*> & obj,xSession * session)
{
	addReply(session->sendBuf,shared.ok);
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

	repliEnabled = true;

	{
		MutexLockGuard lk(mutex);
		tcpconnMaps.insert(std::make_pair(session->conn->getSockfd(),session->conn));
	}

	salveCount++;
	if(slaveRepliTimer != nullptr)
	{
		loop.cancelAfter(slaveRepliTimer);
	}

	slaveRepliTimer = loop.runAfter(REPLI_TIME_OUT,true,std::bind(&xRedis::handleSalveRepliTimeOut,this));

	saveCommond(obj,session);
	char rdb_filename[] = "dump.rdb";
	size_t len = 0;

	MutexLockGuard lk(mutex);
	rObj * buf = rdbLoad(rdb_filename,len);
	if(buf != nullptr)
	{
		LOG_INFO<<"sync load rdb success";
		char str[4];
		int * sendLen = (int*)str;
		*sendLen = len;
		session->sendBuf.append((const char*)str,4);
		session->sendBuf.append(buf->ptr,len);
	}
	else
	{
		salveCount--;
		tcpconnMaps.erase(session->conn->getSockfd());
		repliEnabled = false;
		slaveCached.retrieveAll();
		loop.cancelAfter(slaveRepliTimer);
		LOG_INFO<<"sync load rdb failure";
	}
	
	zfree(buf);
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

	MutexLockGuard lk(mutex);
	if(!repliEnabled)
	{
		//send slave sync
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
	
		for(auto it = setShards.begin(); it != setShards.end(); it++)
		{	
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lk(mu);
			size+=(*it).setMap.size();
		}
	}

	{
		for(auto it = hsetShards.begin(); it != hsetShards.end(); it++)
		{
			MutexLock &mu = (*it).mutex;
			MutexLockGuard lk(mu);
			size+=(*it).hsetMap.size();
		}
	}
	
	addReplyLongLong(session->sendBuf,size);
	
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
	
	MutexLock &mu = hsetShards[hash% kShards].mutex;
	auto &hsetMap = hsetShards[hash% kShards].hsetMap;
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
	
	MutexLock &mu = hsetShards[hash% kShards].mutex;
	auto &hsetMap = hsetShards[hash% kShards].hsetMap;
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
	MutexLock &mu = hsetShards[hash% kShards].mutex;
	auto &hsetMap = hsetShards[hash% kShards].hsetMap;
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
		for(auto it = setShards.begin(); it != setShards.end(); it++)
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
		for(auto it = hsetShards.begin(); it != hsetShards.end(); it++)
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

bool xRedis::delCommond(const std::deque <rObj*> & obj,xSession * session)
{
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
	MutexLock &mu = setShards[hash% kShards].mutex;
	SetMap & setMap = setShards[hash % kShards].setMap;
	{
		MutexLockGuard lock(mu);
		rObj * obj1 = obj[0];
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
	MutexLock &mu = setShards[hash  % kShards].mutex;
	SetMap & setMap = setShards[hash  % kShards].setMap;
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



