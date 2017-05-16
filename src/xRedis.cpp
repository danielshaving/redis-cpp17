#include "xRedis.h"



xRedis::xRedis(const char * ip,int32_t port,int32_t threadCount)
:host(ip),
port(port),
threadCount(threadCount)
{
	
#define REGISTER_REDIS_HANDLER(table, func) \
	handlerCommondMap.insert(std::make_pair(table, std::bind(&xRedis::func, this, std::placeholders::_1, std::placeholders::_2)));
	REGISTER_REDIS_HANDLER("set",setCommond);
	REGISTER_REDIS_HANDLER("get",getCommond);
	REGISTER_REDIS_HANDLER("flushdb",flushdbCommond);
	REGISTER_REDIS_HANDLER("dbsize",dbsizeCommond);
	REGISTER_REDIS_HANDLER("quit",quitCommond);
	REGISTER_REDIS_HANDLER("hset",hsetCommond);
	REGISTER_REDIS_HANDLER("hget",hgetCommond);
	REGISTER_REDIS_HANDLER("hgetall",hgetallCommond);
	REGISTER_REDIS_HANDLER("ping",pingCommond);
	REGISTER_REDIS_HANDLER("save",saveCommond);
	REGISTER_REDIS_HANDLER("slaveof",slaveofCommond);
	REGISTER_REDIS_HANDLER("sync",syncCommond);
	REGISTER_REDIS_HANDLER("COMMAND",commandCommond);
	
	vectorCommonds.insert(std::make_pair("set",2));
	vectorCommonds.insert(std::make_pair("hset",3));
	
	slaveEnabled = false;
	createSharedObjects();
	loadDataFromDisk();
	server.init(&loop, host, port,this);
	server.setConnectionCallback(std::bind(&xRedis::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	server.setThreadNum(threadCount);
	server.start();

//	loop.runAfter(10,true,std::bind(&xRedis::handleTimeout,this));
//	loop.runAfter(5,true,std::bind(&xRedis::handleTimeout,this));
//   loop.runAfter(1,true,std::bind(&xRedis::handleTimeout,this));
}

xRedis::~xRedis()
{
	destorySharedObjects();

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

void xRedis::handleTimeout()
{
	loop.quit();

}





void xRedis::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		struct sockaddr_in sa;
		socklen_t len = sizeof(sa);
		if(!getpeername(conn->getSockfd(), (struct sockaddr *)&sa, &len))
		{
			//LOG_INFO<<"From slave:"<<inet_ntoa(sa.sin_addr)<<":"<<ntohs(sa.sin_port);
		}
		
		std::shared_ptr<xSession> session (new xSession(this,conn));
		MutexLockGuard mu(mutex);
		sessions[conn->getSockfd()] = session;
		conn->host = inet_ntoa(sa.sin_addr);
		conn->port = ntohs(sa.sin_port);
		LOG_INFO<<"Client connect success";
		
	}
	else
	{
		MutexLockGuard mu(mutex);
		sessions.erase(conn->getSockfd());
		auto it = tcpconnMaps.find(conn->getSockfd());
		if(it != tcpconnMaps.end())
		{
			tcpconnMaps.erase(conn->getSockfd());
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

bool xRedis::saveCommond(const std::vector<rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown save error");
		return false;
	}

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


	return true;
}


bool xRedis::slaveofCommond(const std::vector<rObj*> & obj,xSession * session)
{
	if(obj.size() !=  2)
	{
		addReplyErrorFormat(session->sendBuf,"unknown slaveof error");
		return false;
	}

	 if (!strcasecmp(obj[0]->ptr,"no") &&!strcasecmp(obj[1]->ptr,"one")) 
	 {
		if (masterHost.c_str()) 
		{
			LOG_WARN<<"MASTER MODE enabled (user request from "<<masterHost.c_str()<<":"<<masterPort;
			vectors.clear();	
			slaveEnabled =  false;
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

		vectors.clear();
		std::shared_ptr<xReplication> re = std::shared_ptr<xReplication>(new xReplication());
		re->replicationSetMaster(this,obj[0],port);
		vectors.push_back(re);
	      LOG_INFO<<"SLAVE OF "<<obj[0]->ptr<<":"<<port<<" enabled (user request from client";
	 }
	 
	addReply(session->sendBuf,shared.ok);
	return true;
}



bool xRedis::commandCommond(const std::vector<rObj*> & obj,xSession * session)
{
	addReply(session->sendBuf,shared.ok);	
	return true;
}


bool xRedis::syncCommond(const std::vector<rObj*> & obj,xSession * session)
{
	if(obj.size() >  0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown sync  error");
		return false;
	}

	{
		struct sockaddr_in sa;
		socklen_t len = sizeof(sa);
		if(!getpeername(session->conn->getSockfd(), (struct sockaddr *)&sa, &len))
		{
			LOG_INFO<<"From slave:"<<inet_ntoa(sa.sin_addr)<<":"<<ntohs(sa.sin_port);
		}
	}
	
	saveCommond(obj,session);
	char rdb_filename[] = "dump.rdb";
	size_t len = 0;

	rObj * buf = rdbLoad(rdb_filename,len);
	if(buf != nullptr)
	{
		LOG_INFO<<"sync load rdb success";
		char * str = (char*)zmalloc(4);
		int * sendLen = (int*)str;
		*sendLen = len;
		session->sendBuf.append((const char*)str,4);
		session->sendBuf.append(buf->ptr,len);
		zfree(str);
		tcpconnMaps.insert(std::make_pair(session->conn->getSockfd(),session->conn));
	}
	else
	{
		LOG_INFO<<"sync load rdb fail";
	}
	
	zfree(buf);
	return true;
}



bool xRedis::psyncCommond(const std::vector<rObj*> & obj,xSession * session)
{
	return true;
}


bool xRedis::dbsizeCommond(const std::vector<rObj*> & obj,xSession * session)
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

bool xRedis::pingCommond(const std::vector<rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown ping error");
		return false;
	}

	addReply(session->sendBuf,shared.pong);
	return true;
}


bool xRedis::hgetallCommond(const std::vector<rObj*> & obj,xSession * session)
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


bool xRedis::hsetCommond(const std::vector<rObj*> & obj,xSession * session)
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
				it->second.erase(iter);
				zfree(iter->first);
				zfree(iter->second);
				it->second.insert(std::make_pair(obj[1],obj[2]));
				update = true;
			}
		}
	}
	
	if(!slaveEnabled)
	addReply(session->sendBuf,update ? shared.czero : shared.cone);

	return true;
}

bool xRedis::hgetCommond(const std::vector<rObj*> & obj,xSession * session)
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



bool xRedis::flushdbCommond(const std::vector<rObj*> & obj,xSession * session)
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

bool xRedis::quitCommond(const std::vector<rObj*> & obj,xSession * session)
{
	session->conn->forceClose();
	return true;
}

bool xRedis::delCommond(const std::vector<rObj*> & obj,xSession * session)
{
	return true;
}

bool xRedis::setCommond(const std::vector<rObj*> & obj,xSession * session)
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
			setMap.erase(it);
			zfree(it->second);
			zfree(it->first);
			setMap.insert(std::make_pair(obj[0],obj[1]));
		}
	}
	
	if(!slaveEnabled)
	addReply(session->sendBuf,shared.ok);
	return true;
}

bool xRedis::getCommond(const std::vector<rObj*> & obj,xSession * session)
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



