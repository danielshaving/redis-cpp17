#include "xRedis.h"

xRedis::xRedis()
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

	createSharedObjects();
	loadDataFromDisk();
	server.init(&loop,"0.0.0.0",6379,this);
	server.setConnectionCallback(
	      std::bind(&xRedis::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	server.setThreadNum(0);
	server.start();

}

xRedis::~xRedis()
{

}
void xRedis::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		std::shared_ptr<xSession> session (new xSession(this,conn));
		std::unique_lock<std::mutex> lk(mutex);
		sessions[conn->getSockfd()] = session;

		//TRACE("redis client connect success\n");
	}
	else
	{
		std::unique_lock<std::mutex> lk(mutex);
		sessions.erase(conn->getSockfd());
		//TRACE("redis client disconnect\n");
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
	long long start = ustime();
	char rdb_filename[] = "dump.rdb";
	if(rdb.rdbLoad(rdb_filename) == REDIS_OK)
	{
		TRACE("DB loaded from disk: %.3f seconds\n", (float)((ustime()-start)/ 1000000));
	}
	else
	{
		TRACE("Fatal error loading the DB: Exiting\n");
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
	if(rdb.rdbSave(filename) == REDIS_OK)
	{
		addReply(session->sendBuf,shared.ok);
	}
	else
	{
		addReply(session->sendBuf,shared.err);
	}


	return true;
}

bool xRedis::dbsizeCommond(const std::vector<rObj*> & obj,xSession * session)
{
	if(obj.size() > 0)
	{
		addReplyErrorFormat(session->sendBuf,"unknown dbsize error");
		return false;
	}

	addReplyLongLong(session->sendBuf,setMap.size() + hsetMap.size());
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

	auto it = hsetMap.find(obj[0]);
	if(it == hsetMap.end())
	{
		addReply(session->sendBuf,shared.emptymultibulk);
		return false;
	}

	addReplyMultiBulkLen(session->sendBuf,it->second.size() * 2);

	for(auto it1 = it->second.begin(); it1 != it->second.end(); it1++)
	{
		addReplyBulkCBuffer(session->sendBuf,it1->first->ptr,sdsllen(it1->first->ptr));
		addReplyBulkCBuffer(session->sendBuf,it1->second->ptr,sdsllen(it1->second->ptr));
	}

	for(auto it2 = obj.begin(); it2 != obj.end(); it2++)
	{
		zfree(*it2);
	}

	return true;
}


bool xRedis::hsetCommond(const std::vector<rObj*> & obj,xSession * session)
{
	if(obj.size() != 3)
	{
		addReplyErrorFormat(session->sendBuf,"unknown  hset error");
		return false;
	}


	bool update = false;
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
		auto it1 = it->second.find(obj[1]);
		if(it1 == it->second.end())
		{
			it->second.insert(std::make_pair(obj[1],obj[2]));
		}
		else
		{
			auto x = it1->first;
			auto y = it1->second;
			it->second.erase(it1);
			zfree(x);
			zfree(y);

			it->second.insert(std::make_pair(obj[1],obj[2]));
			update = true;
		}
	}

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


	auto it = hsetMap.find(obj[0]);
	if(it == hsetMap.end())
	{
		addReply(session->sendBuf,shared.nullbulk);
		return false;
	}

	auto it1 = it->second.find(obj[1]);
	if(it1 == it->second.end())
	{
		addReply(session->sendBuf,shared.nullbulk);
		return false;
	}

	for(auto it2 = obj.begin(); it2 != obj.end(); it2++)
	{
		zfree(*it2);
	}



	addReplyBulk(session->sendBuf,it1->second);

	return true;
}


bool xRedis::flushdbCommond(const std::vector<rObj*> & obj,xSession * session)
{
	{
		for(auto it = hsetMap.begin(); it != hsetMap.end(); it++)
		{
			for(auto it1 = it->second.begin(); it1 != it->second.end(); it1 ++)
			{
				zfree(it1->first);
				zfree(it1->second);
			}
			it->second.clear();
			zfree(it->first);
		}

		hsetMap.clear();
	}

	{
		for(auto it = setMap.begin(); it != setMap.end(); it++)
		{
			zfree(it->first);
			zfree(it->second);
		}
		setMap.clear();
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
		addReplyErrorFormat(session->sendBuf,"unknown  param error");
		return false;
	}


	auto it = setMap.find(obj[0]);
	if(it != setMap.end())
	{
		setMap.erase(it);
		zfree(it->first);
		zfree(it->second);
	}
	setMap[obj[0]] = obj[1];

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

	auto it = setMap.find(obj[0]);
	if(it == setMap.end())
	{
		addReply(session->sendBuf,shared.nullbulk);
		return false;
	}
	zfree(obj[0]);
	addReplyBulk(session->sendBuf,it->second);
	return true;
}


void xRedis::flush()
{

}



