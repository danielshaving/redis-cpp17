#include "xReplication.h"
#include "xRedis.h"
#include "xLog.h"


xReplication::~xReplication()
{
	redis->masterHost.clear();
	redis->masterPort = 0;
	client->disconnect();
	loop->quit();
	threads->join();
}

void xReplication::connectMaster(const char * str,long port)
{
	pid = getpid();
	start = true;
	condition.notify_one();
	xEventLoop loop;
	xTcpClient client(&loop,str,port,this);
	client.setConnectionCallback(std::bind(&xReplication::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	client.setMessageCallback( std::bind(&xReplication::readCallBack, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
	client.setConnectionErrorCallBack(std::bind(&xReplication::connErrorCallBack, this));
	this->client = & client;
	this->loop = &loop;
	client.connect();
	loop.run();
}


void xReplication::replicationCron()
{
	
}



void xReplication::syncWrite(const xTcpconnectionPtr& conn)
{
	sendBuf.append(shared.sync->ptr,sdsllen(shared.sync->ptr));
}

void xReplication::syncWithMaster(const xTcpconnectionPtr& conn)
{
	int sockerr = 0;
   	socklen_t errlen = sizeof(sockerr);
	/* Check for errors in the socket. */
	if (getsockopt(conn->getSockfd(), SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
	   sockerr = errno;
	if (sockerr) 
	{
	   LOG_WARN<<"Error condition on socket for SYNC"<< strerror(sockerr);
	  return ;
	}
	
	syncWrite(conn);	
	
}

void xReplication::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
{
	while(recvBuf->readableBytes() > 0 && recvBuf->readableBytes()  >= 5)
	{
		if(memcmp(recvBuf->peek(),shared.ok->ptr,sdsllen(shared.ok->ptr)) != 0)
		{
			LOG_WARN<<"OK\r\n error";
			conn->forceClose();
			break;
		}
		
		int32_t  len = *(int32_t*)(recvBuf->peek() + 5);
		if(len >= 4096  * 4096 * 100 || len <=0)
		{
			LOG_WARN<<"Length is too large";
			conn->forceClose();
			break;
		}
		
		if(len > recvBuf->readableBytes() - 9)
		{
			break;
		}

		recvBuf->retrieve(9);
		
		char rdb_filename[] = "dump.rdb";
		if(rdbWrite(rdb_filename,recvBuf->peek(), len) == REDIS_OK)
		{	
			LOG_INFO<<"Replication save  rdb success";
		}
		else
		{
			LOG_INFO<<"Replication save rdb failure";
			recvBuf->retrieve(len);
			break;
		}
		
		if(rdbLoad(rdb_filename,redis) == REDIS_OK)
		{
			LOG_INFO<<"Replication load rdb success";
		}
		else
		{
			LOG_INFO<<"Replication load rdb failure";
			recvBuf->retrieve(len);
			break;
		}
			
		std::shared_ptr<xSession> session (new xSession(redis,conn));
		MutexLockGuard mu(redis->mutex);
		redis->sessions[conn->getSockfd()] = session;
		recvBuf->retrieve(len);
		
	}
	
}


void xReplication::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		redis->masterHost = client->ip;
		redis->masterPort = client->port;
		LOG_INFO<<"Connect master success";
		syncWithMaster(conn);
		if(sendBuf.readableBytes() > 0 )
		{
			conn->send(&sendBuf);	
		}

	}
	else
	{
		MutexLockGuard mu(redis->mutex);
		redis->sessions.erase(conn->getSockfd());
		LOG_INFO<<"Connect  master disconnect";
	}
}



void xReplication::connErrorCallBack()
{
	sleep(2);
	LOG_WARN<<"reconnecting master";
	client->connect();
	
}

void xReplication::replicationSetMaster(xRedis * redis,rObj* obj, long  port)
{
	this->redis = redis;
	threads = std::shared_ptr<std::thread>(new std::thread(std::bind(&xReplication::connectMaster,this,obj->ptr,port)));
	std::unique_lock<std::mutex> lk(mutex);
	while (start == false)
	{
		condition.wait(lk);
	}
		
}


void replicationFeedSlaves(std::string &commond,xRedis * redis ,std::vector<rObj*> robjs,xTcpconnectionPtr & conn)
{

	xBuffer sendBuf;
	{
		std::string str = "*" + std::to_string(robjs.size() + 1) + "\r\n";
		sendBuf.append(str.c_str(),str.length());
		std::string str1 = "$" + std::to_string(commond.length()) + "\r\n";
		sendBuf.append(str1.c_str(),str.length());
		std::string str2  = commond + "\r\n";
		sendBuf.append(str2.c_str(),str2.length());
	}

	for(auto it = robjs.begin(); it != robjs.end(); it++)
	{
		std::string str = "$" + std::to_string(sdsllen((*it)->ptr)) + "\r\n";
		sendBuf.append(str.c_str(),str.length());
		std::string str1 = (*it)->ptr;
		str1 += "\r\n";
		sendBuf.append(str1.c_str(),str1.length());
	}
	
	if(sendBuf.readableBytes() > 0)
	{
		conn->send(&sendBuf);	
	}
	
}


