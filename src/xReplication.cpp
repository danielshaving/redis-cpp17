#include "xReplication.h"
#include "xRedis.h"
#include "xLog.h"

xReplication::xReplication()
:start(false),
 isreconnect(true),
 port(0),
 salveLen(0),
 salveReadLen(0),
 slaveSyncEnabled(false),
 timer(nullptr)
 {

 }

xReplication::~xReplication()
{
	client->disconnect();
	loop->quit();

}

void xReplication::connectMaster()
{
	start = true;
	xEventLoop loop;
	xTcpClient client(&loop,this);
	client.setConnectionCallback(std::bind(&xReplication::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	client.setMessageCallback( std::bind(&xReplication::readCallBack, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
	client.setConnectionErrorCallBack(std::bind(&xReplication::connErrorCallBack, this));
	this->client = & client;
	this->loop = &loop;
	loop.run();
}


void xReplication::replicationCron()
{

}



void xReplication::handleTimer(void * data)
{
	if(conn)
	{
		if(!redis->pingPong)
		{
			LOG_WARN<<"ping pong handleTimer timeout";
		}
		else
		{
			LOG_INFO<<"ping ping ";	
		}
		
		conn->send(stringPiepe(shared.pping->ptr,sdsllen(shared.pping->ptr)));
	}
}

void xReplication::syncWrite(const xTcpconnectionPtr& conn)
{
	conn->send(stringPiepe(shared.sync->ptr,sdsllen(shared.sync->ptr)));
	fp = createFile();
	if(fp == nullptr)
	{
		conn->forceClose();
	}
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
	while(recvBuf->readableBytes() >= 4)
	{
		if(!slaveSyncEnabled)
		{
			salveLen = *(int32_t*)(recvBuf->peek());
			if(salveLen >= INT_MAX || salveLen<=0)
			{
				isreconnect = false;
				LOG_WARN<<"Length is too large";
				closeFile(fp);
				conn->forceClose();
				break;
			}
			slaveSyncEnabled = true;
			recvBuf->retrieveInt32();
			if(recvBuf->readableBytes() == 0)
			{
				break;
			}
		}

		
		char fileName[] = "dump.rdb";
		rdbSyncWrite(recvBuf->peek(),fp,recvBuf->readableBytes());
		salveReadLen += recvBuf->readableBytes();
		recvBuf->retrieveAll();

		if(salveReadLen > salveLen)
		{
			LOG_WARN<<"slave read data failure";
			closeFile(fp);
			conn->forceClose();
			
		}

		if(salveLen == salveReadLen)
		{
			LOG_INFO<<"slave read data sucess";
			rdbSyncClose(fileName,fp);
			redis->clearCommand();

			if(rdbLoad(fileName,redis) == REDIS_OK)
			{
				LOG_INFO<<"Replication load rdb success";
			}
			else
			{
				closeFile(fp);
				conn->forceClose();
				LOG_INFO<<"Replication load rdb failure";
				return ;
			}

			conn->send(stringPiepe(shared.ok->ptr,sdsllen(shared.ok->ptr)));
			std::shared_ptr<xSession> session (new xSession(redis,conn));
			std::unique_lock <std::mutex> lck(redis->mtx);
			redis->sessions[conn->getSockfd()] = session;
			conn->send(stringPiepe(shared.pping->ptr,sdsllen(shared.pping->ptr)));
			timer = loop->runAfter(1,nullptr,true,std::bind(&xReplication::handleTimer,this,std::placeholders::_1));
	
		}
		

	}
	
}


void xReplication::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		this->conn = conn;
		socket.getpeerName(conn->getSockfd(),&(conn->host),conn->port);
		redis->masterHost = conn->host.c_str();
		redis->masterPort = conn->port ;
		redis->slaveEnabled =  true;
		redis->repliEnabled = true;
		isreconnect = true;
		LOG_INFO<<"Connect master success";
		syncWithMaster(conn);
	}
	else
	{
		this->conn = nullptr;
		slaveSyncEnabled = false;
		salveReadLen = 0;
		salveLen = 0;
		redis->masterHost.clear();
		redis->masterPort = 0;
		redis->slaveEnabled =  false;
		redis->repliEnabled = false;
		std::unique_lock <std::mutex> lck(redis->mtx);
		redis->sessions.erase(conn->getSockfd());
		
		if(timer)
		{
			loop->cancelAfter(timer);
		}
		
		LOG_INFO<<"Connect  master disconnect";
	}
}

void xReplication::reconnectTimer(void * data)
{
	LOG_INFO<<"Reconnect..........";
	client->connect(ip.c_str(),port);
}

void xReplication::connErrorCallBack()
{
	return;

	/*
	if(!isreconnect)
	{
		return ;
	}
	
	if(connectCount >= REDIS_RECONNECT_COUNT)
	{
		LOG_WARN<<"Reconnect failure";
		ip.clear();
		port = 0;
		isreconnect = true;
		return ;
	}
	
	++connectCount;
	loop->runAfter(5,nullptr,false,std::bind(&xReplication::reconnectTimer,this,std::placeholders::_1));
	*/
	
}

void xReplication::replicationSetMaster(xRedis * redis,rObj * obj,int32_t port)
{
	this->ip = obj->ptr;
	this->port = port;
	this->redis = redis;

	if(redis->repliEnabled)
	{
		std::unique_lock <std::mutex> lck(redis->slaveMutex);
		for(auto it = redis->salvetcpconnMaps.begin(); it != redis->salvetcpconnMaps.end(); it ++)
		{
			it->second->forceClose();
		}
	}

	client->connect(this->ip.c_str(),this->port);
}

