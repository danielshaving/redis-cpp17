#include "xReplication.h"
#include "xRedis.h"
#include "xLog.h"

xReplication::xReplication()
:start(false),
 isreconnect(true),
 port(0),
 salveLen(0),
 salveReadLen(0),
 slaveSyncEnabled(false)
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



void xReplication::syncWrite(const xTcpconnectionPtr& conn)
{
	sendBuf.append(shared.sync->ptr,sdsllen(shared.sync->ptr));
	conn->send(&sendBuf);
	sendBuf.retrieveAll();
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
			redis->clearCommond();

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

			xBuffer sendbuffer;
			sendbuffer.append(shared.ok->ptr,sdsllen(shared.ok->ptr));
			conn->send(&sendbuffer);
			recvBuf->retrieveAll();
			xBuffer buffer;
			recvBuf->swap(buffer);
			std::shared_ptr<xSession> session (new xSession(redis,conn));
			MutexLockGuard mu(redis->mutex);
			redis->sessions[conn->getSockfd()] = session;
	
		}
		

	}
	
}


void xReplication::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		socket.getpeerName(conn->getSockfd(),conn->host,conn->port);
		redis->masterHost = conn->host;
		redis->masterPort = conn->port ;
		redis->slaveEnabled =  true;
		redis->repliEnabled = true;
		isreconnect = true;
		LOG_INFO<<"Connect master success";
		syncWithMaster(conn);
	}
	else
	{
		slaveSyncEnabled = false;
		salveReadLen = 0;
		salveLen = 0;
		redis->masterHost.clear();
		redis->masterPort = 0;
		redis->slaveEnabled =  false;
		redis->repliEnabled = false;
		MutexLockGuard mu(redis->mutex);
		redis->sessions.erase(conn->getSockfd());
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
		MutexLockGuard mu(redis->slaveMutex);
		for(auto it = redis->tcpconnMaps.begin(); it != redis->tcpconnMaps.end(); it ++)
		{
			it->second->forceClose();
		}
	}

	client->connect(this->ip.c_str(),this->port);
}

void replicationFeedSlaves(xBuffer &  sendBuf,rObj * commond  ,std::deque<rObj*>  &robjs)
{
	int len, j;
	char buf[32];
	buf[0] = '*';
	len =1 +  ll2string(buf+1,sizeof(buf)-1,robjs.size());
	buf[len++] = '\r';
	buf[len++] = '\n';
	sendBuf.append(buf,len);

	buf[0] = '$';
	len =1 +  ll2string(buf+1,sizeof(buf)-1,sdsllen(commond->ptr));
	buf[len++] = '\r';
	buf[len++] = '\n';
	sendBuf.append(buf,len);
	sendBuf.append(commond->ptr,sdsllen(commond->ptr));
	sendBuf.append("\r\n",2);
	
	for(int i = 1;  i < robjs.size() ; i ++)
	{
		buf[0] = '$';
		len = 1 + ll2string(buf+1,sizeof(buf)-1,sdsllen(robjs[i]->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		sendBuf.append(buf,len);
		sendBuf.append(robjs[i]->ptr,sdsllen(robjs[i]->ptr));
		sendBuf.append("\r\n",2);
	}
}


