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
		recvBuf->retrieveAll();
		
	}
	
}


void xReplication::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{	
		struct sockaddr_in sa;
		socklen_t len = sizeof(sa);
		if(!getpeername(conn->getSockfd(), (struct sockaddr *)&sa, &len))
		{
			//LOG_INFO<<"From slave:"<<inet_ntoa(sa.sin_addr)<<":"<<ntohs(sa.sin_port);
		}
		
		conn->host = inet_ntoa(sa.sin_addr);
		conn->port = ntohs(sa.sin_port);
		redis->masterHost = conn->host;
		redis->masterPort = conn->port ;
		redis->slaveEnabled =  true;
		LOG_INFO<<"Connect master success";
		syncWithMaster(conn);
		if(sendBuf.readableBytes() > 0 )
		{
			conn->send(&sendBuf);	
		}

	}
	else
	{
		redis->slaveEnabled =  false;
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


void replicationFeedSlaves(xBuffer &  sendBuf,rObj * commond ,xRedis * redis ,std::deque<rObj*>  &robjs,xTcpconnectionPtr & conn)
{
	int len, j;
	char buf[32];
	buf[0] = '*';
	len = 1 + ll2string(buf+1,sizeof(buf)-1,robjs.size() + 1);
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

	
	for(auto it = robjs.begin(); it != robjs.end(); it++)
	{
		buf[0] = '$';
		len = 1 + ll2string(buf+1,sizeof(buf)-1,sdsllen((*it)->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		sendBuf.append(buf,len);
		sendBuf.append((*it)->ptr,sdsllen((*it)->ptr));
		sendBuf.append("\r\n",2);
	}

	//conn->send(&sendBuf);

	
}


