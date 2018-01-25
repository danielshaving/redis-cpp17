#include "xReplication.h"
#include "xRedis.h"
#include "xLog.h"

xReplication::xReplication(xRedis * redis)
:redis(redis),
start(false),
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



void xReplication::disconnect()
{
	client->disconnect();
}


void xReplication::syncWrite(const xTcpconnectionPtr& conn)
{
	conn->send("sync\r\n",6);
	fp = redis->rdb.createFile();
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
	   LOG_WARN<<"error condition on socket for sync"<< strerror(sockerr);
	   return ;
	}
	
	syncWrite(conn);	
	
}

void xReplication::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
{
	while (recvBuf->readableBytes() >= 4)
	{
		if (salveLen == 0)
		{
			salveLen = *(int32_t*)(recvBuf->peek());
			if (salveLen >= INT_MAX || salveLen <= 0)
			{
				redis->rdb.closeFile(fp);
				conn->forceClose();
				LOG_WARN << "length is too large";
				break;
			}

			recvBuf->retrieveInt32();
			if (recvBuf->readableBytes() == 0)
			{
				break;
			}
		}
		char fileName[] = "dump.rdb";
		redis->rdb.rdbSyncWrite(recvBuf->peek(), fp, recvBuf->readableBytes());
		salveReadLen += recvBuf->readableBytes();
		recvBuf->retrieveAll();

		if (salveReadLen > salveLen)
		{
			redis->rdb.closeFile(fp);
			conn->forceClose();
			salveLen = 0;
			LOG_WARN << "slave read data failure";
		}
		if (salveLen == salveReadLen)
		{
			redis->rdb.rdbSyncClose(fileName, fp);
			redis->clearCommand();

			if (redis->rdb.rdbLoad(fileName) == REDIS_OK)
			{
				salveLen = 0;
				{
                    std::shared_ptr<xSession> session(new xSession(redis, conn));
                    std::unique_lock <std::mutex> lck(redis->mtx);
                    redis->sessions[conn->getSockfd()] = session;
                }
		    	conn->send(shared.ok->ptr, sdslen(shared.ok->ptr));
		    	LOG_INFO << "replication load rdb success";
			}
			else
			{
				redis->rdb.closeFile(fp);
				conn->forceClose();
				salveLen = 0;
				LOG_INFO << "replication load rdb failure";
			}
		}
	}
	
}


void xReplication::slaveCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf, void *data)
{
	while(recvBuf->readableBytes() >= sdslen(shared.ok->ptr))
	{
		std::unique_lock <std::mutex> lck(redis->slaveMutex);
		auto it = redis->salvetcpconnMaps.find(conn->getSockfd());
		if (it != redis->salvetcpconnMaps.end())
		{
			if (memcmp(recvBuf->peek(), shared.ok->ptr, sdslen(shared.ok->ptr)) == 0)
			{
				recvBuf->retrieve(sdslen(shared.ok->ptr));
				if (++redis->salveCount >= redis->salvetcpconnMaps.size())
				{
					if (redis->slaveCached.readableBytes() > 0)
					{
						conn->send(&redis->slaveCached);
						xBuffer buffer;
						redis->slaveCached.swap(buffer);
					}

					auto iter = redis->repliTimers.find(conn->getSockfd());
					assert(iter != redis->repliTimers.end());
					assert(iter->second != nullptr);
					conn->getLoop()->cancelAfter(iter->second);

					redis->repliTimers.erase(conn->getSockfd());

					{
						std::shared_ptr<xSession> session(new xSession(redis, conn));
						std::unique_lock <std::mutex> lck(redis->mtx);;
						redis->sessions[conn->getSockfd()] = session;
					}

					LOG_INFO << "slaveof sync success";
				}


			}
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
		redis->masterfd = conn->getSockfd();
		redis->slaveEnabled = true;
		redis->repliEnabled = true;
		isreconnect = true;
		syncWithMaster(conn);
		LOG_INFO<<"connect master success";
	}
	else
	{
		this->conn = nullptr;
		salveReadLen = 0;
		salveLen = 0;
		redis->masterHost.clear();
		redis->masterPort = 0;
		redis->masterfd = 0;
		redis->slaveEnabled = false;
		redis->repliEnabled = false;
		std::unique_lock <std::mutex> lck(redis->mtx);
		redis->sessions.erase(conn->getSockfd());
		LOG_INFO<<"connect  master disconnect";
	}
}

void xReplication::reconnectTimer(void * data)
{
	LOG_INFO<<"reconnect..........";
	client->connect(ip.c_str(),port);
}

void xReplication::connErrorCallBack()
{
	return;

}

void xReplication::replicationSetMaster(rObj * obj,int32_t port)
{
	this->ip = obj->ptr;
	this->port = port;

	if(redis->repliEnabled)
	{
		std::unique_lock <std::mutex> lck(redis->slaveMutex);
		for(auto it = redis->salvetcpconnMaps.begin(); it != redis->salvetcpconnMaps.end(); ++it)
		{
			it->second->forceClose();
		}
	}

	client->connect(this->ip.c_str(),this->port);
}

