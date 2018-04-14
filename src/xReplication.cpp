#include "xReplication.h"
#include "xRedis.h"
#include "xLog.h"

xReplication::xReplication(xRedis *redis)
:redis(redis),
 port(0),
 salveLen(0),
 salveReadLen(0),
 slaveSyncEnabled(false),
 timer(nullptr),
 client(nullptr)
 {

 }

xReplication::~xReplication()
{

}

void xReplication::connectMaster()
{
	xEventLoop loop;
	this->loop = &loop;
	loop.run();
}

void xReplication::replicationCron()
{

}

void xReplication::disConnect()
{
	client->disConnect();
}

void xReplication::syncWrite(const TcpConnectionPtr &conn)
{
	conn->send("sync\r\n",6);
	fp = redis->getRdb()->createFile();
	if(fp == nullptr)
	{
		conn->forceClose();
	}
}

void xReplication::syncWithMaster(const TcpConnectionPtr &conn)
{
	int32_t sockerr = 0;
   	socklen_t errlen = sizeof(sockerr);
	/* Check for errors in the socket. */
	if (getsockopt(conn->getSockfd(),SOL_SOCKET,SO_ERROR,&sockerr,&errlen) == -1)
	   sockerr = errno;
	if (sockerr) 
	{
	   LOG_WARN<<"error condition on socket for sync"<< strerror(sockerr);
	   return ;
	}
	
	syncWrite(conn);	
	
}

void xReplication::readCallBack(const TcpConnectionPtr &conn, xBuffer *buffer)
{
	while (buffer->readableBytes() >= 4)
	{
		if (salveLen == 0)
		{
			salveLen = *(int32_t*)(buffer->peek());
			if (salveLen >= INT_MAX || salveLen <= 0)
			{
				redis->getRdb()->closeFile(fp);
				conn->forceClose();
				LOG_WARN << "length is too large";
				break;
			}

			buffer->retrieveInt32();
			if (buffer->readableBytes() == 0)
			{
				break;
			}
		}

		redis->getRdb()->rdbSyncWrite(buffer->peek(),fp,buffer->readableBytes());
		salveReadLen += buffer->readableBytes();
		buffer->retrieveAll();

		if (salveReadLen > salveLen)
		{
			redis->getRdb()->closeFile(fp);
			conn->forceClose();
			salveLen = 0;
			LOG_WARN << "slave read data failure";
		}
		if (salveLen == salveReadLen)
		{
			redis->getRdb()->rdbSyncClose(REDIS_DEFAULT_RDB_FILENGTHAME,fp);
			redis->clearCommand();

			if (redis->getRdb()->rdbLoad(REDIS_DEFAULT_RDB_FILENGTHAME) == REDIS_OK)
			{
				salveLen = 0;
				{
					std::shared_ptr<xSession> session(new xSession(redis,conn));
					std::unique_lock <std::mutex> lck(redis->getMutex());
					auto &sessions = redis->getSession();
					sessions[conn->getSockfd()] = session;
				}

				conn->send(redis->getObject()->ok->ptr,sdslen(redis->getObject()->ok->ptr));
				LOG_INFO << "replication load rdb success";
			}
			else
			{
				redis->getRdb()->closeFile(fp);
				conn->forceClose();
				salveLen = 0;
				LOG_INFO << "replication load rdb failure";
			}
		}
	}
}

void xReplication::slaveCallBack(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	while(buffer->readableBytes() >= sdslen(redis->getObject()->ok->ptr))
	{
		std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
		auto &salveConn = redis->getSlaveConn();
		auto it = salveConn.find(conn->getSockfd());
		if (it != salveConn.end())
		{
			if (memcmp(buffer->peek(),redis->getObject()->ok->ptr,sdslen(redis->getObject()->ok->ptr)) == 0)
			{
				buffer->retrieve(sdslen(redis->getObject()->ok->ptr));
				if (++redis->salveCount >= salveConn.size())
				{
					if (redis->slaveCached.readableBytes() > 0)
					{
						conn->send(&redis->slaveCached);
						xBuffer buffer;
						redis->slaveCached.swap(buffer);
					}

					auto &repliTimer = redis->getRepliTimer();
					auto iter = repliTimer.find(conn->getSockfd());
					assert(iter != repliTimer.end());
					assert(iter->second != nullptr);
					conn->getLoop()->cancelAfter(iter->second);
					repliTimer.erase(conn->getSockfd());

					{
						std::shared_ptr<xSession> session(new xSession(redis,conn));
						auto &sessions = redis->getSession();
						std::unique_lock <std::mutex> lck(redis->getMutex());
						sessions[conn->getSockfd()] = session;
					}

					LOG_INFO << "slaveof sync success";
				}
			}
		}
	}
}

void xReplication::connCallBack(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		repliConn = conn;
		char buf[64] = "";
		uint16_t port = 0;
		auto addr = socket.getPeerAddr(conn->getSockfd());
		socket.toIp(buf,sizeof(buf),(const  struct sockaddr *)&addr);
		socket.toPort(&port,(const struct sockaddr *)&addr);
		conn->setip(buf);
		conn->setport(port);
		redis->masterHost = conn->getip();
		redis->masterPort = conn->getport();
		redis->masterfd = conn->getSockfd();
		redis->slaveEnabled = true;
		redis->repliEnabled = true;
		syncWithMaster(conn);
		LOG_INFO<<"connect master success";
	}
	else
	{
		repliConn = nullptr;
		salveReadLen = 0;
		salveLen = 0;
		redis->masterHost.clear();
		redis->masterPort = 0;
		redis->masterfd = 0;
		redis->slaveEnabled = false;
		redis->repliEnabled = false;
		std::unique_lock <std::mutex> lck(redis->getMutex());
		auto &sessions = redis->getSession();
		sessions.erase(conn->getSockfd());
		client.reset();
		LOG_INFO<<"connect  master disconnect";
	}
}

void xReplication::reconnectTimer(const std::any &context)
{
	client->asyncConnect();
}

void xReplication::replicationSetMaster(rObj *obj,int16_t port)
{
	this->ip = obj->ptr;
	this->port = port;

	if(redis->repliEnabled)
	{
		std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
		auto &slaveConns = redis->getSlaveConn();
		for (auto &it : slaveConns)
		{
			it.second->forceClose();
		}
	}

	TcpClientPtr client(new xTcpClient(loop,ip.c_str(),port,this));
	client->setConnectionCallback(std::bind(&xReplication::connCallBack,this,std::placeholders::_1));
	client->setMessageCallback(std::bind(&xReplication::readCallBack,this,std::placeholders::_1,std::placeholders::_2));
	client->asyncConnect();
	this->client = client;
}

