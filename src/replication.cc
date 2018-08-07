#include "replication.h"
#include "redis.h"
#include "log.h"

Replication::Replication(Redis *redis)
:redis(redis),
 port(0),
 salveLen(0),
 salveReadLen(0),
 slaveSyncEnabled(false),
 client(nullptr)
{

}

Replication::~Replication()
{

}

void Replication::connectMaster()
{
	EventLoop loop;
	this->loop = &loop;
	loop.run();
}

void Replication::replicationCron()
{

}

void Replication::disConnect()
{
	client->disConnect();
}

void Replication::syncWrite(const TcpConnectionPtr &conn)
{
	FILE *fp;
	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",getpid());
	fp = ::fopen(tmpfile,"w");
	if (!fp)
	{
		LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		return ;
	}
	conn->send("sync\r\n",6);
}

void Replication::syncWithMaster(const TcpConnectionPtr &conn)
{
	int32_t sockerr = 0;
   	socklen_t errlen = sizeof(sockerr);
	/* Check for errors in the socket. */
	if (getsockopt(conn->getSockfd(),SOL_SOCKET,SO_ERROR,&sockerr,&errlen) == -1)
	   sockerr = errno;
	if (sockerr) 
	{
	   LOG_WARN<<"Error condition on socket for sync"<<strerror(sockerr);
	   return ;
	}
	syncWrite(conn);	
}

void Replication::readCallback(const TcpConnectionPtr &conn,Buffer *buffer)
{
	while (buffer->readableBytes() >= 4)
	{
		if (salveLen == 0)
		{
			salveLen = buffer->peekInt32();
			if (salveLen >= INT_MAX || salveLen <= 0)
			{
				::fclose(fp);
				conn->forceClose();
				LOG_WARN << "Length is too large";
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
			::fclose(fp);
			conn->forceClose();
			salveLen = 0;
			LOG_WARN << "Slave read data failure";
		}
		if (salveLen == salveReadLen)
		{
			redis->getRdb()->rdbSyncClose(REDIS_DEFAULT_RDB_FILENGTHAME,fp);
			redis->clearCommand();

			if (redis->getRdb()->rdbLoad(REDIS_DEFAULT_RDB_FILENGTHAME) == REDIS_OK)
			{
				salveLen = 0;
				{
					std::shared_ptr<Session> session(new Session(redis,conn));
					std::unique_lock <std::mutex> lck(redis->getMutex());
					auto &sessions = redis->getSession();
					sessions[conn->getSockfd()] = session;
					auto &sessionConns = redis->getSessionConn();
					sessionConns[conn->getSockfd()] = conn;
				}

				conn->send(shared.ok->ptr,sdslen(shared.ok->ptr));
				LOG_INFO << "Replication load rdb success";
			}
			else
			{
				::fclose(fp);
				conn->forceClose();
				salveLen = 0;
				LOG_INFO << "Replication load rdb failure";
			}
		}
	}
}

void Replication::slaveCallback(const TcpConnectionPtr &conn,Buffer *buffer)
{
	while (buffer->readableBytes() >= sdslen(shared.ok->ptr))
	{
		std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
		auto &salveConn = redis->getSlaveConn();
		auto it = salveConn.find(conn->getSockfd());
		if (it != salveConn.end())
		{
			if (memcmp(buffer->peek(),shared.ok->ptr,sdslen(shared.ok->ptr)) == 0)
			{
				buffer->retrieve(sdslen(shared.ok->ptr));
				if (++redis->salveCount >= salveConn.size())
				{
					if (redis->slaveCached.readableBytes() > 0)
					{
						conn->send(&redis->slaveCached);
						Buffer buffer;
						redis->slaveCached.swap(buffer);
					}

					auto &repliTimer = redis->getRepliTimer();
					auto iter = repliTimer.find(conn->getSockfd());
					assert(iter != repliTimer.end());
					assert(iter->second != nullptr);
					conn->getLoop()->cancelAfter(iter->second);
					repliTimer.erase(conn->getSockfd());

					{
						std::shared_ptr<Session> session(new Session(redis,conn));
						auto &sessions = redis->getSession();
						std::unique_lock<std::mutex> lck(redis->getMutex());
						sessions[conn->getSockfd()] = session;
						auto &sessionConns = redis->getSessionConn();
						sessionConns[conn->getSockfd()] = conn;
					}

					LOG_INFO << "Slaveof sync success";
				}
			}
		}
	}
}

void Replication::connCallback(const TcpConnectionPtr &conn)
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
		redis->clearSessionState(conn->getSockfd());
		LOG_INFO<<"connect master disconnect";
	}
}
 
void Replication::reconnectTimer(const std::any &context)
{
	client->connect();
}

void Replication::replicationSetMaster(const RedisObjectPtr &obj,int16_t port)
{
	this->ip = obj->ptr;
	this->port = port;

	if(redis->repliEnabled)
	{
		std::unique_lock<std::mutex> lck(redis->getSlaveMutex());
		auto &slaveConns = redis->getSlaveConn();
		for (auto &it : slaveConns)
		{
			it.second->forceClose();
		}
	}

	TcpClientPtr client(new TcpClient(loop,ip.c_str(),port,this));
	client->setConnectionCallback(std::bind(&Replication::connCallback,
			this,std::placeholders::_1));
	client->setMessageCallback(std::bind(&Replication::readCallback,
			this,std::placeholders::_1,std::placeholders::_2));
	client->connect();
	this->client = client;
}

