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
	char tmpfile[256];
	snprintf(tmpfile, 256, "temp-%d.rdb", std::this_thread::get_id());
	fp = ::fopen(tmpfile, "w");
	if (!fp)
	{
		LOG_TRACE << "Failed opening .rdb for saving:" << strerror(errno);
		return;
	}
	conn->send("sync\r\n", 6);
}

void Replication::syncWithMaster(const TcpConnectionPtr &conn)
{
	int32_t sockerr = 0;
	socklen_t errlen = sizeof(sockerr);
	/* Check for errors in the Socket:: */
	if (::getsockopt(conn->getSockfd(),
		SOL_SOCKET, SO_ERROR, (char*)&sockerr, &errlen) == REDIS_ERR)
	{
		sockerr = errno;
	}

	if (sockerr)
	{
		LOG_WARN << "Error condition on socket for sync" << strerror(sockerr);
		return;
	}
	syncWrite(conn);
}

void Replication::close()
{
	::fclose(fp);
	salveLen = 0;
	repliConn->forceClose();
}

void Replication::readCallback(const TcpConnectionPtr &conn, Buffer *buffer)
{
	while (buffer->readableBytes() >= sizeof(int32_t))
	{
		if (salveLen == 0)
		{
			salveLen = buffer->peekInt32();
			buffer->retrieveInt32();
			if (buffer->readableBytes() == 0)
			{
				break;
			}
		}

		int32_t status = redis->getRdb()->rdbSyncWrite(buffer->peek(),
			fp, buffer->readableBytes());
		assert(status != REDIS_ERR);

		salveReadLen += buffer->readableBytes();
		buffer->retrieveAll();

		assert(salveReadLen <= salveLen);
		if (salveLen == salveReadLen)
		{
			redis->getRdb()->rdbSyncClose(REDIS_DEFAULT_RDB_FILENGTHAME, fp);
			redis->clearCommand();

			assert(redis->getRdb()->rdbLoad(REDIS_DEFAULT_RDB_FILENGTHAME) != REDIS_ERR);
			std::shared_ptr<Session> session(new Session(redis, conn));
			{
				std::unique_lock <std::mutex> lck(redis->getMutex());
				auto &sessions = redis->getSession();
				sessions[conn->getSockfd()] = session;
				auto &sessionConns = redis->getSessionConn();
				sessionConns[conn->getSockfd()] = conn;
			}

			conn->send(shared.ok->ptr, sdslen(shared.ok->ptr));
			LOG_INFO << "Replication load rdb success";
		}
	}
}

void Replication::slaveCallback(const TcpConnectionPtr &conn, Buffer *buffer)
{
	while (buffer->readableBytes() >= sdslen(shared.ok->ptr))
	{
		std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
		auto &salveConn = redis->getSlaveConn();
		auto it = salveConn.find(conn->getSockfd());
		if (it != salveConn.end())
		{
			if (memcmp(buffer->peek(), shared.ok->ptr, sdslen(shared.ok->ptr)) == 0)
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
						std::shared_ptr<Session> session(new Session(redis, conn));
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
	if (conn->connected())
	{
		repliConn = conn;
		char buf[64] = "";
		uint16_t port = 0;
		salveLen = 0;
		auto addr = Socket::getPeerAddr(conn->getSockfd());
		Socket::toIp(buf, sizeof(buf), (const  struct sockaddr *)&addr);
		Socket::toPort(&port, (const struct sockaddr *)&addr);
		//conn->setip(buf);
		//conn->setport(port);

		redis->masterHost = buf;
		redis->masterPort = port;
		redis->masterfd = conn->getSockfd();
		redis->slaveEnabled = true;
		redis->repliEnabled = true;
		syncWithMaster(conn);
		LOG_INFO << "connect master success";
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
		LOG_INFO << "connect master disconnect";
	}
}

void Replication::reconnectTimer(const std::any &context)
{
	client->connect();
}

void Replication::replicationSetMaster(const RedisObjectPtr &obj, int16_t port)
{
	if (redis->repliEnabled)
	{
		std::unique_lock<std::mutex> lck(redis->getSlaveMutex());
		auto &slaveConns = redis->getSlaveConn();
		for (auto &it : slaveConns)
		{
			it.second->forceClose();
		}
	}

	TcpClientPtr client(new TcpClient(loop, ip.c_str(), port, this));
	client->setConnectionCallback(std::bind(&Replication::connCallback,
		this, std::placeholders::_1));
	client->setMessageCallback(std::bind(&Replication::readCallback,
		this, std::placeholders::_1, std::placeholders::_2));
	client->connect();

	this->ip = obj->ptr;
	this->port = port;
	this->client = client;
}

