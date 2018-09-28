#include "redisproxy.h"
#include "socket.h"

RedisProxy::RedisProxy(const char *ip, int16_t port, const char *redisIp, int16_t redisPort,
	int16_t threadCount, int16_t sessionCount)
	:server(&loop, ip, port, nullptr),
	ip(ip),
	port(port),
	redisIp(redisIp),
	redisPort(redisPort),
	threadCount(threadCount),
	sessionCount(sessionCount)
{	
	initRedisPorxy();
	initRedisAsync();
	initRedisSync();
	initRedisCommand();	
	initRedisTimer();
}

RedisProxy::~RedisProxy()
{
	sessions.clear();
	threadHiredis.clear();
	threadRedisContexts.clear();
}

void RedisProxy::initRedisPorxy()
{
	server.setThreadNum(threadCount);
	server.setConnectionCallback(std::bind(&RedisProxy::proxyConnCallback,
		this, std::placeholders::_1));
	server.start();
}

void RedisProxy::initRedisAsync()
{
	auto pools = server.getThreadPool()->getAllLoops();
	for (int i = 0; i < pools.size(); i++)
	{
		auto it = threadHiredis.find(pools[i]->getThreadId());
		assert (it == threadHiredis.end());
		std::shared_ptr<Hiredis> hiredis(new Hiredis(pools[i], sessionCount, redisIp, redisPort, true));
		hiredis->setConnectionCallback(std::bind(&RedisProxy::redisConnCallback,
			this, std::placeholders::_1));
		hiredis->setDisconnectionCallback(std::bind(&RedisProxy::redisDisconnCallback,
			this, std::placeholders::_1));
		hiredis->start(pools[i], 0);
		threadHiredis[pools[i]->getThreadId()] = hiredis;
	}
}

void RedisProxy::initRedisSync()
{
	auto pools = server.getThreadPool()->getAllLoops();
	for (int i = 0; i < pools.size(); i++)
	{
		RedisContextPtr c = redisConnect(redisIp, redisPort);
		if (c == nullptr || c->err)
		{
			if (c) 
			{ 
				LOG_WARN << "Connection error: " << c->errstr; 
			}
			else 
			{
				LOG_WARN << "Connection error: can't allocate redis context";
			}
			exit(1);
		}

		struct timeval tv = { 5, 0 };
		assert (Socket::setTimeOut(c->fd, tv));
		auto it = threadRedisContexts.find(pools[i]->getThreadId());
		if (it == threadRedisContexts.end())
		{
			std::vector<RedisContextPtr> context;
			context.push_back(c);
			threadRedisContexts[pools[i]->getThreadId()] = std::move(context);
		}
		else
		{
			it->second.push_back(c);
		}
	}
}

void RedisProxy::initRedisTimer()
{
	auto pools = server.getThreadPool()->getAllLoops();
	for (int i = 0; i < pools.size(); i++)
	{
		pools[i]->runAfter(1.0, true, std::bind(&RedisProxy::redisContextTimer, this));
		auto it = threadHiredis.find(pools[i]->getThreadId());
		assert(it != threadHiredis.end());
		pools[i]->runAfter(1.0, true, std::bind(&Hiredis::clusterNodeTimer, it->second.get()));
	}
}

void RedisProxy::initRedisCommand()
{
	shared.hgetall = createObject(REDIS_STRING, sdsnew("hgetall"));
	redisCommands[shared.hgetall] = std::bind(&RedisProxy::hgetallCommand,
		this, std::placeholders::_1, std::placeholders::_2);
}

bool RedisProxy::getRedisCommand(const RedisObjectPtr &command)
{
	auto it = redisCommands.find(command);
	if (it != redisCommands.end())
	{
		return true;
	}
	return false;
}

bool RedisProxy::handleRedisCommand(const RedisObjectPtr &command, 
	const ProxySessionPtr &session, const std::deque<RedisObjectPtr> &objs)
{
	auto it = redisCommands.find(command);
	if (it != redisCommands.end())
	{
		if (!it->second(objs, session))
		{
			return false;
		}
	}
	return true;
}

void RedisProxy::redisContextTimer()
{
	auto it = threadRedisContexts.find(std::this_thread::get_id());
	assert(it != threadRedisContexts.end());
	assert(!it->second.empty());
	for (auto &iter : it->second)
	{
		RedisReplyPtr reply = iter->redisCommand("PING");
		if (reply == nullptr)
		{
			LOG_WARN << "PING connection error: "<< iter->errstr; 
			if (iter->redisReconnect() == REDIS_OK)
			{
				struct timeval tv = { 5, 0 };
				assert (Socket::setTimeOut(iter->fd, tv));
			}
		}
	}
}

void RedisProxy::run()
{
	loop.run();
}

void RedisProxy::redisConnCallback(const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
}

void RedisProxy::redisDisconnCallback(const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
}

void RedisProxy::processCommand(const TcpConnectionPtr &conn, const char *buf, size_t len)
{
	conn->getLoop()->assertInLoopThread();
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());
	auto redis = it->second->getRedisAsyncContext(
		conn->getLoop()->getThreadId(), conn->getSockfd());
	if (redis == nullptr)
	{
		Buffer *buffer = conn->outputBuffer();
		std::string reply = it->second->getTcpClientInfo(conn->getLoop()->getThreadId(), conn->getSockfd());
		buffer->append(reply.c_str(), reply.size());
		conn->sendPipe();
	}
	else
	{ 
		int32_t status = redis->proxyRedisvAsyncCommand(std::bind(&RedisProxy::proxyCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			buf, len, conn);
		assert(status == REDIS_OK);
	}
}

void RedisProxy::proxyCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(privdata.has_value());
	const TcpConnectionPtr &conn = std::any_cast<TcpConnectionPtr>(privdata);
	assert(conn != nullptr);
	conn->getLoop()->assertInLoopThread();
	Buffer *buffer = conn->outputBuffer();
	if (reply == nullptr)
	{
		LOG_WARN << "proxyCallback err: " << c->errstr;
		auto it = threadHiredis.find(conn->getLoop()->getThreadId());
		assert(it != threadHiredis.end());
		std::string r = it->second->getTcpClientInfo(conn->getLoop()->getThreadId(), conn->getSockfd());
		buffer->append(r.c_str(), r.size());
	}
	else
	{
		assert(!reply->view.empty());
		buffer->append(reply->view.data(), reply->view.size());
	}
	conn->sendPipe();
}

void RedisProxy::highWaterCallBack(const TcpConnectionPtr &conn, size_t bytesToSent)
{
	LOG_INFO << " bytes " << bytesToSent;
	conn->getLoop()->assertInLoopThread();
	if (conn->outputBuffer()->readableBytes() > 0)
	{
		conn->stopRead();
		conn->setWriteCompleteCallback(
			std::bind(&RedisProxy::writeCompleteCallBack, this, std::placeholders::_1));
	}
}

void RedisProxy::writeCompleteCallBack(const TcpConnectionPtr &conn)
{
	conn->startRead();
	conn->setWriteCompleteCallback(WriteCompleteCallback());
}

void RedisProxy::proxyConnCallback(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		Socket::setkeepAlive(conn->getSockfd(), kHeart);
		conn->setHighWaterMarkCallback(
				std::bind(&RedisProxy::highWaterCallBack,
				this, std::placeholders::_1, std::placeholders::_2),
				kHighWaterBytes);

		char buf[64] = "";
		uint16_t port = 0;
		auto addr = Socket::getPeerAddr(conn->getSockfd());
		Socket::toIp(buf, sizeof(buf), (const struct sockaddr *)&addr);
		Socket::toPort(&port, (const struct sockaddr *)&addr);
		ProxySessionPtr session(new ProxySession(this, conn));
		std::unique_lock <std::mutex> lck(mutex);
		auto it = sessions.find(conn->getSockfd());
		assert(it == sessions.end());
		sessions[conn->getSockfd()] = session;
		//LOG_INFO << "Client connect success " << buf << " " << port << " " << conn->getSockfd();
	}
	else
	{
		std::unique_lock <std::mutex> lck(mutex);
		auto it = sessions.find(conn->getSockfd());
		assert(it != sessions.end());
		sessions.erase(it);
		//LOG_INFO << "Client diconnect " << conn->getSockfd();
	}
}

bool RedisProxy::hgetallCommand(const std::deque<RedisObjectPtr> &obj, const ProxySessionPtr &session)
{
	return true;
}
