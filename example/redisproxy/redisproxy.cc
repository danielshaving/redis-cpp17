#include "redisproxy.h"
#include "socket.h"

struct Message
{
	int64_t proxyCount;
	WeakTcpConnectionPtr conn;
};

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
		assert(it == threadHiredis.end());
		
		std::shared_ptr<Hiredis> hiredis(new Hiredis(pools[i], sessionCount, redisIp, redisPort, true));
		hiredis->setConnectionCallback(std::bind(&RedisProxy::redisConnCallback,
			this, std::placeholders::_1));
		hiredis->setDisconnectionCallback(std::bind(&RedisProxy::redisDisconnCallback,
			this, std::placeholders::_1));
		hiredis->start(pools[i], 0);
		threadHiredis[pools[i]->getThreadId()] = hiredis;
		
		std::unordered_map<int32_t,std::map<int64_t, RedisReplyPtr>> umaps;
		proxyReplys[pools[i]->getThreadId()] = umaps;
		
		std::unordered_map<int32_t, std::set<int64_t>> sets;
		proxySends[pools[i]->getThreadId()] = sets;
		
		std::unordered_map<int32_t, int64_t> maps;
		proxyCounts[pools[i]->getThreadId()] = maps;
		
		std::unordered_map<int32_t, std::unordered_set<int32_t>> vmaps;
		proxyRedis[pools[i]->getThreadId()] = vmaps;
	}
}

void RedisProxy::initRedisTimer()
{
	auto pools = server.getThreadPool()->getAllLoops();
	for (int i = 0; i < pools.size(); i++)
	{
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
	clearProxyRedis(conn->getLoop()->getThreadId(), conn->getSockfd());
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
		clearProxyReply(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearProxySend(conn->getLoop()->getThreadId(), conn->getSockfd());
		
		std::string reply = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->sendPipe(reply.c_str(), reply.size());
	}
	else
	{ 	
		int64_t proxyCount = 0;
		{
			auto iter = proxyCounts.find(conn->getLoop()->getThreadId());
			assert(iter != proxyCounts.end());
			auto iterr = iter->second.find(conn->getSockfd());
			if (iterr == iter->second.end())
			{
				proxyCount = 1;
				iter->second.insert(std::make_pair(conn->getSockfd(), proxyCount));
			}
			else
			{
				proxyCount = ++iterr->second;
			}
		}

		{
			auto iter = proxySends.find(conn->getLoop()->getThreadId());
			assert(iter != proxySends.end());
			auto iterr = iter->second.find(conn->getSockfd());
			if (iterr == iter->second.end())
			{
				std::set<int64_t> sets;
				sets.insert(proxyCount);
				iter->second.insert(std::make_pair(conn->getSockfd(), sets));
			}
			else
			{
				iterr->second.insert(proxyCount);
			}
		}

		Message message;
		message.conn = conn;
		message.proxyCount = proxyCount;
		
		int32_t status = redis->proxyRedisvAsyncCommand(std::bind(&RedisProxy::proxyCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			buf, len, message);
		assert(status == REDIS_OK);
	}
}

void RedisProxy::proxyCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(privdata.has_value());
	Message message = std::any_cast<Message>(privdata);
	int64_t proxyCount = message.proxyCount;
	WeakTcpConnectionPtr weakConn = message.conn;
	TcpConnectionPtr conn = weakConn.lock();
	if (conn == nullptr)
	{
		return ;
	}
	
	conn->getLoop()->assertInLoopThread();
	Buffer *buffer = conn->outputBuffer();

	auto it = proxySends.find(conn->getLoop()->getThreadId());
	assert(it != proxySends.end());

	auto iter = it->second.find(conn->getSockfd());
	if (iter == it->second.end())
	{
		return ;
	}

	auto iterr = iter->second.find(proxyCount);
	assert(iterr != iter->second.end());

	int64_t beginProxyCount = *(iter->second.begin());
	if (beginProxyCount == proxyCount)
	{
		iter->second.erase(iterr);
	}

	if (reply == nullptr)
	{
		LOG_WARN << "proxyCallback err: " << c->errstr;
		auto it = threadHiredis.find(conn->getLoop()->getThreadId());
		assert(it != threadHiredis.end());
		std::string r = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		buffer->append(r.c_str(), r.size());
		conn->sendPipe();
		return ;
	}
	
	if (reply->type == REDIS_REPLY_PROXY)
	{
		clearProxyReply(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearProxySend(conn->getLoop()->getThreadId(), conn->getSockfd());
		
		buffer->append(reply->buffer, sdslen(reply->buffer));
		conn->sendPipe();
		return ;
	}
	
	{
		auto it = proxyReplys.find(conn->getLoop()->getThreadId());
		assert(it != proxyReplys.end());
		auto iter = it->second.find(conn->getSockfd());
		if (iter != it->second.end())
		{
			iter->second.insert(std::make_pair(proxyCount, reply));
			for (auto iterr = iter->second.begin(); iterr != iter->second.end();)
			{
				if (beginProxyCount++ == iterr->first)
				{
					{
						auto it = proxySends.find(conn->getLoop()->getThreadId());
						assert(it != proxySends.end());

						auto iter = it->second.find(conn->getSockfd());
						assert (iter != it->second.end());
						iter->second.erase(iterr->first);
					}
					
					const RedisReplyPtr &r = iterr->second;
					buffer->append(r->buffer, sdslen(r->buffer));
					iter->second.erase(iterr++);
					continue;
				}
				else
				{
					break;
				}
			}
		}
		else
		{
			if (beginProxyCount == proxyCount)
			{
				buffer->append(reply->buffer, sdslen(reply->buffer));
			}
			else
			{
				std::map<int64_t, RedisReplyPtr> maps;
				maps.insert(std::make_pair(proxyCount, reply));
				it->second.insert(std::make_pair(conn->getSockfd(), maps));
			}
		}
		conn->sendPipe();
	}
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
		{
			std::unique_lock <std::mutex> lck(mutex);
			auto it = sessions.find(conn->getSockfd());
			assert(it == sessions.end());
			sessions[conn->getSockfd()] = session;
		}
		LOG_INFO << "Client connect success " << buf << " " << port << " " << conn->getSockfd();
	}
	else
	{
		{
			std::unique_lock <std::mutex> lck(mutex);
			auto it = sessions.find(conn->getSockfd());
			assert(it != sessions.end());
			sessions.erase(it);
		}
		
		clearProxyReply(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearProxySend(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearProxyRedisClient(conn->getLoop()->getThreadId(), conn->getSockfd());
		LOG_INFO << "Client diconnect " << conn->getSockfd();
	}
}

bool RedisProxy::hgetallCommand(const std::deque<RedisObjectPtr> &obj, const ProxySessionPtr &session)
{
	if (obj.size() >= 2)
	{
		return false;
	}
	return true;
}

void RedisProxy::clearProxyReply(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = proxyReplys.find(threadId);
	assert(it != proxyReplys.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			it->second.erase(iter);
		}
	}
}

void RedisProxy::clearProxyCount(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = proxyCounts.find(threadId);
	assert(it != proxyCounts.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			it->second.erase(iter);
		}
	}
}

void RedisProxy::clearProxySend(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = proxySends.find(threadId);
	assert(it != proxySends.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			it->second.erase(iter);
		}
	}
}

void RedisProxy::clearProxyRedis(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = proxyRedis.find(threadId);
	assert(it != proxyRedis.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			for (auto &iterr : iter->second)
			{
				clearProxyCount(threadId, iterr);
				clearProxySend(threadId, iterr);
				clearProxyReply(threadId, iterr);
			}
			it->second.erase(iter);
		}
	}
}

void RedisProxy::clearProxyRedisClient(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadHiredis.find(threadId);
	assert(it != threadHiredis.end());
	auto redis = it->second->getRedisAsyncContext(threadId, sockfd);
	if (redis != nullptr)
	{
		auto conn = redis->getTcpConnection().lock();
		assert(conn != nullptr);
		
		auto it = proxyRedis.find(threadId);
		assert(it != proxyRedis.end());
		
		if (!it->second.empty())
		{
			auto iter = it->second.find(conn->getSockfd());
			if (iter != it->second.end())
			{
				auto iterr = iter->second.find(sockfd);
				if (iterr != iter->second.end())
				{
					iter->second.erase(iterr);
				}
			}
		}
	}		
}
