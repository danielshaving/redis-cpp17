#include "redisproxy.h"
#include "socket.h"
#include "object.h"

RedisProxy::RedisProxy(const char *ip, int16_t port, const char *redisIp, int16_t redisPort,
	int16_t threadCount, int16_t sessionCount)
	:server(&loop, ip, port, nullptr),
	ip(ip),
	port(port),
	redisIp(redisIp),
	redisPort(redisPort),
	threadCount(threadCount),
	sessionCount(sessionCount),
	clusterEnabled(false)
{
	LOG_INFO << "Proxy initialized";
	initRedisPorxy();
	initRedisAsync();
	initRedisCommand();
	initRedisTimer();
}

RedisProxy::~RedisProxy()
{
	sessions.clear();
	threadHiredis.clear();
	threadProxyReplys.clear();
	threadProxySends.clear();
	threadProxyCounts.clear();
	threadProxyRedis.clear();
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

		{
			std::unordered_map<int32_t, std::map<int64_t, RedisReplyPtr>> m;
			threadProxyReplys[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map<int32_t, std::set<int64_t>> m;
			threadProxySends[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map<int32_t, int64_t> m;
			threadProxyCounts[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map<int32_t, std::unordered_set<int32_t>> m;
			threadProxyRedis[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map<int32_t, std::vector<RedisReplyPtr>> m;
			threadCommandReplys[pools[i]->getThreadId()] = m;
		}

		{
			std::vector<RedisObjectPtr> m;
			threadProxyCommands[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map<int32_t, std::vector<RedisObjectPtr>> m;
			threadPubSubCommands[pools[i]->getThreadId()] = m;
		}
	}
}

void RedisProxy::initRedisTimer()
{
	auto pools = server.getThreadPool()->getAllLoops();
	for (int i = 0; i < pools.size(); i++)
	{
		auto it = threadHiredis.find(pools[i]->getThreadId());
		assert(it != threadHiredis.end());
		if (clusterEnabled)
		{
			pools[i]->runAfter(1.0, true, std::bind(&Hiredis::redisContextTimer, it->second.get()));
		}
	}
}

void RedisProxy::initRedisCommand()
{
	createSharedObjects();
	redisReplyCommands[shared.mget] = std::bind(&RedisProxy::mgetCallback,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	redisReplyCommands[shared.mget] = std::bind(&RedisProxy::delCallback,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
		
	redisCommands[shared.publish] = std::bind(&RedisProxy::debugCommand,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
	redisCommands[shared.subscribe] = std::bind(&RedisProxy::debugCommand,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
	redisCommands[shared.unsubscribe] = std::bind(&RedisProxy::debugCommand,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
	redisCommands[shared.monitor] = std::bind(&RedisProxy::debugCommand,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
	if (clusterEnabled)
	{
		redisCommands[shared.select] = std::bind(&RedisProxy::selectCommand,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
		redisCommands[shared.mget] = std::bind(&RedisProxy::mgetCommand,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
		redisCommands[shared.dbsize] = std::bind(&RedisProxy::dbsizeCommand,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
		redisCommands[shared.flushdb] = std::bind(&RedisProxy::flushdbCommand,
				this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
		redisCommands[shared.del] = std::bind(&RedisProxy::mgetCommand,
				this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
	}
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
	const ProxySessionPtr &session,
	const std::vector<RedisObjectPtr> &commands,
	const TcpConnectionPtr &conn)
{
	auto it = redisCommands.find(command);
	if (it != redisCommands.end())
	{
		if (!it->second(command, commands, session, conn))
		{
			return false;
		}
	}
	return true;
}

void RedisProxy::run()
{
	LOG_INFO << "Ready to accept connections";
	loop.run();
}

void RedisProxy::redisConnCallback(const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
	LOG_INFO << "RedisClient connect redis-server success " << conn->getSockfd();
}

void RedisProxy::redisDisconnCallback(const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
	clearthreadProxyRedis(conn->getLoop()->getThreadId(), conn->getSockfd());
	LOG_INFO << "RedisClient disconnect redis-server " << conn->getSockfd();
}

void RedisProxy::clearProxy(const std::thread::id &threadId, const int32_t sockfd)
{
	clearProxyReply(threadId, sockfd);
	clearProxyCount(threadId, sockfd);
	clearProxySend(threadId, sockfd);
	clearCommandReply(threadId, sockfd);
}

int64_t RedisProxy::insertProxyCount(const std::thread::id &threadId, const int32_t sockfd)
{
	int64_t proxyCount = 0;
	auto it = threadProxyCounts.find(threadId);
	assert(it != threadProxyCounts.end());
	auto iter = it->second.find(sockfd);
	if (iter == it->second.end())
	{
		proxyCount = 1;
		it->second.insert(std::make_pair(sockfd, proxyCount));
	}
	else
	{
		proxyCount = ++iter->second;
	}
	return proxyCount;
}

void RedisProxy::insertProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count)
{
	auto it = threadProxySends.find(threadId);
	assert(it != threadProxySends.end());
	auto iter = it->second.find(sockfd);
	if (iter == it->second.end())
	{
		std::set<int64_t> sets;
		sets.insert(count);
		it->second.insert(std::make_pair(sockfd, sets));
	}
	else
	{
		iter->second.insert(count);
	}
}

void RedisProxy::processCommand(const RedisObjectPtr &command, const TcpConnectionPtr &conn, const char *buf, size_t len)
{
	auto redis = checkReply(command, conn);
	if (redis)
	{	
		int64_t proxyCount = insertProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
		insertProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount);
		RedisCluster proxy;
		proxy.conn = conn;
		proxy.proxyCount = proxyCount;
		proxy.command = nullptr;
		int32_t status = redis->threadProxyRedisvAsyncCommand(std::bind(&RedisProxy::proxyCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			buf, len, proxy);
		assert(status == REDIS_OK);
	}
}

void RedisProxy::proxyCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(privdata.has_value());
	RedisCluster proxy = std::any_cast<RedisCluster>(privdata);

	int64_t proxyCount = proxy.proxyCount;
	int32_t commandCount = proxy.commandCount;
	RedisObjectPtr command = proxy.command;
	WeakTcpConnectionPtr weakConn = proxy.conn;
	TcpConnectionPtr conn = weakConn.lock();
	if (conn == nullptr)
	{	
		return;
	}

	conn->getLoop()->assertInLoopThread();
	Buffer *buffer = conn->outputBuffer();
	if (reply == nullptr)
	{
		LOG_WARN << "proxyCallback err: " << c->err;
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		buffer->append(c->errstr.c_str(), c->errstr.size());
		conn->sendPipe();
		return;
	}

	if (reply->type == REDIS_REPLY_PROXY)
	{
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		buffer->append(reply->buffer, sdslen(reply->buffer));
		conn->sendPipe();
		return;
	}

	auto it = threadProxySends.find(conn->getLoop()->getThreadId());
	assert(it != threadProxySends.end());

	auto iter = it->second.find(conn->getSockfd());
	if (iter == it->second.end())
	{
		return;
	}

	auto iterr = iter->second.find(proxyCount);
	assert(iterr != iter->second.end());
	int64_t beginProxyCount = *(iter->second.begin());

	{
		reply->commandCount = commandCount;
		reply->command = command;
		insertProxyReply(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount, reply);

		auto it = threadProxyReplys.find(conn->getLoop()->getThreadId());
		assert(it != threadProxyReplys.end());
		auto iter = it->second.find(conn->getSockfd());
		assert(iter != it->second.end());

		for (auto iterr = iter->second.begin();
			iterr != iter->second.end(); beginProxyCount++)
		{
			if (beginProxyCount == iterr->first)
			{
				if (iterr->second->command == nullptr)
				{
					const RedisReplyPtr &r = iterr->second;
					buffer->append(r->buffer, sdslen(r->buffer));
				}
				else
				{
					insertCommandReply(conn->getLoop()->getThreadId(), conn->getSockfd(), iterr->second);
					if (getCommandReplyCount(conn->getLoop()->getThreadId(),
						conn->getSockfd()) >= iterr->second->commandCount)
					{
						processCommandReply(iterr->second->command,
							conn->getLoop()->getThreadId(), conn->getSockfd(), conn);
						clearCommandReply(conn->getLoop()->getThreadId(), conn->getSockfd());
					}
				}
			}
			else
			{
				break;
			}

			eraseProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), iterr->first);
			iter->second.erase(iterr++);
		}
		conn->sendPipe();
	}
}

void RedisProxy::delCallback(const std::thread::id &threadId, const int32_t sockfd,
	const TcpConnectionPtr &conn)
{
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	assert(iter != it->second.end());
	auto buffer = conn->outputBuffer();
	addReply(buffer, shared.ok);
}

void RedisProxy::mgetCallback(const std::thread::id &threadId, const int32_t sockfd,
	const TcpConnectionPtr &conn)
{
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	assert(iter != it->second.end());
	auto buffer = conn->outputBuffer();

	addReplyMultiBulkLen(buffer, iter->second.size());
	for (auto &iterr : iter->second)
	{
		const RedisReplyPtr &r = iterr;
		assert(r->type == REDIS_REPLY_ARRAY);
		for (int i = 0; i < r->element.size(); i++)
		{
			if (r->element[i]->str == nullptr)
			{
				addReply(buffer, shared.nullbulk);
			}
			else
			{
				addReplyBulkCBuffer(buffer, r->element[i]->str, sdslen(r->element[i]->str));
			}
		}
	}
}

int32_t RedisProxy::getCommandReplyCount(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter != it->second.end())
	{
		return iter->second.size();
	}
	return 0;
}

void RedisProxy::insertProxyReply(const std::thread::id &threadId,
	const int32_t sockfd, const int64_t count, const RedisReplyPtr &reply)
{
	auto it = threadProxyReplys.find(threadId);
	assert(it != threadProxyReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter != it->second.end())
	{
		iter->second.insert(std::make_pair(count, reply));
	}
	else
	{
		std::map<int64_t, RedisReplyPtr> maps;
		maps.insert(std::make_pair(count, reply));
		it->second.insert(std::make_pair(sockfd, maps));
	}
}

void RedisProxy::foreachProxyReply(const std::thread::id &threadId, const int32_t sockfd, int64_t begin)
{

}

void RedisProxy::highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent)
{
	LOG_INFO << " bytes " << bytesToSent;
	conn->getLoop()->assertInLoopThread();
	if (conn->outputBuffer()->readableBytes() > 0)
	{
		conn->stopRead();
		conn->setWriteCompleteCallback(
			std::bind(&RedisProxy::writeCompleteCallback, this, std::placeholders::_1));
	}
}

void RedisProxy::writeCompleteCallback(const TcpConnectionPtr &conn)
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
			std::bind(&RedisProxy::highWaterCallback,
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
		//LOG_INFO << "Client connect success " << buf << " " << port << " " << conn->getSockfd();
	}
	else
	{
		{
			std::unique_lock <std::mutex> lck(mutex);
			auto it = sessions.find(conn->getSockfd());
			assert(it != sessions.end());
			sessions.erase(it);
		}

		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		clearthreadProxyRedisClient(conn->getLoop()->getThreadId(), conn->getSockfd());
		//LOG_INFO << "Client diconnect " << conn->getSockfd();
	}
}

RedisAsyncContextPtr RedisProxy::checkReply(const TcpConnectionPtr &conn)
{
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());
	auto redis = it->second->getRedisAsyncContext(
		conn->getLoop()->getThreadId(), conn->getSockfd());
	if (redis == nullptr)
	{
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		std::string reply = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->sendPipe(reply.c_str(), reply.size());
	}
	return redis;
}

RedisAsyncContextPtr RedisProxy::checkReply(const RedisObjectPtr &command, const TcpConnectionPtr &conn)
{
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	RedisAsyncContextPtr redis = nullptr;
	if (clusterEnabled)
	{
		redis = it->second->getRedisAsyncContext(command,
				conn->getLoop()->getThreadId(), conn->getSockfd());
	}
	else
	{
		redis = it->second->getRedisAsyncContext(
				conn->getLoop()->getThreadId(), conn->getSockfd());
	}

	if (redis == nullptr)
	{
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		std::string reply = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->sendPipe(reply.c_str(), reply.size());
	}
	return redis;
}

bool RedisProxy::debugCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn)
{
	return false;
}

bool RedisProxy::selectCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn)
{
	if (commands.size() > 1)
	{
		return false;
	}

	addReply(conn->outputBuffer(), shared.ok);
	return true;
}

bool RedisProxy::mgetCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
	if (commands.size() < 1)
	{
		return false;
	}

	auto redis = checkReply(commands[0], conn);
	if (redis)
	{
		int64_t commandCount = 0;
		auto it = threadProxyCommands.find(conn->getLoop()->getThreadId());
		assert(it != threadProxyCommands.end());

		auto cmd = createStringObject(command->ptr,
			sdslen(command->ptr));
		for (int i = 0; i < commands.size(); i++)
		{
			it->second.clear();
			int64_t proxyCount = insertProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
			insertProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount);

			RedisCluster proxy;
			proxy.conn = conn;
			proxy.proxyCount = proxyCount;
			proxy.commandCount = commands.size();
			proxy.command = cmd;

			it->second.push_back(cmd);
			it->second.push_back(commands[i]);
			int32_t status = redis->processCommand(std::bind(&RedisProxy::proxyCallback,
				this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3), proxy, it->second);
			assert(status == REDIS_OK);
		}
	}
	return true;
}

bool RedisProxy::dbsizeCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn)
{
	int64_t dbsize  = 0;
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	auto contexts = it->second->getRedisContext(
		conn->getLoop()->getThreadId());
	assert(!contexts.empty());

	for (auto &iter : contexts)
	{
		RedisReplyPtr reply = iter->redisCommand("dbsize");
		if (reply != nullptr)
		{
			if (reply->type != REDIS_REPLY_INTEGER)
			{
				conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
				return true;
			}
			dbsize += reply->integer;
		}
		else
		{
			std::string str = it->second->setTcpClientInfo(iter->ip, iter->port);
			conn->outputBuffer()->append(str.c_str(), str.size());
			return true;
		}
	}

	addReplyLongLong(conn->outputBuffer(), dbsize);
	return true;
}

bool RedisProxy::flushdbCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn)
{
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	auto contexts = it->second->getRedisContext(
		conn->getLoop()->getThreadId());
	assert(!contexts.empty());

	for (auto &iter : contexts)
	{
		RedisReplyPtr reply = iter->redisCommand("flushdb");
		if (reply != nullptr)
		{
			if (reply->type != REDIS_REPLY_STATUS)
			{
				conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
				return true;
			}
		}
		else
		{
			std::string str = it->second->setTcpClientInfo(iter->ip, iter->port);
			conn->outputBuffer()->append(str.c_str(), str.size());
			return true;
		}
	}

	addReply(conn->outputBuffer(), shared.ok);
	return true;
}

void RedisProxy::processCommandReply(const RedisObjectPtr &command,
	const std::thread::id &threadId, const int32_t sockfd, const TcpConnectionPtr &conn)
{
	auto it = redisReplyCommands.find(command);
	assert(it != redisReplyCommands.end());
	it->second(threadId, sockfd, conn);
}

void RedisProxy::eraseProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count)
{
	auto it = threadProxySends.find(threadId);
	assert(it != threadProxySends.end());

	auto iter = it->second.find(sockfd);
	assert(iter != it->second.end());
	iter->second.erase(count);
}

void RedisProxy::clearProxyReply(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadProxyReplys.find(threadId);
	assert(it != threadProxyReplys.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			it->second.erase(iter);
		}
		//printf("clearProxyReply %d %d\n", sockfd, it->second.size());
	}
}

void RedisProxy::clearProxyCount(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadProxyCounts.find(threadId);
	assert(it != threadProxyCounts.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			it->second.erase(iter);
		}
		//printf("clearProxyCount %d %d\n", sockfd, it->second.size());
	}
}

void RedisProxy::clearProxySend(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadProxySends.find(threadId);
	assert(it != threadProxySends.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			it->second.erase(iter);
		}
		//printf("clearProxySend %d %d\n", sockfd, it->second.size());
	}
}

void RedisProxy::clearthreadProxyRedis(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadProxyRedis.find(threadId);
	assert(it != threadProxyRedis.end());
	if (!it->second.empty())
	{
		auto iter = it->second.find(sockfd);
		if (iter != it->second.end())
		{
			for (auto &iterr : iter->second)
			{
				clearProxy(threadId, iterr);
			}
			it->second.erase(iter);
			//printf("clearthreadProxyRedis %d %d\n", sockfd, it->second.size());
		}
	}
}

void RedisProxy::clearCommandReply(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter != it->second.end())
	{
		iter->second.clear();
	}
}

void RedisProxy::insertCommandReply(const std::thread::id &threadId,
	const int32_t sockfd, const RedisReplyPtr &reply)
{
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter == it->second.end())
	{
		std::vector<RedisReplyPtr> vectors;
		vectors.push_back(reply);
		it->second.insert(std::make_pair(sockfd, vectors));
	}
	else
	{
		iter->second.push_back(reply);
	}
}

void RedisProxy::clearthreadProxyRedisClient(const std::thread::id &threadId, const int32_t sockfd)
{
	auto it = threadHiredis.find(threadId);
	assert(it != threadHiredis.end());
	auto redis = it->second->getRedisAsyncContext(threadId, sockfd);
	if (redis != nullptr)
	{
		auto conn = redis->getTcpConnection().lock();
		assert(conn != nullptr);

		auto it = threadProxyRedis.find(threadId);
		assert(it != threadProxyRedis.end());

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
