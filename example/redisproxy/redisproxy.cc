#include "redisproxy.h"
#include "socket.h"
#include "object.h"

RedisProxy::RedisProxy(const char *ip, int16_t port, const char *redisIp,
	int16_t redisPort, int16_t threadCount, int16_t sessionCount, const char *logPath, bool cluster)
	: server(&loop, ip, port, nullptr),
	ip(ip),
	port(port),
	redisIp(redisIp),
	redisPort(redisPort),
	threadCount(threadCount),
	sessionCount(sessionCount),
	clusterEnabled(cluster),
	asyncLog(new AsyncLogging(logPath, "proxyserver", kRollSize)) {
	LOG_INFO << "Proxy initialized";
	initRedisPorxy();
	initRedisAsync();
	initRedisCommand();
}

RedisProxy::~RedisProxy() {
	tcpConnInfos.clear();
	threadSessions.clear();
	threadHiredis.clear();
	threadProxyReplys.clear();
	threadProxySends.clear();
}

void RedisProxy::asyncOutput(const char *msg, int32_t len) {
	std::string str = std::string(msg, len);
	printf("%s", str.c_str());
	asyncLog->append(msg, len);
}

void RedisProxy::initRedisPorxy() {
	Logger::setLogLevel(Logger::LogLevel::TRACE);
	Logger::setOutput(std::bind(&RedisProxy::asyncOutput, this,
		std::placeholders::_1, std::placeholders::_2));
	asyncLog->start();
	server.setThreadNum(threadCount);
	server.setConnectionCallback(std::bind(&RedisProxy::proxyConnCallback,
		this, std::placeholders::_1));
	server.start();
}

void RedisProxy::initRedisAsync() {
	auto pools = server.getThreadPool()->getAllLoops();
	for (int i = 0; i < pools.size(); i++) {
		auto it = threadHiredis.find(pools[i]->getThreadId());
		assert(it == threadHiredis.end());
		
		{
			std::unordered_map <int32_t, std::string> m;
			tcpConnInfos[pools[i]->getThreadId()] = m; 
		}
		
		{
			std::shared_ptr <Hiredis> hiredis(new Hiredis(pools[i], sessionCount, redisIp, redisPort, clusterEnabled));
			hiredis->setConnectionCallback(std::bind(&RedisProxy::redisConnCallback,
				this, std::placeholders::_1));
			hiredis->setDisconnectionCallback(std::bind(&RedisProxy::redisDisconnCallback,
				this, std::placeholders::_1));
			hiredis->start(pools[i], 0, redisIp, redisPort);
			hiredis->startTimer();
			threadHiredis[pools[i]->getThreadId()] = hiredis;
		}
		
		
		{
			std::shared_ptr <Hiredis> hiredis(new Hiredis(pools[i], sessionCount, redisIp, redisPort, clusterEnabled));
			hiredis->setConnectionCallback(std::bind(&RedisProxy::monitorConnCallback,
				this, std::placeholders::_1));
			hiredis->setDisconnectionCallback(std::bind(&RedisProxy::monitorDisconnCallback,
				this, std::placeholders::_1));
			hiredis->start(pools[i], 0, redisIp, redisPort);
			hiredis->startTimer();
			threadMonitorHierdis[pools[i]->getThreadId()] = hiredis;
		}
		
		
		{
			std::unordered_map <int32_t, std::shared_ptr<Hiredis>> m;
			threadSubscribeHiredis[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map <int32_t, std::map<int64_t, RedisReplyPtr>> m;
			threadProxyReplys[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map <int32_t, std::set<int64_t>> m;
			threadProxySends[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map <int32_t, std::vector<RedisReplyPtr>> m;
			threadCommandReplys[pools[i]->getThreadId()] = m;
		}

		{
			std::vector <RedisObjectPtr> m;
			threadProxyCommands[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map <int32_t, int64_t> m;
			threadProxyCounts[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map <int32_t, TcpConnectionPtr> m;
			threadMonitors[pools[i]->getThreadId()] = m;
		}

		{
			std::unordered_map <int32_t, ProxySessionPtr> m;
			threadSessions[pools[i]->getThreadId()] = m;
		}

	}
}

void RedisProxy::initRedisCommand() {
	createSharedObjects();
	redisReplyCommands[shared.mget] = std::bind(&RedisProxy::mgetCallback,
		this, std::placeholders::_1, std::placeholders::_2,
		std::placeholders::_3, std::placeholders::_4);

	redisReplyCommands[shared.mset] = std::bind(&RedisProxy::msetCallback,
		this, std::placeholders::_1, std::placeholders::_2,
		std::placeholders::_3, std::placeholders::_4);

	redisReplyCommands[shared.del] = std::bind(&RedisProxy::delCallback,
		this, std::placeholders::_1, std::placeholders::_2,
		std::placeholders::_3, std::placeholders::_4);

	redisCommands[shared.subscribe] = std::bind(&RedisProxy::subscribeCommand,
		this, std::placeholders::_1, std::placeholders::_2,
		std::placeholders::_3, std::placeholders::_4,
		std::placeholders::_5, std::placeholders::_6);

	redisCommands[shared.pubsub] = std::bind(&RedisProxy::debugCommand,
		this, std::placeholders::_1, std::placeholders::_2,
		std::placeholders::_3, std::placeholders::_4,
		std::placeholders::_5, std::placeholders::_6);

	redisCommands[shared.monitor] = std::bind(&RedisProxy::monitorCommand,
		this, std::placeholders::_1, std::placeholders::_2,
		std::placeholders::_3, std::placeholders::_4,
		std::placeholders::_5, std::placeholders::_6);

	if (clusterEnabled) {
		redisCommands[shared.cluster] = std::bind(&RedisProxy::clusterCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.ping] = std::bind(&RedisProxy::pingCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.keys] = std::bind(&RedisProxy::keysCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.select] = std::bind(&RedisProxy::selectCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.mget] = std::bind(&RedisProxy::asyncCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.mset] = std::bind(&RedisProxy::msetCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.dbsize] = std::bind(&RedisProxy::dbsizeCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.flushdb] = std::bind(&RedisProxy::flushdbCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.del] = std::bind(&RedisProxy::asyncCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.randomkey] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.renamenx] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.bitop] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.brpoplpush] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.rpoplpush] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.sinterstore] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.sdiffstore] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.sinter] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.smove] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.sunionstore] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.zinterstore] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.zunionstore] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.hscan] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.scan] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.object] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.move] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.debug] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);
		redisCommands[shared.migrate] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.rename] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.info] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);

		redisCommands[shared.command] = std::bind(&RedisProxy::debugCommand,
			this, std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4,
			std::placeholders::_5, std::placeholders::_6);
	}
}

bool RedisProxy::getRedisCommand(const RedisObjectPtr &command) {
	auto it = redisCommands.find(command);
	if (it != redisCommands.end()) {
		return true;
	}
	return false;
}

bool RedisProxy::handleRedisCommand(const RedisObjectPtr &command,
	const ProxySessionPtr &session,
	const std::vector <RedisObjectPtr> &commands,
	const TcpConnectionPtr &conn,
	const char *buf,
	const size_t len) {
	auto it = redisCommands.find(command);
	if (it != redisCommands.end()) {
		if (!it->second(command, commands, session, conn, buf, len)) {
			return false;
		}
	}
	return true;
}

void RedisProxy::run() {
	LOG_INFO << "Ready to accept connections";
	loop.run();
}

bool RedisProxy::subscribeCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	auto it = threadSubscribeHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadSubscribeHiredis.end());

	std::shared_ptr <Hiredis> hiredis(new Hiredis(conn->getLoop(), sessionCount, redisIp, redisPort, clusterEnabled));
	hiredis->setConnectionCallback(std::bind(&RedisProxy::subscribeConnCallback,
		this, std::placeholders::_1));
	hiredis->setDisconnectionCallback(std::bind(&RedisProxy::subscribeDisConnCallback,
		this, std::placeholders::_1));
	hiredis->start(conn->getLoop(), 1, redisIp, redisPort);
	auto redis = hiredis->getRedisAsyncContext();
	if (redis == nullptr) {
		std::string reply = hiredis->getTcpClientInfo(conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->outputBuffer()->append(reply.c_str(), reply.size());
		conn->sendPipe();
		return true;
	}

	it->second[conn->getSockfd()] = hiredis;
	WeakTcpConnectionPtr weakConn = conn;
	assert(redis->threadProxyRedisvAsyncCommand(std::bind(&RedisProxy::subscribeCallback,
		this, std::placeholders::_1,
		std::placeholders::_2,
		std::placeholders::_3),
		buf, len, weakConn) == REDIS_OK);

	return true;
}

void RedisProxy::subscribeCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata) {
	LOG_INFO << "Subscribe callback ";
	assert(privdata.has_value());
	WeakTcpConnectionPtr weakConn = std::any_cast<WeakTcpConnectionPtr>(privdata);
	const TcpConnectionPtr &conn = weakConn.lock();
	if (conn == nullptr) {
		return;
	}

	if (reply == nullptr) {
		auto it = threadSubscribeHiredis.find(std::this_thread::get_id());
		assert(it != threadSubscribeHiredis.end());
		auto iter = it->second.find(conn->getSockfd());
		if (iter == it->second.end()) {
			return;
		}

		std::string reply = iter->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->outputBuffer()->append(reply.c_str(), reply.size());
	}
	else {
		conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
	}
	conn->sendPipe();
}

void RedisProxy::monitorCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata) {
	if (reply != nullptr) {
		auto it = threadMonitors.find(std::this_thread::get_id());
		assert(it != threadMonitors.end());
		for (auto &iter : it->second) {
			auto buffer = iter.second->outputBuffer();
			buffer->append(reply->buffer, sdslen(reply->buffer));
			iter.second->sendPipe();
		}
	}
}

void RedisProxy::monitorConnCallback(const TcpConnectionPtr &conn) {
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);
	Socket::setkeepAlive(conn->getSockfd(), kHeart);
	conn->getLoop()->assertInLoopThread();
	LOG_INFO << "MonitorClient connect "
		<< conn->getSockfd() << " " << buf << " " << port;

	assert(conn->getContext().has_value());
	const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
	assert(ac->redisAsyncCommand(std::bind(&RedisProxy::monitorCallback,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
		nullptr, "monitor") == REDIS_OK);
}

void RedisProxy::monitorDisconnCallback(const TcpConnectionPtr &conn) {
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);
	conn->getLoop()->assertInLoopThread();
	LOG_INFO << "MonitorClient disconnect "
		<< conn->getSockfd() << " " << buf << " " << port;
}

void RedisProxy::subscribeConnCallback(const TcpConnectionPtr &conn) {
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);
	Socket::setkeepAlive(conn->getSockfd(), kHeart);
	conn->getLoop()->assertInLoopThread();
	LOG_INFO << "SubcribeClient connect "
		<< conn->getSockfd() << " " << buf << " " << port;
}

void RedisProxy::subscribeDisConnCallback(const TcpConnectionPtr &conn) {
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);
	
	conn->getLoop()->assertInLoopThread();
	LOG_INFO << "SubcribeClient disconnect "
		<< conn->getSockfd() << " " << buf << " " << port;
}

void RedisProxy::redisConnCallback(const TcpConnectionPtr &conn) {
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);
	Socket::setkeepAlive(conn->getSockfd(), kHeart);
	conn->getLoop()->assertInLoopThread();
	LOG_INFO << "RedisClient connect "
		<< conn->getSockfd() << " " << buf << " " << port;
}

void RedisProxy::redisDisconnCallback(const TcpConnectionPtr &conn) {
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);

	conn->getLoop()->assertInLoopThread();
	clearCurrentThread(conn->getLoop()->getThreadId());
	LOG_INFO << "RedisClient disconnect "
		<< conn->getSockfd() << " " << buf << " " << port;
}

void RedisProxy::clearProxy(const std::thread::id &threadId, const int32_t sockfd) {
	clearProxyReply(threadId, sockfd);
	clearProxySend(threadId, sockfd);
	clearProxyCount(threadId, sockfd);
	clearCommandReply(threadId, sockfd);
}

void RedisProxy::insertProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count) {
	auto it = threadProxySends.find(threadId);
	assert(it != threadProxySends.end());
	auto iter = it->second.find(sockfd);
	if (iter == it->second.end()) {
		std::set <int64_t> sets;
		sets.insert(count);
		it->second.insert(std::make_pair(sockfd, sets));
	}
	else {
		iter->second.insert(count);
	}
}

void RedisProxy::processCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const TcpConnectionPtr &conn, const char *buf,
	size_t len) {
	RedisObjectPtr cmd = createStringObject(command->ptr, sdslen(command->ptr));
	assert(!commands.empty());
	auto redis = checkCommandToCluster(commands[0], conn);
	if (redis) {
		int64_t proxyCount = insertProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
		insertProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount);
		std::shared_ptr <RedisCluster> proxy(new RedisCluster());
		proxy->conn = conn;
		proxy->proxyCount = proxyCount;
		proxy->command = cmd;
		proxy->commandCount = proxyCount;
		assert(redis->threadProxyRedisvAsyncCommand(std::bind(&RedisProxy::proxyCallback,
			this, std::placeholders::_1,
			std::placeholders::_2,
			std::placeholders::_3),
			buf, len, proxy) == REDIS_OK);
	}
}

void RedisProxy::proxyCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata) {
	
	assert(privdata.has_value());
	std::shared_ptr <RedisCluster> proxy = std::any_cast <std::shared_ptr < RedisCluster >> (privdata);
	assert(proxy != nullptr);
	
	reply->commandCount = proxy->commandCount;
	reply->command = proxy->command;
	int64_t proxyCount = proxy->proxyCount;

	WeakTcpConnectionPtr weakConn = proxy->conn;
	TcpConnectionPtr conn = weakConn.lock();
	if (conn == nullptr) {
		return;
	}

	conn->getLoop()->assertInLoopThread();
	Buffer *buffer = conn->outputBuffer();
	if (reply == nullptr) {
		LOG_WARN << "Callback: " << c->err;
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		auto it = threadHiredis.find(conn->getLoop()->getThreadId());
		assert(it != threadHiredis.end());
		std::string str = it->second->setTcpClientInfo(c->redisContext->errstr, c->redisContext->ip.c_str(),
			c->redisContext->port);
		conn->outputBuffer()->append(str.c_str(), str.size());
		conn->sendPipe();
		return;
	}

	if (reply->type == REDIS_REPLY_PROXY) {
		LOG_WARN << "Callback proxy";
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		buffer->append(reply->buffer, sdslen(reply->buffer));
		conn->sendPipe();
		return;
	}

	auto it = threadProxySends.find(conn->getLoop()->getThreadId());
	assert(it != threadProxySends.end());

	auto iter = it->second.find(conn->getSockfd());
	if (iter == it->second.end()) {
		LOG_WARN << "Callback fd " << conn->getSockfd();
		return;
	}

	auto iterr = iter->second.find(proxyCount);
	if (iterr == iter->second.end()) {
		return;
	}

	int64_t beginProxyCount = *(iter->second.begin());

	{
		insertProxyReply(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount, reply);

		auto it = threadProxyReplys.find(conn->getLoop()->getThreadId());
		assert(it != threadProxyReplys.end());
		auto iter = it->second.find(conn->getSockfd());
		assert(iter != it->second.end());

		for (auto iterr = iter->second.begin();
			iterr != iter->second.end(); beginProxyCount++) {
			if (beginProxyCount == iterr->first) {
				if (!findRedisReplyCommand(iterr->second->command)) {
					const RedisReplyPtr &r = iterr->second;
					buffer->append(r->buffer, sdslen(r->buffer));
				}
				else {
					insertCommandReply(conn->getLoop()->getThreadId(), conn->getSockfd(), iterr->second);
					if (getCommandReplyCount(conn->getLoop()->getThreadId(),
						conn->getSockfd()) >= iterr->second->commandCount) {
						processCommandReply(iterr->second->command, iterr->second->commandCount,
							conn->getLoop()->getThreadId(), conn->getSockfd(), conn);
						clearCommandReply(conn->getLoop()->getThreadId(), conn->getSockfd());
					}
				}
			}
			else {
				break;
			}

			eraseProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), iterr->first);
			iter->second.erase(iterr++);
		}
		conn->sendPipe();
	}
}

void RedisProxy::delCallback(const std::thread::id &threadId, const int32_t sockfd,
	const TcpConnectionPtr &conn, int32_t commandCount) {
	LOG_DEBUG <<"";
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	assert(iter != it->second.end());

	int32_t count = 0;
	for (auto &iterr : iter->second) {
		const RedisReplyPtr &reply = iterr;
		assert(reply != nullptr);

		if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
			count++;
		}
		else {
			conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
			return;
		}
	}
	addReplyLongLong(conn->outputBuffer(), count);
}

void RedisProxy::msetCallback(const std::thread::id &threadId, const int32_t sockfd,
	const TcpConnectionPtr &conn, int32_t commandCount) {
	LOG_DEBUG <<"";
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	assert(iter != it->second.end());
	auto buffer = conn->outputBuffer();
	for (auto &iterr : iter->second) {
		const RedisReplyPtr &reply = iterr;
		assert(reply != nullptr);

		if (reply->type != REDIS_REPLY_STATUS && strcmp(iterr->str, "OK") != 0) {
			conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
			return;
		}
	}
	addReply(buffer, shared.ok);
}

void RedisProxy::mgetCallback(const std::thread::id &threadId, const int32_t sockfd,
	const TcpConnectionPtr &conn, int32_t commandCount) {
	LOG_DEBUG <<"";
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	assert(iter != it->second.end());
	auto buffer = conn->outputBuffer();

	addReplyMultiBulkLen(buffer, iter->second.size());
	for (auto &iterr : iter->second) {
		const RedisReplyPtr &reply = iterr;
		assert(reply != nullptr);

		if (reply->type == REDIS_REPLY_ARRAY) {
			for (int i = 0; i < reply->element.size(); i++) {
				if (reply->element[i]->str == nullptr) {
					addReply(buffer, shared.nullbulk);
				}
				else {
					addReplyBulkCBuffer(buffer, reply->element[i]->str, sdslen(reply->element[i]->str));
				}
			}
		}
		else {
			conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
			return;
		}
	}
}

int32_t RedisProxy::getCommandReplyCount(const std::thread::id &threadId, const int32_t sockfd) {
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter != it->second.end()) {
		return iter->second.size();
	}
	return 0;
}

int64_t RedisProxy::insertProxyCount(const std::thread::id &threadId, const int32_t sockfd) {
	int64_t proxyCount = 0;
	auto it = threadProxyCounts.find(threadId);
	assert(it != threadProxyCounts.end());
	auto iter = it->second.find(sockfd);
	if (iter == it->second.end()) {
		proxyCount = 1;
		it->second.insert(std::make_pair(sockfd, proxyCount));
	}
	else {
		proxyCount = ++iter->second;
	}
	return proxyCount;
}

void RedisProxy::insertProxyReply(const std::thread::id &threadId,
	const int32_t sockfd, const int64_t count, const RedisReplyPtr &reply) {
	auto it = threadProxyReplys.find(threadId);
	assert(it != threadProxyReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter != it->second.end()) {
		iter->second.insert(std::make_pair(count, reply));
	}
	else {
		std::map <int64_t, RedisReplyPtr> maps;
		maps.insert(std::make_pair(count, reply));
		it->second.insert(std::make_pair(sockfd, maps));
	}
}

void RedisProxy::highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent) {
	LOG_INFO << " bytes " << bytesToSent;
	conn->getLoop()->assertInLoopThread();
	if (conn->outputBuffer()->readableBytes() > 0) {
		conn->stopRead();
		conn->setWriteCompleteCallback(
			std::bind(&RedisProxy::writeCompleteCallback, this, std::placeholders::_1));
	}
}

void RedisProxy::writeCompleteCallback(const TcpConnectionPtr &conn) {
	conn->startRead();
	conn->setWriteCompleteCallback(WriteCompleteCallback());
}

void RedisProxy::proxyConnCallback(const TcpConnectionPtr &conn) {
	if (conn->connected()) {
		char buf[64] = "";
		uint16_t port = 0;
		auto addr = Socket::getPeerAddr(conn->getSockfd());
		Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
		Socket::toPort(&port, (const struct sockaddr *) &addr);
		Socket::setkeepAlive(conn->getSockfd(), kHeart);
		conn->setHighWaterMarkCallback(
			std::bind(&RedisProxy::highWaterCallback,
				this, std::placeholders::_1, std::placeholders::_2),
			kHighWaterBytes);

		ProxySessionPtr session(new ProxySession(this, conn));
		{
			auto it = threadSessions.find(conn->getLoop()->getThreadId());
			assert(it != threadSessions.end());
			it->second[conn->getSockfd()] = session;
		}
		
		std::string addrinfo = std::string(buf) + ":" + std::to_string(port);
		{
			auto it = tcpConnInfos.find(conn->getLoop()->getThreadId());
			assert(it != tcpConnInfos.end());
			it->second[conn->getSockfd()] = addrinfo;
		}
		LOG_INFO << "Client connect success " << addrinfo << " " << conn->getSockfd();
	}
	else {
		{
			auto it = threadSessions.find(conn->getLoop()->getThreadId());
			assert(it != threadSessions.end());
			size_t n = it->second.erase(conn->getSockfd());
			assert(n == 1);
		}

		{
			auto it = threadMonitors.find(conn->getLoop()->getThreadId());
			assert(it != threadMonitors.end());
			it->second.erase(conn->getSockfd());
		}

		{
			auto it = threadSubscribeHiredis.find(conn->getLoop()->getThreadId());
			assert(it != threadSubscribeHiredis.end());
			auto iter = it->second.find(conn->getSockfd());
			if (iter != it->second.end()) {
				iter->second->diconnectTcpClient();
				it->second.erase(iter);
			}
		}
		
		{
			auto it = tcpConnInfos.find(conn->getLoop()->getThreadId());
			assert(it != tcpConnInfos.end());
			auto iter = it->second.find(conn->getSockfd());
			if (iter != it->second.end()) {
				LOG_INFO << "Client disconnect " << iter->second << " " << conn->getSockfd();
				it->second.erase(iter);
			}
		}
	}
}

RedisAsyncContextPtr RedisProxy::checkCommandToCluster(const TcpConnectionPtr &conn, bool pubsub) {
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());
	auto redisAsync = it->second->getRedisAsyncContext(
		conn->getLoop()->getThreadId(), conn->getSockfd());
	if (redisAsync == nullptr) {
		LOG_DEBUG <<"";
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		std::string reply = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->sendPipe(reply.c_str(), reply.size());
	}
	return redisAsync;
}

RedisAsyncContextPtr RedisProxy::checkCommandToCluster(const RedisObjectPtr &command, const TcpConnectionPtr &conn, bool pubsub) {
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	RedisAsyncContextPtr redisAsync;
	if (clusterEnabled) {
		redisAsync = it->second->getRedisAsyncContext(command,
			conn->getLoop()->getThreadId(), conn->getSockfd());
	}
	else {
		redisAsync = it->second->getRedisAsyncContext(
			conn->getLoop()->getThreadId(), conn->getSockfd());
	}

	if (redisAsync == nullptr) {
		LOG_DEBUG <<"";
		clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		std::string reply = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->sendPipe(reply.c_str(), reply.size());
	}
	return redisAsync;
}

bool RedisProxy::debugCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	addReply(conn->outputBuffer(), shared.ok);
	return true;
}

bool RedisProxy::monitorCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (commands.size() > 1) {
		return false;
	}

	auto it = threadMonitors.find(conn->getLoop()->getThreadId());
	it->second[conn->getSockfd()] = conn;
	addReply(conn->outputBuffer(), shared.ok);
	return true;
}

bool RedisProxy::selectCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (commands.size() > 1) {
		return false;
	}

	addReply(conn->outputBuffer(), shared.ok);
	return true;
}

bool RedisProxy::keysCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (commands.size() != 1) {
		return false;
	}

	int64_t numkeys = 0;
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	auto contexts = it->second->getRedisContext(
		conn->getLoop()->getThreadId());
	assert(!contexts.empty());

	sds cmd = sdsnewlen(command->ptr, sdslen(command->ptr));
	cmd = sdscatlen(cmd, " ", 1);
	cmd = sdscatlen(cmd, commands[0]->ptr, sdslen(commands[0]->ptr));

	for (auto &iter : contexts) {
		const RedisReplyPtr &reply = iter->redisCommand(cmd);
		if (reply == nullptr) {
			std::string str = it->second->setTcpClientInfo(iter->errstr, iter->ip, iter->port);
			conn->outputBuffer()->append(str.c_str(), str.size());
			sdsfree(cmd);
			return true;
		}
		else if (reply->type == REDIS_REPLY_ARRAY) {
			for (int i = 0; i < reply->element.size(); i++) {
				addReplyBulkCBuffer(conn->outputBuffer(), reply->element[i]->str,
					sdslen(reply->element[i]->str));
				numkeys++;
			}
		}
		else {
			conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
			sdsfree(cmd);
			return true;
		}
	}

	sdsfree(cmd);
	prePendReplyLongLongWithPrefix(conn->outputBuffer(), numkeys);
	return true;
}

bool RedisProxy::msetCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (commands.size() < 2 || commands.size() % 2 != 0) {
		return false;
	}

	auto it = threadProxyCommands.find(conn->getLoop()->getThreadId());
	assert(it != threadProxyCommands.end());
	auto cmd = createStringObject(command->ptr, sdslen(command->ptr));

	for (int i = 0; i < commands.size(); i += 2) {
		auto redis = checkCommandToCluster(commands[i], conn);
		if (redis) {
			it->second.clear();
			int64_t proxyCount = insertProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
			insertProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount);

			std::shared_ptr <RedisCluster> proxy(new RedisCluster());
			proxy->conn = conn;
			proxy->proxyCount = proxyCount;
			proxy->commandCount = commands.size() / 2;
			proxy->command = cmd;

			it->second.push_back(cmd);
			it->second.push_back(commands[i]);
			it->second.push_back(commands[i + 1]);
			int32_t status = redis->processCommand(std::bind(&RedisProxy::proxyCallback,
				this, std::placeholders::_1, std::placeholders::_2,
				std::placeholders::_3), proxy, it->second);
			assert(status == REDIS_OK);
		}
	}
	return true;
}

bool RedisProxy::pingCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (commands.size() > 0) {
		return false;
	}

	addReply(conn->outputBuffer(), shared.pong);
	return true;
}

bool RedisProxy::clusterCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (commands.size() < 1) {
		return false;
	}

	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	auto context = it->second->getRedisContext(
		conn->getSockfd());
	if (context == nullptr) {
		std::string reply = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->outputBuffer()->append(reply.c_str(), reply.size());
		return true;
	}

	sds cmd = sdsnewlen(command->ptr, sdslen(command->ptr));
	for (int i = 0; i < commands.size(); i++) {
		cmd = sdscatlen(cmd, " ", 1);
		cmd = sdscatlen(cmd, commands[i]->ptr, sdslen(commands[i]->ptr));
	}

	const RedisReplyPtr &reply = context->redisCommand(cmd);
	if (reply == nullptr) {
		std::string reply = it->second->getTcpClientInfo(
			conn->getLoop()->getThreadId(), conn->getSockfd());
		conn->outputBuffer()->append(reply.c_str(), reply.size());
		sdsfree(cmd);
		return true;
	}
	else if (reply->type == REDIS_REPLY_STRING) {
		conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
	}
	else {
		conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
		sdsfree(cmd);
		return true;
	}
	return true;
}

bool RedisProxy::asyncCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (commands.size() < 1) {
		return false;
	}

	auto it = threadProxyCommands.find(conn->getLoop()->getThreadId());
	assert(it != threadProxyCommands.end());
	auto cmd = createStringObject(command->ptr, sdslen(command->ptr));

	for (int i = 0; i < commands.size(); i++) {
		auto redis = checkCommandToCluster(commands[i], conn);
		if (redis) {
			it->second.clear();
			int64_t proxyCount = insertProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
			insertProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount);

			std::shared_ptr <RedisCluster> proxy(new RedisCluster());
			proxy->conn = conn;
			proxy->proxyCount = proxyCount;
			proxy->commandCount = commands.size();
			proxy->command = cmd;

			it->second.push_back(cmd);
			it->second.push_back(commands[i]);
			int32_t status = redis->processCommand(std::bind(&RedisProxy::proxyCallback,
				this, std::placeholders::_1, std::placeholders::_2,
				std::placeholders::_3), proxy, it->second);
			assert(status == REDIS_OK);
		}
	}
	return true;
}

bool RedisProxy::dbsizeCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (!commands.empty()) {
		return false;
	}

	int64_t dbsize = 0;
	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	auto contexts = it->second->getRedisContext(
		conn->getLoop()->getThreadId());
	assert(!contexts.empty());

	for (auto &iter : contexts) {
		const RedisReplyPtr &reply = iter->redisCommand(command->ptr, sdslen(command->ptr));
		if (reply == nullptr) {
			std::string str = it->second->setTcpClientInfo(iter->errstr, iter->ip, iter->port);
			conn->outputBuffer()->append(str.c_str(), str.size());
			return true;
		}
		else if (reply->type == REDIS_REPLY_INTEGER) {
			dbsize += reply->integer;
		}
		else {
			conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
			return true;
		}
	}

	addReplyLongLong(conn->outputBuffer(), dbsize);
	return true;
}

bool RedisProxy::flushdbCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
	const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
	const size_t len) {
	if (!commands.empty()) {
		return false;
	}

	auto it = threadHiredis.find(conn->getLoop()->getThreadId());
	assert(it != threadHiredis.end());

	auto contexts = it->second->getRedisContext(
		conn->getLoop()->getThreadId());
	assert(!contexts.empty());

	for (auto &iter : contexts) {
		const RedisReplyPtr &reply = iter->redisCommand(command->ptr, sdslen(command->ptr));
		if (reply == nullptr) {
			std::string str = it->second->setTcpClientInfo(iter->errstr, iter->ip, iter->port);
			conn->outputBuffer()->append(str.c_str(), str.size());
			return true;
		}
		else if (reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK") != 0) {
			conn->outputBuffer()->append(reply->buffer, sdslen(reply->buffer));
			return true;
		}
	}

	addReply(conn->outputBuffer(), shared.ok);
	return true;
}

bool RedisProxy::findRedisReplyCommand(const RedisObjectPtr &command) {
	auto it = redisReplyCommands.find(command);
	if (it == redisReplyCommands.end()) {
		return false;
	}
	else {
		return true;
	}
}

void RedisProxy::processCommandReply(const RedisObjectPtr &command, int32_t commandCount,
	const std::thread::id &threadId, const int32_t sockfd,
	const TcpConnectionPtr &conn) {
	auto it = redisReplyCommands.find(command);
	assert(it != redisReplyCommands.end());
	it->second(threadId, sockfd, conn, commandCount);
}

void RedisProxy::clearCurrentThread(const std::thread::id &threadId) {
	{
		auto it = threadProxySends.find(threadId);
		assert(it != threadProxySends.end());
		it->second.clear();
	}

	{
		auto it = threadProxyCounts.find(threadId);
		assert(it != threadProxyCounts.end());
		it->second.clear();
	}

	{
		auto it = threadProxyCounts.find(threadId);
		assert(it != threadProxyCounts.end());
		it->second.clear();
	}

	{
		auto it = threadCommandReplys.find(threadId);
		assert(it != threadCommandReplys.end());
		it->second.clear();
	}
}

void RedisProxy::eraseProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count) {
	auto it = threadProxySends.find(threadId);
	assert(it != threadProxySends.end());

	auto iter = it->second.find(sockfd);
	assert(iter != it->second.end());
	iter->second.erase(count);
}

void RedisProxy::clearProxyReply(const std::thread::id &threadId, const int32_t sockfd) {
	auto it = threadProxyReplys.find(threadId);
	assert(it != threadProxyReplys.end());
	it->second.erase(sockfd);
}

void RedisProxy::clearProxyCount(const std::thread::id &threadId, const int32_t sockfd) {
	auto it = threadProxyCounts.find(threadId);
	assert(it != threadProxyCounts.end());
	it->second.erase(sockfd);
}

void RedisProxy::clearProxySend(const std::thread::id &threadId, const int32_t sockfd) {
	auto it = threadProxySends.find(threadId);
	assert(it != threadProxySends.end());
	it->second.erase(sockfd);
}

void RedisProxy::clearCommandReply(const std::thread::id &threadId, const int32_t sockfd) {
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter != it->second.end()) {
		iter->second.clear();
	}
}

void RedisProxy::insertCommandReply(const std::thread::id &threadId,
	const int32_t sockfd, const RedisReplyPtr &reply) {
	auto it = threadCommandReplys.find(threadId);
	assert(it != threadCommandReplys.end());
	auto iter = it->second.find(sockfd);
	if (iter == it->second.end()) {
		std::vector <RedisReplyPtr> vectors;
		vectors.push_back(reply);
		it->second.insert(std::make_pair(sockfd, vectors));
	}
	else {
		iter->second.push_back(reply);
	}
}
