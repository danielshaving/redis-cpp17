#include "redisproxy.h"
#include "socket.h"
#include "object.h"

RedisProxy::RedisProxy(const char *ip, int16_t port, const char *redisIp,
                       int16_t redisPort, int16_t threadCount, int16_t sessionCount)
        : server(&loop, ip, port, nullptr),
          ip(ip),
          port(port),
          redisIp(redisIp),
          redisPort(redisPort),
          threadCount(threadCount),
          sessionCount(sessionCount),
          clusterEnabled(true) {
    LOG_INFO << "Proxy initialized";
    initRedisPorxy();
    initRedisAsync();
    initRedisCommand();
    initRedisTimer();
}

RedisProxy::~RedisProxy() {
    sessions.clear();
    threadHiredis.clear();
    threadProxyReplys.clear();
    threadProxySends.clear();
}

void RedisProxy::initRedisPorxy() {
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

        std::shared_ptr<Hiredis> hiredis(new Hiredis(pools[i], sessionCount, redisIp, redisPort, true));
        hiredis->setConnectionCallback(std::bind(&RedisProxy::redisConnCallback,
                                                 this, std::placeholders::_1));
        hiredis->setDisconnectionCallback(std::bind(&RedisProxy::redisDisconnCallback,
                                                    this, std::placeholders::_1));
        hiredis->start(pools[i], 0, redisIp, redisPort);
        threadHiredis[pools[i]->getThreadId()] = hiredis;

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
	
    }
}

void RedisProxy::initRedisTimer() {
    auto pools = server.getThreadPool()->getAllLoops();
    for (int i = 0; i < pools.size(); i++) {
        auto it = threadHiredis.find(pools[i]->getThreadId());
        assert(it != threadHiredis.end());
        pools[i]->runAfter(1.0, true, std::bind(&Hiredis::redisContextTimer, it->second));
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

    redisCommands[shared.publish] = std::bind(&RedisProxy::debugCommand,
                                              this, std::placeholders::_1, std::placeholders::_2,
                                              std::placeholders::_3, std::placeholders::_4,
                                              std::placeholders::_5, std::placeholders::_6);
    redisCommands[shared.subscribe] = std::bind(&RedisProxy::debugCommand,
                                                this, std::placeholders::_1, std::placeholders::_2,
                                                std::placeholders::_3, std::placeholders::_4,
                                                std::placeholders::_5, std::placeholders::_6);
    redisCommands[shared.unsubscribe] = std::bind(&RedisProxy::debugCommand,
                                                  this, std::placeholders::_1, std::placeholders::_2,
                                                  std::placeholders::_3, std::placeholders::_4,
                                                  std::placeholders::_5, std::placeholders::_6);
    redisCommands[shared.pubsub] = std::bind(&RedisProxy::debugCommand,
                                             this, std::placeholders::_1, std::placeholders::_2,
                                             std::placeholders::_3, std::placeholders::_4,
                                             std::placeholders::_5, std::placeholders::_6);

    if (clusterEnabled) {
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
        redisCommands[shared.mget] = std::bind(&RedisProxy::clusterCommand,
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
        redisCommands[shared.del] = std::bind(&RedisProxy::clusterCommand,
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
        redisCommands[shared.cluster] = std::bind(&RedisProxy::debugCommand,
                                                  this, std::placeholders::_1, std::placeholders::_2,
                                                  std::placeholders::_3, std::placeholders::_4,
                                                  std::placeholders::_5, std::placeholders::_6);
        redisCommands[shared.monitor] = std::bind(&RedisProxy::debugCommand,
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

void RedisProxy::redisConnCallback(const TcpConnectionPtr &conn) {
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);

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
    } else {
        iter->second.insert(count);
    }
}

void RedisProxy::processCommand(const RedisObjectPtr &command, const TcpConnectionPtr &conn, const char *buf,
                                size_t len) {
    auto redis = checkReply(command, conn);
    if (redis) {
        int64_t proxyCount = insertProxyCount(conn->getLoop()->getThreadId(), conn->getSockfd());
		insertProxySend(conn->getLoop()->getThreadId(), conn->getSockfd(), proxyCount);
        std::shared_ptr<RedisCluster> proxy(new RedisCluster());
        proxy->conn = conn;
        proxy->proxyCount = proxyCount;
        proxy->command = nullptr;
		proxy->commandCount = proxyCount;
        int32_t status = redis->threadProxyRedisvAsyncCommand(std::bind(&RedisProxy::proxyCallback,
                                                                        this, std::placeholders::_1,
                                                                        std::placeholders::_2,
                                                                        std::placeholders::_3),
                                                              buf, len, proxy);
        assert(status == REDIS_OK);
    }
}

void RedisProxy::proxyCallback(const RedisAsyncContextPtr &c,
                               const RedisReplyPtr &reply, const std::any &privdata) {
	
    assert(privdata.has_value());
    std::shared_ptr<RedisCluster> proxy = std::any_cast<std::shared_ptr<RedisCluster>>(privdata);
	assert(proxy != nullptr);
	
	reply->commandCount = proxy->commandCount;
	reply->command = proxy->command;
    int64_t proxyCount = proxy->proxyCount;
	if (proxy->command != nullptr) {
		LOG_INFO << "Callback command " <<std::string(proxy->command->ptr, sdslen(proxy->command->ptr));
	}
	
	LOG_INFO << "Callback proxycount " << proxy->proxyCount;
    WeakTcpConnectionPtr weakConn = proxy->conn;
    TcpConnectionPtr conn = weakConn.lock();
    if (conn == nullptr) {
        LOG_WARN << "Callback client disconnect err: ";
        return;
    }

    conn->getLoop()->assertInLoopThread();
    Buffer *buffer = conn->outputBuffer();
    if (reply == nullptr) {
        LOG_WARN << "Callback: " << c->err;
        clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
		auto it = threadHiredis.find(conn->getLoop()->getThreadId());
		assert(it != threadHiredis.end());
        std::string str = it->second->setTcpClientInfo(c->redisContext->errstr, c->redisContext->ip.c_str(), c->redisContext->port);
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
	if (iter == it->second.end())
	{
		LOG_WARN << "Callback fd " << conn->getSockfd();
		return;
	}

	auto iterr = iter->second.find(proxyCount);
	assert(iterr != iter->second.end());
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
				if (iterr->second->command == nullptr) {
					const RedisReplyPtr &r = iterr->second;
					buffer->append(r->buffer, sdslen(r->buffer));
				} else {
					insertCommandReply(conn->getLoop()->getThreadId(), conn->getSockfd(), iterr->second);
					if (getCommandReplyCount(conn->getLoop()->getThreadId(),
						conn->getSockfd()) >= iterr->second->commandCount) {
						processCommandReply(iterr->second->command, iterr->second->commandCount,
							conn->getLoop()->getThreadId(), conn->getSockfd(), conn);
						clearCommandReply(conn->getLoop()->getThreadId(), conn->getSockfd());
					}
				}
			} else {
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
	LOG_INFO << "Del callback ";
    auto it = threadCommandReplys.find(threadId);
    assert(it != threadCommandReplys.end());
    auto iter = it->second.find(sockfd);
    assert(iter != it->second.end());

    int32_t count = 0;
    for (auto &iterr : iter->second) {
		assert(iterr != nullptr);
        if (iterr->type == REDIS_REPLY_INTEGER && iterr->integer == 1) {
			count++;
		} else {
			conn->outputBuffer()->append(iterr->buffer, sdslen(iterr->buffer));
			return ;
		}
    }

    auto buffer = conn->outputBuffer();
    addReplyLongLong(buffer, count);
}

void RedisProxy::msetCallback(const std::thread::id &threadId, const int32_t sockfd,
                              const TcpConnectionPtr &conn, int32_t commandCount) {
	LOG_INFO << "Mset callback ";
    auto it = threadCommandReplys.find(threadId);
    assert(it != threadCommandReplys.end());
    auto iter = it->second.find(sockfd);
    assert(iter != it->second.end());
	auto buffer = conn->outputBuffer();
    for (auto &iterr : iter->second) {
        if (iterr == nullptr || iterr->type != REDIS_REPLY_STATUS || strcmp(iterr->str, "OK") == 0) {
			buffer->append(iterr->buffer, sdslen(iterr->buffer));
			return ;
		}
    }
    addReply(buffer, shared.ok);
}

void RedisProxy::mgetCallback(const std::thread::id &threadId, const int32_t sockfd,
                              const TcpConnectionPtr &conn, int32_t commandCount) {
	LOG_INFO << "Mget callback ";
    auto it = threadCommandReplys.find(threadId);
    assert(it != threadCommandReplys.end());
    auto iter = it->second.find(sockfd);
    assert(iter != it->second.end());
    auto buffer = conn->outputBuffer();

    addReplyMultiBulkLen(buffer, iter->second.size());
    for (auto &iterr : iter->second) {
        const RedisReplyPtr &r = iterr;
        assert(r->type == REDIS_REPLY_ARRAY);
        for (int i = 0; i < r->element.size(); i++) {
            if (r->element[i]->str == nullptr) {
                addReply(buffer, shared.nullbulk);
            } else {
                addReplyBulkCBuffer(buffer, r->element[i]->str, sdslen(r->element[i]->str));
            }
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
	} else {
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
    } else {
        std::map <int64_t, RedisReplyPtr> maps;
        maps.insert(std::make_pair(count, reply));
        it->second.insert(std::make_pair(sockfd, maps));
    }
}

void RedisProxy::foreachProxyReply(const std::thread::id &threadId, const int32_t sockfd, int64_t begin) {

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
	char buf[64] = "";
	uint16_t port = 0;
	auto addr = Socket::getPeerAddr(conn->getSockfd());
	Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
	Socket::toPort(&port, (const struct sockaddr *) &addr);
		
    if (conn->connected()) {
        Socket::setkeepAlive(conn->getSockfd(), kHeart);
        conn->setHighWaterMarkCallback(
                std::bind(&RedisProxy::highWaterCallback,
                          this, std::placeholders::_1, std::placeholders::_2),
                kHighWaterBytes);
				
        ProxySessionPtr session(new ProxySession(this, conn));
        {
            std::unique_lock <std::mutex> lck(mutex);
            auto it = sessions.find(conn->getSockfd());
            assert(it == sessions.end());
            sessions[conn->getSockfd()] = session;
        }
        LOG_INFO << "Client connect success " << buf << " " << port << " " << conn->getSockfd();
    } else {
        {
            std::unique_lock <std::mutex> lck(mutex);
            auto it = sessions.find(conn->getSockfd());
            assert(it != sessions.end());
            sessions.erase(it);
        }

        clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
        LOG_INFO << "Client diconnect " << buf << " " << port << " " << conn->getSockfd();
    }
}

RedisAsyncContextPtr RedisProxy::checkReply(const TcpConnectionPtr &conn) {
    auto it = threadHiredis.find(conn->getLoop()->getThreadId());
    assert(it != threadHiredis.end());
    auto redis = it->second->getRedisAsyncContext(
            conn->getLoop()->getThreadId(), conn->getSockfd());
    if (redis == nullptr) {
		LOG_INFO << " redis == nullptr ";
        clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
        std::string reply = it->second->getTcpClientInfo(
                conn->getLoop()->getThreadId(), conn->getSockfd());
        conn->sendPipe(reply.c_str(), reply.size());
    }
    return redis;
}

RedisAsyncContextPtr RedisProxy::checkReply(const RedisObjectPtr &command, const TcpConnectionPtr &conn) {
    auto it = threadHiredis.find(conn->getLoop()->getThreadId());
    assert(it != threadHiredis.end());

    RedisAsyncContextPtr redisAsync = nullptr;
    if (clusterEnabled) {
        redisAsync = it->second->getRedisAsyncContext(command,
                                                      conn->getLoop()->getThreadId(), conn->getSockfd());
    } else {
        redisAsync = it->second->getRedisAsyncContext(
                conn->getLoop()->getThreadId(), conn->getSockfd());
    }

    if (redisAsync == nullptr) {
		LOG_INFO << " redisAsync == nullptr ";
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

    std::string cmd = command->ptr;
    cmd += " ";
    cmd += std::string(commands[0]->ptr, sdslen(commands[0]->ptr));
    for (auto &iter : contexts) {
        RedisReplyPtr reply = iter->redisCommand(cmd.c_str());
        if (reply != nullptr || reply->type == REDIS_REPLY_ARRAY) {
            for (int i = 0; i < reply->element.size(); i++) {
                addReplyBulkCBuffer(conn->outputBuffer(), reply->element[i]->str,
                                    sdslen(reply->element[i]->str));
                numkeys++;
            }
        } else {
            std::string str = it->second->setTcpClientInfo(iter->errstr, iter->ip, iter->port);
            conn->outputBuffer()->append(str.c_str(), str.size());
            return true;
        }
    }

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
		auto redis = checkReply(commands[i], conn);
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
		} else {
			clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
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

	auto it = threadProxyCommands.find(conn->getLoop()->getThreadId());
	assert(it != threadProxyCommands.end());
	auto cmd = createStringObject(command->ptr, sdslen(command->ptr));
									  
	for (int i = 0; i < commands.size(); i ++) {
		auto redis = checkReply(commands[i], conn);
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
		} else {
			clearProxy(conn->getLoop()->getThreadId(), conn->getSockfd());
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
        RedisReplyPtr reply = iter->redisCommand(command->ptr, sdslen(command->ptr));
        if (reply != nullptr && reply->type == REDIS_REPLY_INTEGER) {
            dbsize += reply->integer;
        } else {
            std::string str = it->second->setTcpClientInfo(iter->errstr, iter->ip, iter->port);
            conn->outputBuffer()->append(str.c_str(), str.size());
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
        RedisReplyPtr reply = iter->redisCommand(command->ptr, sdslen(command->ptr));
        if (reply == nullptr && reply->type != REDIS_REPLY_STATUS) {
            std::string str = it->second->setTcpClientInfo(iter->errstr, iter->ip, iter->port);
            conn->outputBuffer()->append(str.c_str(), str.size());
            return true;
        }
    }

    addReply(conn->outputBuffer(), shared.ok);
    return true;
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
        std::vector<RedisReplyPtr> vectors;
        vectors.push_back(reply);
        it->second.insert(std::make_pair(sockfd, vectors));
    } else {
        iter->second.push_back(reply);
    }
}
