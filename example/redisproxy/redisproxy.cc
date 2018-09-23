#include "redisproxy.h"
#include "socket.h"

RedisProxy::RedisProxy(const char *ip, int16_t port, int16_t redisPort,
	int16_t threadCount, int16_t sessionCount)
	:server(&loop, ip, port, nullptr),
	hiredis(&loop, sessionCount, ip, redisPort, true)
{
	unlockScript = sdsnew("if redis.call('get', KEYS[1]) == ARGV[1] \
				 then return redis.call('del', KEYS[1]) else return 0 end");

	server.setThreadNum(threadCount);
	server.setConnectionCallback(std::bind(&RedisProxy::proxyConnCallback,
		this, std::placeholders::_1));

	hiredis.setConnectionCallback(std::bind(&RedisProxy::redisConnCallback,
		this, std::placeholders::_1));
	hiredis.setDisconnectionCallback(std::bind(&RedisProxy::redisDisconnCallback,
		this, std::placeholders::_1));

	server.start();
	hiredis.setPool(server.getThreadPool());
	hiredis.start();
}

RedisProxy::~RedisProxy()
{
	sdsfree(unlockScript);
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

void RedisProxy::processCommand(const TcpConnectionPtr &conn, const std::string_view &view)
{
	conn->getLoop()->assertInLoopThread();
	auto redis = hiredis.getRedisAsyncContext(
		conn->getLoop()->getThreadId(), conn->getSockfd());
	if (redis == nullptr)
	{
		Buffer *buffer = conn->outputBuffer();
		std::string reply = hiredis.getTcpClientInfo(conn->getLoop()->getThreadId(), conn->getSockfd());
		buffer->append(reply.c_str(), reply.size());
		conn->sendPipe();
	}
	else
	{
		int32_t status = redis->proxyRedisvAsyncCommand(std::bind(&RedisProxy::proxyCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			view.data(), view.size(), conn);
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
		std::string r = hiredis.getTcpClientInfo(conn->getLoop()->getThreadId(), conn->getSockfd());
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
	conn->getLoop()->assertInLoopThread();
	conn->startRead();
	conn->setWriteCompleteCallback(WriteCompleteCallback());
}

void RedisProxy::proxyConnCallback(const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
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
