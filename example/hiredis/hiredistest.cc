#include "hiredistest.h"

HiredisTest::HiredisTest(EventLoop *loop, int8_t threadCount,
	int16_t sessionCount, int32_t messageCount, const char *ip, int16_t port)
	:hiredis(loop, sessionCount,ip,port),
	connectCount(0),
	sessionCount(sessionCount),
	loop(loop),
	messageCount(messageCount),
	count(0)
{
	unlockScript = sdsnew("if redis.call('get', KEYS[1]) == ARGV[1] \
				 then return redis.call('del', KEYS[1]) else return 0 end");
	if (threadCount <= 0)
	{
		threadCount = 1;
	}

	hiredis.setConnectionCallback(std::bind(&HiredisTest::connectionCallback,
		this, std::placeholders::_1));
	hiredis.setDisconnectionCallback(std::bind(&HiredisTest::disConnectionCallback,
		this, std::placeholders::_1));

	hiredis.setThreadNum(threadCount);
	hiredis.poolStart();
	hiredis.start();

	std::unique_lock<std::mutex> lk(mutex);
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}
}

HiredisTest::~HiredisTest()
{
	sdsfree(unlockScript);
}

void HiredisTest::connectionCallback(const TcpConnectionPtr &conn)
{
	connectCount++;
	condition.notify_one();
}

void HiredisTest::disConnectionCallback(const TcpConnectionPtr &conn)
{
	connectCount--;
}

void HiredisTest::setCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(strcmp(reply->str, "OK") == 0);
}

void HiredisTest::getCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STRING);
}

void HiredisTest::hsetCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_INTEGER);
	assert(reply->len == 0);
	assert(reply->str == nullptr);
	assert(reply->integer == 1);
}

void HiredisTest::hgetCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_ARRAY);
	int32_t count = std::any_cast<int32_t>(privdata);
	int64_t replyCount = 0;
	string2ll(reply->str, reply->len, &replyCount);
	assert(count == replyCount);
}

void HiredisTest::hgetallCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_ARRAY);
	int32_t count = std::any_cast<int32_t>(privdata);
	assert(reply->len == count);

	for (int i = 0; i < reply->len; i += 2)
	{
		assert(reply->element[i]);
		assert(reply->element[i]->type == REDIS_REPLY_STRING);
		assert(reply->element[i + 1]);
		assert(reply->element[i + 1]->type == REDIS_REPLY_STRING);

		{
			int64_t value = 0;
			string2ll(reply->element[i]->str, reply->element[i]->len, &value);
			assert(value == i);
		}

		{
			int64_t value = 0;
			string2ll(reply->element[i + 1]->str, reply->element[i + 1]->len, &value);
			assert(value == i);
		}
	}
}


void HiredisTest::infoCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata)
{
	printf("%s\n",reply->str);
}

void HiredisTest::lrangeCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr && reply->type == REDIS_REPLY_ARRAY);
	assert(reply != nullptr && reply->element.size() == messageCount);
}

void HiredisTest::lpushCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	//printf("lpush\n");
}

void HiredisTest::rpushCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{

}

void HiredisTest::rpopCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{

}

void HiredisTest::lpopCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	//printf("lpop\n");
}

void HiredisTest::subscribeCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply->type == REDIS_REPLY_ARRAY);
	assert(!reply->element.empty());
}

void HiredisTest::monitorCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(strcmp(reply->str, "OK") == 0);
}

void HiredisTest::publishCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply->type == REDIS_REPLY_STATUS);
}

void HiredisTest::redUnlcokCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_INTEGER);
	assert(reply->integer == 1);
}

void HiredisTest::redLockCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(privdata.has_value());
	const RedLockCallbackPtr &callback = std::any_cast<RedLockCallbackPtr>(privdata);
	if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0)
	{
		callback->doingCallback();
	}
	else
	{
		//get lock failure rand retry
		printf("get lock falure\n");
		auto willCallback = c->getRedisAsyncCommand(std::bind(&HiredisTest::redLockCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			callback, "set %s %s px %d nx", callback->resource, callback->val, callback->ttl);

		double retry = ((double)(rand() % 100) / 100);
		c->redisConn->getLoop()->runAfter(retry, false, std::move(willCallback));
	}
}

void HiredisTest::setRedLockCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(strcmp(reply->str, "OK") == 0);
	assert(privdata.has_value());
	const std::function<void()> &callback = std::any_cast<std::function<void()>>(privdata);
	callback();
}

void HiredisTest::redlock(const char *resource, const char *val, const int32_t ttl)
{
	auto redis = hiredis.getRedisAsyncContext();
	assert(redis != nullptr);

	for (; count < messageCount; count++)
	{
		RedLockCallbackPtr callback(new RedLockCallback);
		auto willCallback = redis->getRedisAsyncCommand(std::bind(&HiredisTest::redUnlcokCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "eval %s %d %s %s", unlockScript, 1, resource, val);

		auto doingCallback = redis->getRedisAsyncCommand(std::bind(&HiredisTest::setRedLockCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			willCallback, "set redlock redlock");

		auto wasCallback = redis->getRedisAsyncCommand(std::bind(&HiredisTest::redLockCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "set %s %s px %d nx", resource, val, ttl);

		assert(wasCallback != nullptr);
		assert(willCallback != nullptr);
		assert(doingCallback != nullptr);

		callback->resource = resource;
		callback->ttl = ttl;
		callback->val = val;
		callback->wasCallback = std::move(wasCallback);
		callback->doingCallback = std::move(doingCallback);

		//thread safe
		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::redLockCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			callback, "set %s %s px %d nx", resource, val, ttl) == REDIS_OK);
	}
}

void HiredisTest::publish()
{
	auto redis = hiredis.getRedisAsyncContext();
	assert(redis != nullptr);
	assert(redis->redisAsyncCommand(std::bind(&HiredisTest::publishCallback,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
		nullptr, "publish test test") == REDIS_OK);
}

void HiredisTest::subscribe()
{
	auto redis = hiredis.getRedisAsyncContext();
	assert(redis != nullptr);
	assert(redis->redisAsyncCommand(std::bind(&HiredisTest::subscribeCallback,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
		nullptr, "subscribe test") == REDIS_OK);
}

void HiredisTest::monitor()
{
	auto redis = hiredis.getRedisAsyncContext();
	assert(redis != nullptr);
	assert(redis->redisAsyncCommand(std::bind(&HiredisTest::monitorCallback,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
		nullptr, "monitor") == REDIS_OK);
}

void HiredisTest::string()
{
	int32_t k = 0;
	for (; k < messageCount; k++)
	{
		auto redis = hiredis.getRedisAsyncContext();
		assert(redis != nullptr);
		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::setCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "set string%d %d", k, k) == REDIS_OK);

		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::getCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "get string%d", k) == REDIS_OK);
	}
}

void HiredisTest::hash()
{
	int32_t count = 0;
	for (; count < messageCount; count++)
	{
		auto redis = hiredis.getRedisAsyncContext();
		assert(redis != nullptr);
		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::hsetCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "hset %d hash %d", count, count) == REDIS_OK);

		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::hsetCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "hset hash %d %d", count, count) == REDIS_OK);

		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::hgetCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			count, "hget %d hash", count) == REDIS_OK);
	}

	auto redis = hiredis.getRedisAsyncContext();
	assert(redis != nullptr);

	assert(redis->redisAsyncCommand(std::bind(&HiredisTest::hgetallCallback,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
		count, "hgetall hash") == REDIS_OK);

	printf("hash done\n");
}

void HiredisTest::lrange()
{
	//thread 1
	auto redis = hiredis.getRedisAsyncContext();
	assert(redis != nullptr);

	int32_t count = 0;
	for (; count < messageCount; count++)
	{
		assert(redis->redisAsyncCommand(nullptr, nullptr, "lpush mylist foo") == REDIS_OK);
	}

	assert(redis->redisAsyncCommand(std::bind(&HiredisTest::lrangeCallback,
				this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
				nullptr, "lrange mylist 0 %d", messageCount - 1) == REDIS_OK);
}

void HiredisTest::info()
{
	auto redis = hiredis.getRedisAsyncContext();
	assert(redis != nullptr);

	assert(redis->redisAsyncCommand(std::bind(&HiredisTest::infoCallback,
					this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
					nullptr, "info") == REDIS_OK);

}

void HiredisTest::list()
{
	int32_t count = 0;
	for (; count < messageCount; count++)
	{
		auto redis = hiredis.getRedisAsyncContext();
		assert(redis != nullptr);

		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::lpushCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "lpush list%d %d", count, count) == REDIS_OK);

		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::lpopCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "lpop list%d %d", count, count) == REDIS_OK);
	}

	for (; count < messageCount; count++)
	{
		auto redis = hiredis.getRedisAsyncContext();
		assert(redis != nullptr);
		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::rpushCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "rpush list%d %d", count, count) == REDIS_OK);

		assert(redis->redisAsyncCommand(std::bind(&HiredisTest::rpopCallback,
			this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
			nullptr, "rpop list%d %d", count, count) == REDIS_OK);
	}
}


int main(int argc, char *argv[])
{
	#ifdef _WIN64
		WSADATA wsaData;
		int32_t iRet = WSAStartup(MAKEWORD(2, 2), &wsaData);
		assert(iRet == 0);
	#else
		signal(SIGPIPE, SIG_IGN);
		signal(SIGHUP, SIG_IGN);
	#endif

	const char *ip = "127.0.0.1";
	uint16_t port = 6379;
	int16_t sessionCount = 100;
	int8_t threadCount = 4;
	int32_t messageCount = 100000;

	EventLoop loop;
	HiredisTest hiredis(&loop,
		threadCount, sessionCount, messageCount, ip, port);

	LOG_INFO << "all connect success";
	// 		hiredis.monitor();
	//		hiredis.subscribe();
	//		hiredis.string();
	//		hiredis.hash();
	//		hiredis.list();
	//		hiredis.lrange();
	hiredis.info();
	//hiredis.redlock("test","test",100000);
	loop.run();

	return 0;
}

