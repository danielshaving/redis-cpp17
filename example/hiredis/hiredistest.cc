#include "hiredistest.h"

HiredisTest::HiredisTest(EventLoop *loop,int8_t threadCount,
		int16_t sessionCount,int32_t messageCount,const char *ip,int16_t port)
:hiredis(loop,sessionCount),
 connectCount(0),
 sessionCount(sessionCount),
 loop(loop),
 messageCount(messageCount),
 count(0)
{
	if (threadCount <=0 )
	{
		threadCount = 1;
	}

	hiredis.setConnectionCallback(std::bind(&HiredisTest::connectionCallback,
			this,std::placeholders::_1));
	hiredis.setDisconnectionCallback(std::bind(&HiredisTest::disConnectionCallback,
			this,std::placeholders::_1));

	hiredis.setThreadNum(threadCount);
	hiredis.start(ip,port);

	std::unique_lock<std::mutex> lk(mutex);
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}
	printf("all connect\n");
}

HiredisTest::~HiredisTest()
{

}

void HiredisTest::connectionCallback(const TcpConnectionPtr &conn)
{
	connectCount++;
	condition.notify_one();
	std::unique_lock<std::mutex> lk(rmutex);
	redisAsyncConns[conn->getSockfd()] = conn;
}

void HiredisTest::disConnectionCallback(const TcpConnectionPtr &conn)
{
	connectCount--;
	std::unique_lock<std::mutex> lk(rmutex);
	redisAsyncConns.erase(conn->getSockfd());
}

void HiredisTest::setCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(strcmp(reply->str,"OK") == 0);
}

void HiredisTest::getCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STRING);
}

void HiredisTest::hsetCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_INTEGER);
	assert(reply->len == 0);
	assert(reply->str == nullptr);
	assert(reply->integer == 1);
}

void HiredisTest::hgetCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_ARRAY);
	int32_t count = std::any_cast<int32_t>(privdata);
	int64_t replyCount = 0;
	string2ll(reply->str,reply->len,&replyCount);
	assert(count == replyCount);
}

void HiredisTest::hgetallCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_ARRAY);
	int32_t count = std::any_cast<int32_t>(privdata);
	assert(reply->len == count);

	for(int i = 0; i < reply->len; i += 2 )
	{
		assert(reply->element[i]);
		assert(reply->element[i]->type == REDIS_REPLY_STRING);
		assert(reply->element[i + 1]);
		assert(reply->element[i + 1]->type == REDIS_REPLY_STRING);

		{
			int64_t value = 0;
			string2ll(reply->element[i]->str,reply->element[i]->len,&value);
			assert(value == i);
		}

		{
			int64_t value = 0;
			string2ll(reply->element[i + 1]->str,reply->element[i + 1]->len,&value);
			assert(value == i);
		}
	}
}

void HiredisTest::lpushCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	//printf("lpush\n");
}

void HiredisTest::rpushCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{

}

void HiredisTest::rpopCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{

}

void HiredisTest::lpopCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	//printf("lpop\n");
}

void HiredisTest::subscribeCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply->type == REDIS_REPLY_ARRAY);
	assert(!reply->element.empty());
	assert(reply->element.size() == reply->elements);
}

void HiredisTest::monitorCallback(const RedisAsyncContextPtr &c,
				const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(strcmp(reply->str,"OK") == 0);
}

void HiredisTest::publishCallback(const RedisAsyncContextPtr &c,
				const RedisReplyPtr &reply,const std::any &privdata)
{
	assert(reply->type == REDIS_REPLY_STATUS);
}

void HiredisTest::publish()
{
	auto redis = getRedisAsyncContext();
	assert(redis != nullptr);
	redis->redisAsyncCommand(std::bind(&HiredisTest::publishCallback,
			this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
			nullptr,"publish test test");
}

void HiredisTest::subscribe()
{
	auto redis = getRedisAsyncContext();
	assert(redis != nullptr);
	redis->redisAsyncCommand(std::bind(&HiredisTest::subscribeCallback,
			this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
			nullptr,"subscribe test");
}

void HiredisTest::monitor()
{
	auto redis = getRedisAsyncContext();
	assert(redis != nullptr);
	redis->redisAsyncCommand(std::bind(&HiredisTest::monitorCallback,
			this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
			nullptr,"monitor");
}

void HiredisTest::string()
{
	int32_t k = 0;
	for(; k < messageCount; k++)
	{
		auto redis = getRedisAsyncContext();
		assert(redis != nullptr);
		redis->redisAsyncCommand(std::bind(&HiredisTest::setCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"set string%d %d",k,k);

		redis->redisAsyncCommand(std::bind(&HiredisTest::getCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"get string%d",k);
	}
}

void HiredisTest::hash()
{
	int32_t count = 0;
	for(; count < messageCount; count++)
	{
		auto redis = getRedisAsyncContext();
		assert(redis != nullptr);
		redis->redisAsyncCommand(std::bind(&HiredisTest::hsetCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"hset %d hash %d",count,count);

		redis->redisAsyncCommand(std::bind(&HiredisTest::hsetCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"hset hash %d %d",count,count);

		redis->redisAsyncCommand(std::bind(&HiredisTest::hgetCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				count,"hget %d hash",count);
	}

	auto redis = getRedisAsyncContext();
	assert(redis != nullptr);

	redis->redisAsyncCommand(std::bind(&HiredisTest::hgetallCallback,
			this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
			count,"hgetall hash");

	printf("hash done\n");
}

void HiredisTest::list()
{
	int32_t count = 0;
	for(; count < messageCount; count++)
	{
		auto redis = getRedisAsyncContext();
		assert(redis != nullptr);

		redis->redisAsyncCommand(std::bind(&HiredisTest::lpushCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"lpush list%d %d",count,count);

		redis->redisAsyncCommand(std::bind(&HiredisTest::lpopCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"lpop list%d %d",count,count);
	}

	for(; count < messageCount; count++)
	{
		auto redis = getRedisAsyncContext();
		assert(redis != nullptr);
		redis->redisAsyncCommand(std::bind(&HiredisTest::rpushCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"rpush list%d %d",count,count);

		redis->redisAsyncCommand(std::bind(&HiredisTest::rpopCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"rpop list%d %d",count,count);
	}

}


RedisAsyncContextPtr HiredisTest::getRedisAsyncContext()
{
	std::unique_lock<std::mutex> lk(rmutex);
	if (redisAsyncConns.empty())
	{
		return nullptr;
	}

	auto conn = redisAsyncConns.begin()->second;
	assert(conn->getContext().has_value());
	const RedisAsyncContextPtr &ac =
				std::any_cast<RedisAsyncContextPtr>(conn->getContext());
	return ac;
}

int main(int argc,char *argv[])
{
 	if (argc != 6)
 	{
 		fprintf(stderr,"Usage: client <host_ip> <port> <sessionCount> <threadCount> <messageCount> \n ");
 	}
 	else
 	{
 		const char *ip = argv[1];
 		uint16_t port = atoi(argv[2]);
 		int16_t sessionCount = atoi(argv[3]);
 		int8_t threadCount = atoi(argv[4]);
 		int32_t messageCount = atoi(argv[5]);

 		EventLoop loop;
 		HiredisTest hiredis(&loop,
				threadCount,sessionCount,messageCount,ip,port);

// 		hiredis.monitor();
//		hiredis.subscribe();
//		hiredis.string();
//		hiredis.hash();
		hiredis.list();
 		loop.run();
 	}
 	return 0;
}



