#include "hiredistest.h"

HiredisTest::HiredisTest(EventLoop *loop,int8_t threadCount,
		int16_t sessionCount,int32_t messageCount,const char *ip,int16_t port)
:hiredis(loop),
 connectCount(0),
 sessionCount(sessionCount),
 loop(loop),
 messageCount(messageCount),
 count(0)
{
	if(threadCount == 0)
	{
		threadCount = 1;
	}

	hiredis.setThreadNum(threadCount);
	hiredis.start();

	for(int i = 0; i < sessionCount; i++)
	{
		TcpClientPtr client(new xTcpClient(hiredis.getPool().getNextLoop(),ip,port,nullptr));
		client->setConnectionCallback(std::bind(&HiredisTest::redisConnCallBack,this,std::placeholders::_1));
		client->setMessageCallback(std::bind(&xHiredis::redisReadCallBack,
				&hiredis,std::placeholders::_1,std::placeholders::_2));
		client->asyncConnect();
		hiredis.pushTcpClient(client);
	}

	std::unique_lock<std::mutex> lk(hiredis.getMutex());
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}
}

HiredisTest::~HiredisTest()
{

}

void HiredisTest::redisConnCallBack(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		RedisAsyncContextPtr ac(new RedisAsyncContext(conn->intputBuffer(),conn));
		hiredis.insertRedisMap(conn->getSockfd(),ac);
		connectCount++;
		condition.notify_one();
	}
	else
	{
		if(--connectCount == 0)
		{
			hiredis.clearTcpClient();
		}
		hiredis.eraseRedisMap(conn->getSockfd());
	}
}


void HiredisTest::setCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(reply->str == nullptr);
	assert(reply->element == nullptr);
}

void HiredisTest::getCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STRING);
	int64_t replyCount = 0;
	string2ll(reply->str,reply->len,&replyCount);
	assert(count++ == replyCount);
}

void HiredisTest::hsetCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_INTEGER);
	assert(reply->len == 0);
	assert(reply->str == nullptr);
	assert(reply->element == nullptr);
	assert(reply->integer == 1);
}

void HiredisTest::hgetCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_ARRAY);
	int32_t count = std::any_cast<int32_t>(privdata);
	int64_t replyCount = 0;
	string2ll(reply->str,reply->len,&replyCount);
	assert(count == replyCount);
}

void HiredisTest::hgetallCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
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

void HiredisTest::lpushCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{

}

void HiredisTest::rpushCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{

}

void HiredisTest::rpopCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{

}

void HiredisTest::lpopCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{

}

void HiredisTest::string()
{
	int32_t count = 0;
	for(; cout < messageCount; count++)
	{
		auto redis = hiredis.getIteratorNode();
		assert(redis != nullptr);
		redis->redisAsyncCommand(std::bind(&HiredisTest::setCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"set string%d %d",count,count);

		redis->redisAsyncCommand(std::bind(&HiredisTest::getCallback,
							this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
							nullptr,"get string%d",count);
	}
}

void HiredisTest::hash()
{
	int32_t count = 0;
	for(; cout < messageCount; count++)
	{
		auto redis = hiredis.getIteratorNode();
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

	auto redis = hiredis.getIteratorNode();
	assert(redis != nullptr);

	redis->redisAsyncCommand(std::bind(&HiredisTest::hgetallCallback,
					this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
					count,"hgetall hash");

	printf("hash done\n");
}

void HiredisTest::list()
{
	int32_t count = 0;
	for(; cout < messageCount; i++)
	{
		auto redis = hiredis.getIteratorNode();
		assert(redis != nullptr);
		redis->redisAsyncCommand(std::bind(&HiredisTest::hsetCallback,
				this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
				nullptr,"lpush list%d %d",count,count);

		redis->redisAsyncCommand(std::bind(&HiredisTest::hsetCallback,
					this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
					nullptr,"lpop list%d %d",count,count);
	}

}

int main(int argc,char* argv[])
{
 	if (argc != 6)
 	{
 		fprintf(stderr, "Usage: client <host_ip> <port> <sessionCount> <threadCount> <messageCount> \n ");
 	}
 	else
 	{
 		const char *ip = argv[1];
 		uint16_t port = atoi(argv[2]);
 		int16_t sessionCount = atoi(argv[3]);
 		int8_t threadCount = atoi(argv[4]);
 		int32_t messageCount = atoi(argv[5]);

 		EventLoop loop;
		HiredisTest hiredis(&loop,threadCount,sessionCount,messageCount,ip,port);
		if(threadCount == 0)
		{
			hiredis.string();
		}

		hiredis.hash();
		hiredis.list();
 		loop.run();
 	}
 	return 0;
}



