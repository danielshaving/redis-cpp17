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
		TcpClientPtr client(new TcpClient(hiredis.getPool().getNextLoop(),ip,port,nullptr));
		client->setConnectionCallback(std::bind(&HiredisTest::redisConnCallBack,this,std::placeholders::_1));
		client->setMessageCallback(std::bind(&Hiredis::redisReadCallBack,
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

void HiredisTest::sync()
{
    unsigned int j;
    RedisContextPtr c;
    RedisReplyPtr reply;
    const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? atoi(argv[2]) : 6379;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(hostname,port,timeout);
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n",c->errstr);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    /* PING server */
    reply = c->redisCommand("PING");
    printf("PING: %s\n",reply->str);

    /* Set a key */
    reply = c->redisCommand("SET %s %s","foo","hello world");
    printf("SET: %s\n",reply->str);

    /* Set a key using binary safe API */
    reply = c->redisCommand("SET %b %b","bar",(size_t) 3,"hello",(size_t) 5);
    printf("SET (binary API): %s\n",reply->str);

    /* Try a GET and two INCR */
    reply = c->redisCommand("GET foo");
    printf("GET foo: %s\n", reply->str);

    reply = c->redisCommand("INCR counter");
    printf("INCR counter: %lld\n", reply->integer);
    /* again ... */
    reply = c->redisCommand("INCR counter");
    printf("INCR counter: %lld\n", reply->integer);

    /* Create a list of numbers, from 0 to 9 */
    reply = redisCommand(c,"DEL mylist");
    for (j = 0; j < 10; j++)
    {
        char buf[64];
        snprintf(buf,64,"%u",j);
        reply = c->redisCommand("LPUSH mylist element-%s",buf);
    }

    /* Let's check what we have inside the list */
    reply = c->redisCommand("LRANGE mylist 0 -1");
    if (reply->type == REDIS_REPLY_ARRAY)
    {
        for (j = 0; j < reply->elements; j++)
        {
            printf("%u) %s\n",j,reply->element[j]->str);
        }
    }
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

}

void HiredisTest::string()
{
	int32_t k = 0;
	for(; k < messageCount; k++)
	{
		auto redis = hiredis.getIteratorNode();
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
	for(; count < messageCount; count++)
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
 		fprintf(stderr, "Usage: client <host_ip> <port> <sessionCount> \
 				<threadCount> <messageCount> \n ");
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

		hiredis.string();


		//hiredis.hash();
		//hiredis.list();
 		loop.run();
 	}
 	return 0;
}



