#include "xHiredisTest.h"

xHiredisTest::xHiredisTest(xEventLoop *loop,int8_t threadCount,
		int16_t sessionCount,int32_t messageCount,const char *ip,int16_t port)
:hiredis(loop),
 connectCount(0),
 sessionCount(sessionCount),
 loop(loop),
 messageCount(messageCount)
{
	hiredis.setThreadNum(threadCount);
	hiredis.start();

	for(int i = 0; i < sessionCount; i++)
	{
		TcpClientPtr client(new xTcpClient(hiredis.getPool().getNextLoop(),ip,port,nullptr));
		client->setConnectionCallback(std::bind(&xHiredisTest::redisConnCallBack,this,std::placeholders::_1));
		client->setMessageCallback(std::bind(&xHiredis::redisReadCallBack,&hiredis,std::placeholders::_1,std::placeholders::_2));
		client->asyncConnect();
		hiredis.pushTcpClient(client);
	}
}

xHiredisTest::~xHiredisTest()
{

}

void xHiredisTest::redisConnCallBack(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		RedisAsyncContextPtr ac(new xRedisAsyncContext(conn->intputBuffer(),conn));
		hiredis.insertRedisMap(conn->getSockfd(),ac);
		connectCount++;

		int32_t count = 0;
		while(1)
		{

			if(count++ >= messageCount)
			{
				break;
			}

			auto redis = hiredis.getIteratorNode();
			redis->redisAsyncCommand(std::bind(&xHiredisTest::hsetCallback,
					this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
					nullptr,"hset zhanghao key%d %d",count,count);

			redis->redisAsyncCommand(std::bind(&xHiredisTest::hgetCallback,
					this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
					count,"hget zhanghao key%d",count);
		}

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

void xHiredisTest::hsetCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_INTEGER);
	assert(reply->len == 0);
	assert(reply->str == nullptr);
	assert(reply->element == nullptr);
	assert(reply->integer == 1);
}

void xHiredisTest::hgetCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STRING);
	int32_t count = std::any_cast<int32_t>(privdata);
	int64_t replyCount = 0;
	string2ll(reply->str,reply->len,&replyCount);
	assert(count == replyCount);
}

int main(int argc,char* argv[])
{
 	if (argc != 6)
 	{
 		fprintf(stderr, "Usage: client <host_ip> <port> <sessionCount> <threadCount>  <messageCount> \n ");
 	}
 	else
 	{
 		const char *ip = argv[1];
 		uint16_t port = atoi(argv[2]);
 		int16_t sessionCount = atoi(argv[3]);
 		int8_t threadCount = atoi(argv[4]);
 		int32_t messageCount = atoi(argv[5]);

 		xEventLoop loop;
		xHiredisTest async(&loop,threadCount,sessionCount,messageCount,ip,port);
 		loop.run();
 	}
 	return 0;
}



