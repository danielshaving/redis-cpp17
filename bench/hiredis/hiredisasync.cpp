#include "hiredisasync.h"

HiredisAsync::HiredisAsync(EventLoop *loop,int8_t threadCount,const char *ip,int16_t port)
:hiredis(loop),
 connectCount(0),
 loop(loop),
 cron(true)
{
	if(threadCount == 0)
	{
		threadCount = 1;
	}

	hiredis.setThreadNum(threadCount);
	hiredis.start();

	for(int i = 0; i < sessionCount; i++)
	{
		TcpClientPtr client(new TcpClient(hiredis.getPool().getNextLoop(),
										   ip,port,nullptr));
		client->setConnectionCallback(std::bind(&HiredisAsync::redisConnCallBack,
												this,std::placeholders::_1));
		client->setMessageCallback(std::bind(&Hiredis::redisReadCallBack,
											 &hiredis,std::placeholders::_1,std::placeholders::_2));
		client->syncConnect();
		hiredis.pushTcpClient(client);
	}

	std::unique_lock<std::mutex> lk(hiredis.getMutex());
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}
}

HiredisAsync::~HiredisAsync()
{

}

void HiredisAsync::redisConnCallBack(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		RedisAsyncContextPtr ac(new xRedisAsyncContext(conn->intputBuffer(),conn));
		hiredis.insertRedisMap(conn->getSockfd(),ac);
		connectCount++;
		condition.notify_one();
	}
	else
	{
		if(--connectCount == 0)
		{
			test_cond(true);
			hiredis.clearTcpClient();
			endTime = mstime();
			printf("becnch seconds %02d\n",(endTime - startTime) / 1000);
		}
		hiredis.eraseRedisMap(conn->getSockfd());
	}
}

void HiredisAsync::setCallback(const RedisAsyncContextPtr &c,RedisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(reply->len == 2);
	assert(strcmp(reply->str,"ok") == 0);

	std::thread::id threadId = std::any_cast< std::thread::id>(privdata);
	assert(threadId == std::this_thread::get_id());
	sconnetCount++;
}

void HiredisAsync::getCallback(const RedisAsyncContextPtr &c,RedisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STRING);
	std::thread::id threadId = std::any_cast< std::thread::id>(privdata);
	assert(threadId == std::this_thread::get_id());
	if(++gconnetCount == sessionCount && sconnetCount == sessionCount)
	{
		test_cond(true);
	}
}

void HiredisAsync::serverCron()
{
	if(cron && gconnetCount == sessionCount && sconnetCount == sessionCount)
	{
		{
			std::unique_lock<std::mutex> lk(hiredis.getMutex());
			for(auto &it : hiredis.getTcpClient())
			{
				it->disConnect();
			}
		}
		cron = false;
	}
}


 int main(int argc,char* argv[])
 {
 	if (argc != 5)
 	{
 		fprintf(stderr,"Usage: client <host_ip> <port> <sessionCount> <threadCount> \n ");
 	}
 	else
 	{
 		const char *ip = argv[1];
 		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
 		sessionCount = atoi(argv[3]);
 		int8_t threadCount = atoi(argv[4]);

 		startTime = mstime();
 		EventLoop loop;
		HiredisAsync async(&loop,threadCount,ip,port);

		int32_t count = 0;
		while(1)
		{
			if(count++ >= sessionCount)
			{
				break;
			}

			auto redis = async.getHiredis()->getIteratorNode();
			std::thread::id threadId = redis->getServerConn()->getLoop()->getThreadId();

			redis->redisAsyncCommand(std::bind(&HiredisAsync::setCallback,
					&async,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
					threadId,"set key%d %d",count,count);

			redis->redisAsyncCommand(std::bind(&HiredisAsync::getCallback,
					&async,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
					threadId,"get key%d",count);
		}

		loop.runAfter(1.0,true,std::bind(&HiredisAsync::serverCron,&async));
 		loop.run();
 	}
 	return 0;
 }



