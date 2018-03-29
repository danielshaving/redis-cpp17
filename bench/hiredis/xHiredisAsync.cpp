#include "xHiredisAsync.h"

xHiredisAsync::xHiredisAsync(xEventLoop *loop,int threadCount,const char *ip,int16_t port)
:hiredis(loop),
connectCount(0),
loop(loop),
cron(true)
{
	hiredis.getPool().setThreadNum(threadCount);
	hiredis.getPool().start();
	for(int i = 0; i < sessionCount; i++)
	{
		TcpClientPtr client(new xTcpClient(hiredis.getPool().getNextLoop(),hiredis.setCount()));
		hiredis.insertTcpMap(hiredis.getCount(),client);
		client->setConnectionErrorCallBack(std::bind(&xHiredisAsync::redisErrorConnCallBack,this,std::placeholders::_1));
		client->setConnectionCallback(std::bind(&xHiredisAsync::redisConnCallBack,this,std::placeholders::_1));
		client->setMessageCallback(std::bind(&xHiredis::redisReadCallBack,&hiredis,std::placeholders::_1,std::placeholders::_2));
		client->asyncConnect(ip,port);
	}

	std::unique_lock<std::mutex> lk(hiredis.getMutex());
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}

	sessionCount = sessionCount * benchCount;
}

void xHiredisAsync::redisErrorConnCallBack(const std::any &context)
{
	hiredis.eraseTcpMap(std::any_cast<int32_t>(context));
}

void xHiredisAsync::redisConnCallBack(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		RedisAsyncContextPtr ac (new xRedisAsyncContext(conn->intputBuffer(),conn,conn->getSockfd()));
		hiredis.insertRedisMap(conn->getSockfd(), ac);
		connectCount++;
		condition.notify_one();
	}
	else
	{
		if(--connectCount == 0)
		{
			test_cond(true);
		}

		const std::any &context = conn->getContext();
		hiredis.eraseTcpMap(std::any_cast<int32_t>(context));
		hiredis.eraseRedisMap(conn->getSockfd());
	}
}

void xHiredisAsync::setCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);
	if(reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_NIL)
	{
		assert(false);
	}

	std::thread::id threadId = std::any_cast< std::thread::id>(privdata);
	assert(threadId == std::this_thread::get_id());
	sconnetCount++;
}

void xHiredisAsync::getCallback(const RedisAsyncContextPtr &c,redisReply *reply,const std::any &privdata)
{
	assert(reply != nullptr);

	if(reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_NIL)
	{
		assert(false);
	}

	std::thread::id threadId = std::any_cast< std::thread::id>(privdata);
	assert(threadId == std::this_thread::get_id());
	if(++gconnetCount == sessionCount  && sconnetCount == sessionCount)
	{
		test_cond(true);
	}
}

void xHiredisAsync::serverCron(const std::any &context)
{
	if(cron && gconnetCount == sessionCount  && sconnetCount == sessionCount)
	{
		test("Redis async close safe test");
		{
			//std::unique_lock<std::mutex> lk(hiredis.getMutex());
			for(auto &it : hiredis.getClientMap())
			{
				it.second->disConnect();
			}
		}
		cron = false;
	}
}

 int main(int argc, char* argv[])
 {
 	if (argc != 5)
 	{
 		fprintf(stderr, "Usage: client <host_ip> <port> <sessionCount> <threadCount> \n ");
 	}
 	else
 	{
 		test("Redis async Connect test ");
 		const char* ip = argv[1];
 		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
 		sessionCount = atoi(argv[3]);
 		int threadCount = atoi(argv[4]);
 		xEventLoop loop;
		xHiredisAsync redisAsync(&loop,threadCount,ip,port);
		test_cond(true);

		test("Redis async multithreaded safe test ");

		{
			//std::unique_lock<std::mutex> lk(redisAsync.getHiredis()->getMutex());
			auto  &redisMap = redisAsync.getHiredis()->getRedisMap();
			int32_t count = 0;
			for(auto it = redisMap.begin(); it != redisMap.end();)
			{
				if(count++ >= sessionCount)
				{
					break;
				}

				auto redis = it->second;
				std::thread::id  threadId = redis->getServerConn()->getLoop()->getThreadId();

				redis->redisAsyncCommand(std::bind(&xHiredisAsync::setCallback,
						&redisAsync,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
						threadId,"set key%d %d",count,count);

				redis->redisAsyncCommand(std::bind(&xHiredisAsync::getCallback,
						&redisAsync,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
						threadId,"get key%d",count);

				if(++it == redisMap.end())
				{
					it = redisMap.begin();
				}
			}
		}

		loop.runAfter(1.0,nullptr,true,std::bind(&xHiredisAsync::serverCron,&redisAsync,std::placeholders::_1));
 		loop.run();
 	}
 	return 0;
 }



