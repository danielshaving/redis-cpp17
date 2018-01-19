#include "xHiredisAsync.h"

xHiredisAsync::xHiredisAsync(xEventLoop * loop,int threadCount,int sessionCount,const char *ip,int32_t port)
:hiredis(loop),
connectCount(0),
loop(loop)
{
	hiredis.getPoll().setThreadNum(threadCount);
	hiredis.getPoll().start();
	for(int i = 0; i < sessionCount; i++)
	{
		hiredis.setCount();
		int c = hiredis.getCount();
		xTcpClientPtr client(new xTcpClient(hiredis.getPoll().getNextLoop(),(void*)&c));
		hiredis.insertTcpMap(c,client);
		client->setConnectionErrorCallBack(std::bind(&xHiredisAsync::redisErrorConnCallBack,this,std::placeholders::_1));
		client->setConnectionCallback(std::bind(&xHiredisAsync::redisConnCallBack,this,std::placeholders::_1,std::placeholders::_2));
		client->setMessageCallback(std::bind(&xHiredis::redisReadCallBack,&hiredis,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
		client->connect(ip,port);
	}

	std::unique_lock<std::mutex> lk(hiredis.getMutex());
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}

	connectCount = 0;
}


void xHiredisAsync::redisErrorConnCallBack(void *data)
{
	hiredis.eraseTcpMap(*(int*)data);
}


void xHiredisAsync::redisConnCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		xRedisAsyncContextPtr ac (new xRedisAsyncContext());
		ac->c->reader->buf = &(conn->recvBuff);
		ac->conn = conn;
		ac->c->fd = conn->getSockfd();
		hiredis.insertRedisMap(conn->getSockfd(), ac);
		connectCount++;
		condition.notify_one();
	}
	else
	{
		hiredis.eraseTcpMap(*(int*)data);
		hiredis.eraseRedisMap(conn->getSockfd());
	}
}


static void getCallback(const xRedisAsyncContextPtr &c, void *r, void *privdata)
{
	redisReply *reply = (redisReply*) r;
	assert(reply != nullptr);

	if(reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_NIL)
	{
		assert(false);
	}

	long long threadId = 0;
	string2ll(reply->str,reply->len,&threadId);
	if(threadId != c->conn->getLoop()->getThreadId())
	{
		printf(" %d %d\n",threadId, getpid());
	    	assert(false);
	}

	if(++connetCount == sessionCount )
	{
		test_cond(true);
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
 		connetCount = 0;
 		const char* ip = argv[1];
 		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
 		sessionCount = atoi(argv[3]);
 		int threadCount = atoi(argv[4]);

 		xEventLoop loop;
 		test("Redis async test ");
		xHiredisAsync redisAsync(&loop,threadCount,sessionCount,ip,port);
		int count = 0;
		test_cond(true);
		test("Redis async multithreaded safe test ");
		auto  &redisMap = redisAsync.getHiredis()->getRedisMap();
		for(auto it = redisMap.begin(); it != redisMap.end(); ++it)
		{
			count ++;
			redisAsyncCommand(it->second,nullptr,nullptr,"set key%d %d",count,it->second->conn->getLoop()->getThreadId());
			redisAsyncCommand(it->second,getCallback,nullptr,"get key%d",count);
		}

 		loop.run();
 	}
 	return 0;
 }



