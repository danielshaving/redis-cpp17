#include "xHiredisAsync.h"


xHiredisAsync::xHiredisAsync(xEventLoop * loop,int threadCount,int sessionCount,const char *ip,int16_t port)
:hiredis(loop),
connectCount(0),
loop(loop)
{
	hiredis.getPool().setThreadNum(threadCount);
	hiredis.getPool().start();
	for(int i = 0; i < sessionCount; i++)
	{
		hiredis.setCount();
		int c = hiredis.getCount();
		xTcpClientPtr client(new xTcpClient(hiredis.getPool().getNextLoop(),c));
		hiredis.insertTcpMap(c,client);
		client->setConnectionErrorCallBack(std::bind(&xHiredisAsync::redisErrorConnCallBack,this,std::placeholders::_1));
		client->setConnectionCallback(std::bind(&xHiredisAsync::redisConnCallBack,this,std::placeholders::_1));
		client->setMessageCallback(std::bind(&xHiredis::redisReadCallBack,&hiredis,std::placeholders::_1,std::placeholders::_2));
		client->connect(ip,port);
	}

	std::unique_lock<std::mutex> lk(hiredis.getMutex());
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}

	connectCount = 0;
}


void xHiredisAsync::redisErrorConnCallBack(const std::any &context)
{
	hiredis.eraseTcpMap(std::any_cast<int>(context));
}


void xHiredisAsync::redisConnCallBack(const xTcpconnectionPtr& conn)
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
		hiredis.eraseTcpMap(*(std::any_cast<int32_t>(conn->getContext())));
		hiredis.eraseRedisMap(conn->getSockfd());
	}
}


std::atomic<int32_t> sconnetCount = 0;
static void setCallback(const xRedisAsyncContextPtr &c, void *r, const std::any &privdata)
{
	redisReply *reply = (redisReply*) r;
	assert(reply != nullptr);

	if(reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_NIL)
	{
		assert(false);
	}

	std::thread::id threadId = std::any_cast< std::thread::id>(privdata);
	assert(threadId == std::this_thread::get_id());
	sconnetCount++;
}

std::atomic<int32_t> gconnetCount = 0;

static void getCallback(const xRedisAsyncContextPtr &c, void *r, const std::any &privdata)
{
	redisReply *reply = (redisReply*) r;
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


 int main(int argc, char* argv[])
 {
 	if (argc != 5)
 	{
 		fprintf(stderr, "Usage: client <host_ip> <port> <sessionCount> <threadCount> \n ");
 	}
 	else
 	{
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
			it->second->redisAsyncCommand(setCallback,it->second->conn->getLoop()->getThreadId(),"set key%d %d",count,count);
			it->second->redisAsyncCommand(getCallback,it->second->conn->getLoop()->getThreadId(),"get key%d",count);
		}

 		loop.run();
 	}
 	return 0;
 }



