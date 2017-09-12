#include "xHiredisAsync.h"

xHiredis::xHiredis(xEventLoop *loop,xClient * owner)
:loop(loop),
 owner(owner),
 client(loop,this)
{
	client.setConnectionCallback(std::bind(&xHiredis::connCallBack, this, std::placeholders::_1,std::placeholders::_2));
	client.setMessageCallback( std::bind(&xHiredis::readCallBack, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
	client.setConnectionErrorCallBack(std::bind(&xHiredis::connErrorCallBack, this));
}


void xHiredis::connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		xSocket socket;
		xRedisAsyncContextPtr ac (new xRedisAsyncContext());
		ac->conn = conn;
		ac->c->fd = conn->getSockfd();
		{
			std::unique_lock<std::mutex> lk(mutex);
			redisAsyncs.insert(std::make_pair(conn->getSockfd(),ac));
		}

	}
	else
	{
		std::unique_lock<std::mutex> lk(mutex);
		redisAsyncs.erase(conn->getSockfd());
	}

}


void xHiredis::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
{
	xRedisAsyncContextPtr redis;

	{
		std::unique_lock<std::mutex> lk(mutex);
		auto it = redisAsyncs.find(conn->getSockfd());
		if(it == redisAsyncs.end())
		{
			assert(false);
		}

		redis = it->second;
	}

	 redisCallback cb;
	 xRedisContextPtr  c = (redis->c);
	 void  *reply = nullptr;
	 int status;
	 while((status = redisGetReply(c,&reply)) == REDIS_OK)
	 {
		 if(reply == nullptr)
		 {
			 break;
		 }

		 {
			 std::unique_lock<std::mutex> lk(mutex);
			 cb = std::move(redis->replies.front());
		 }

		 c->flags |= REDIS_IN_CALLBACK;
		 if(cb.fn)
		 {
			 cb.fn(redis,reply,cb.privdata);
		 }
		 else
		 {
			 redisAsyncs.erase(conn->getSockfd());
		 }

		 c->reader->fn->freeObjectFuc(reply);

		 c->flags &= ~REDIS_IN_CALLBACK;

		 {
			 std::unique_lock<std::mutex> lk(mutex);
			 redis->replies.pop_front();
		 }

	 }
	
}


void xHiredis::start()
{
	client.connect(owner->ip,owner->port);
}


void xHiredis::connErrorCallBack()
{
	LOG_WARN<<"connect server failure";
}


 static void getCallback(const xRedisAsyncContextPtr &c, void *r, void *privdata)
 {
	redisReply *reply = (redisReply*)r;
	if (reply == nullptr)
	{
		assert(false);
	}

	if(reply->type == REDIS_REPLY_NIL || reply->type == REDIS_REPLY_ERROR)
	{
		assert(false);
	}

}



 int main(int argc, char* argv[])
 {
 	if (argc != 6)
 	{
 		fprintf(stderr, "Usage: client <host_ip> <port> <blockSize> <sessionCount> <threadCount> \n ");
 	}
 	else
 	{
 		const char* ip = argv[1];
 		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
 		int blockSize = atoi(argv[3]);
 		int sessionCount = atoi(argv[4]);
 		int threadCount = atoi(argv[5]);

 		xEventLoop loop;
 		xClient client(&loop, ip,port, blockSize, sessionCount, threadCount);
 		loop.run();
 	}
 	return 0;
 }



