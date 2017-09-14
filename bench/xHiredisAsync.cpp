#include "xHiredis.h"

static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

std::map<int32_t ,xRedisAsyncContextPtr> maps;
std::vector<std::shared_ptr<xTcpClient>> vectors;
int sessionCount = 0;
int connetCount = 0;

std::mutex  tmutex;

static void getCallback(const xRedisAsyncContextPtr &c, void *r, void *privdata)
{
	redisReply *reply = (redisReply*) r;
	if (reply == nullptr)
	{
		assert(false);
	}

	if(reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_NIL)
	{
		assert(false);
	}

	long long threadId = 0;
	string2ll(reply->str,reply->len,&threadId);
	if(threadId != c->conn->getLoop()->getThreadId())
	{
		printf(" %d %d\n",threadId,c->conn->getLoop()->getThreadId());
	    assert(false);
	}

	if(++connetCount == sessionCount )
	{
		LOG_INFO<<"All callback success";
	}
}

void connCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		//LOG_INFO<<"Connect success";
		xRedisAsyncContextPtr ac (new xRedisAsyncContext());
		ac->c->reader->buf = &(conn->recvBuff);
		ac->conn = conn;
		ac->c->fd = conn->getSockfd();

		{
			std::unique_lock<std::mutex> lk(tmutex);
			maps.insert(std::make_pair(conn->getSockfd(),ac));
		}

		if(++connetCount == sessionCount )
		{
			connetCount = 0;
			LOG_INFO<<"All conneted";
			int count = 0;

			for(auto it = maps.begin(); it != maps.end(); ++it)
			{
				count ++;
				std::string key = "key";
				std::string str = "set " + key + std::to_string(count) +  " " + std::to_string(it->second->conn->getLoop()->getThreadId());
				redisAsyncCommand(it->second,nullptr,nullptr,str.c_str());
				str.clear();
				str = "get " + key + std::to_string(count);
				redisAsyncCommand(it->second,getCallback,nullptr,str.c_str());
			}

		}

	}
	else
	{
		//LOG_INFO<<"Connect disconnect";
		std::unique_lock<std::mutex> lk(tmutex);
		maps.erase(conn->getSockfd());
	}

}


void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
{
	xRedisAsyncContextPtr redis;
	{
		std::unique_lock<std::mutex> lk(mutex);
		auto it = maps.find(conn->getSockfd());
		if(it == maps.end())
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
			 std::unique_lock<std::mutex> lk(hiMutex);
			 cb = std::move(redis->replies.front());

		 }

		 c->flags |= REDIS_IN_CALLBACK;
		 if(cb.fn)
		 {
			 cb.fn(redis,reply,cb.privdata);
		 }

		 {
			 std::unique_lock<std::mutex> lk(hiMutex);
			 redis->replies.pop_front();
		 }

		 c->reader->fn->freeObjectFuc(reply);
		 c->flags &= ~REDIS_IN_CALLBACK;

	 }
	
}


void connErrorCallBack(void * data)
{
	LOG_WARN<<"connect server failure";
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
 		xThreadPool pool(&loop);
		if(threadCount > 1)
		{
			pool.setThreadNum(threadCount);
		}

		pool.start();

 		for(int i = 0; i < sessionCount; i++)
 		{
 			std::shared_ptr<xTcpClient> client(new xTcpClient(pool.getNextLoop(),nullptr));
 			client->setConnectionCallback(connCallBack);
 			client->setMessageCallback(readCallBack);
 			client->connect(ip,port);
 			vectors.push_back(client);
 		}

 		loop.run();
 	}
 	return 0;
 }



