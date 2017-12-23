#include "xHiredis.h"


static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

std::vector<std::shared_ptr<xTcpClient>> vectors;
int sessionCount = 0;

std::atomic<int64_t>  connetCount;


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
		printf(" %d %d\n",threadId, getpid());
	    	assert(false);
	}

	if(++ connetCount ==   sessionCount )
	{
		test_cond(true);
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
		test("Redis async multithreaded safe test");
		for(auto it = redisAsync.redisMaps.begin(); it != redisAsync.redisMaps.end(); it ++)
		{
			count ++;
			redisAsyncCommand(it->second,nullptr,nullptr,"set key%d %d",count,it->second->conn->getLoop()->getThreadId());
			redisAsyncCommand(it->second,getCallback,nullptr,"get key%d",count);
		}

 		loop.run();
 	}
 	return 0;
 }



