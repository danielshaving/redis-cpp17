#include "xHiredis.h"

static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

int sessionCount = 0;
std::atomic<int64_t>  connetCount;


class  xHiredisAsync : noncopyable
{
public:
	xHiredisAsync(xEventLoop * loop,int threadCount,int sessionCount,const char *ip,int32_t port);
	void redisConnCallBack(const xTcpconnectionPtr& conn,void *data);
        void redisErrorConnCallBack(void *data);
	xHiredis * getHiredis(){ return &hiredis; }
private:
	xHiredis hiredis;
	std::atomic<int> connectCount;
	xEventLoop *loop;
	std::condition_variable condition;
};
