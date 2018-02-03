#pragma once
#include "xHiredis.h"

int sessionCount = 0;
static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

class xHiredisAsync
{
public:
	xHiredisAsync(xEventLoop * loop,int threadCount,int sessionCount,const char *ip,int32_t port);
	void redisErrorConnCallBack(void *data);
	void redisConnCallBack(const xTcpconnectionPtr& conn,void *data);
	xHiredis *getHiredis() { return &hiredis;}
private:
	xHiredis  hiredis;
	int connectCount;
	xEventLoop *loop;
	std::condition_variable condition;
};
