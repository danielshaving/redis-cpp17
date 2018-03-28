#pragma once
#include "xHiredis.h"

std::atomic<int32_t> sessionCount = 0;
std::atomic<int32_t> gconnetCount = 0;
std::atomic<int32_t> sconnetCount = 0;
int32_t  benchCount  = 10000;
static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

class xHiredisAsync
{
public:
	xHiredisAsync(xEventLoop *loop,int threadCount,const char *ip,int16_t port);
	void redisErrorConnCallBack(const std::any &context);
	void redisConnCallBack(const TcpConnectionPtr& conn);
	xHiredis *getHiredis() { return &hiredis; }
	void serverCron(const std::any & context);

private:
	xHiredis  hiredis;
	std::atomic<int> connectCount;
	xEventLoop *loop;
	std::condition_variable condition;
	bool cron;
};
