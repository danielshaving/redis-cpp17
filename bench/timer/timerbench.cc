#pragma once
#include "all.h"
#include "log.h"
#include "tcpserver.h"

int count1 = 100000;
int count2 = 0;
int count3 = 0;
EventLoop loop;

std::vector<TimerPtr> timers;
void serverCron1()
{
	assert(count2 <= count1 - count3);
	if (++count2 == count1 - count3)
	{
		count2 = 0;
		printf("serverCron2\n");
	}
}

void serverCron2()
{
	printf("timer size:%d\n",loop.getTimerQueue()->getTimerSize());
}

void serverCron3()
{
	for (auto it = timers.begin(); it != timers.end();)
	{
		loop.cancelAfter(*it);
		timers.erase(it++);
		count3++;
		break;
	}
}

int main(int argc,char* argv[])
{
	TcpServer server(&loop,"127.0.0.1",6379,nullptr);
	server.start();

	for(int i = 0; i < count1; i++)
	{
		TimerPtr timer = loop.runAfter(10.0,true,std::bind(serverCron1));
		timers.push_back(timer);
		//loop.cancelAfter(timer);
	}

	loop.runAfter(1.0,true,std::bind(serverCron2));
	loop.runAfter(1.0,true,std::bind(serverCron3));
	loop.run();
	return 0;
}
