#pragma once
#include "all.h"
#include "log.h"
#include "tcpserver.h"

int num = 1024 * 100;
int count = 0;
std::vector<Timer*> timers;

void canceltimerOut(EventLoop *loop)
{
	loop->quit();
}

void runtimerOut(EventLoop *loop)
{
	if(++count >=num)
	{
		for(auto it = timers.begin(); it != timers.end(); ++it)
		{
			loop->cancelAfter(*it);
		}
		canceltimerOut(data);
	}
}

int main(int argc,char* argv[])
{

	EventLoop loop;
	TcpServer server;
	server.init(&loop,"0.0.0.0",6378,nullptr);

	TimeStamp start(TimeStamp::now());
	for(int i = 0; i < num; i++)
	{
		timers.push_back(loop.runAfter(1.0,true,std::bind(runtimerOut,&loop,&loop)));
	}
	
	loop.run();

	TimeStamp end(TimeStamp::now());
	LOG_INFO<<"bench sec:"<< timeDifference(end,start);
	return 0;
}
