#pragma once
#include "xAll.h"
#include "xLog.h"
#include "xTcpServer.h"

int num = 1024 * 100;
int count = 0;
std::vector<xTimer*> timers;

void canceltimerOut(xEventLoop *loop)
{
	loop->quit();
}


void runtimerOut(xEventLoop *loop)
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

	xEventLoop loop;
	xTcpServer server;
	server.init(&loop,"0.0.0.0",6378,nullptr);

	xTimestamp start(xTimestamp::now());
	for(int i = 0; i < num; i++)
	{
		timers.push_back(loop.runAfter(1.0,true,std::bind(runtimerOut,&loop,&loop)));
	}
	
	loop.run();

	xTimestamp end(xTimestamp::now());
	LOG_INFO<<"bench sec:"<< timeDifference(end,start);
	return 0;
}
