#pragma once
#include "all.h"
#include "xLog.h"
#include "xTcpServer.h"




int num = 1024 * 100;
int count = 0;
std::vector<xTimer*> timers;


void canceltimerOut(const std::any &context)
{
	xEventLoop *loop = std::any_cast<xEventLoop*>(context);
	loop->quit();
}


void runtimerOut(const std::any &context)
{
	if(++count >=num)
	{
		xEventLoop *loop = std::any_cast<xEventLoop*>(context);
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
	server.init(&loop,"127.0.0.1",6378,nullptr);

	xTimestamp start(xTimestamp::now());
	for(int i = 0; i < num; i++)
	{
		timers.push_back(loop.runAfter(1.0,nullptr,true,std::bind(runtimerOut,&loop)));
	}
	
	loop.run();

	xTimestamp end(xTimestamp::now());
	LOG_INFO<<"bench sec:"<< timeDifference(end, start);
	return 0;
}
