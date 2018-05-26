#pragma once
#include "all.h"
#include "tcpconnection.h"
#include "tcpserver.h"
#include "eventloop.h"
#include "log.h"
#include "timer.h"

std::vector<int32_t> pipes;
int32_t numPipes;
int32_t numActive;
int32_t numWrites;
EventLoop *loop;
std::vector<std::shared_ptr<Channel>> channels;
int32_t reads,writes,fired;

void readCallback(int32_t fd,int32_t idx)
{
	char ch;
	reads += static_cast<int32_t>(::recv(fd,&ch,sizeof(ch),0));
	if (writes > 0)
	{
		int32_t widx = idx+1;
		if (widx >= numPipes)
		{
			widx -= numPipes;
		}
		::send(pipes[2 * widx + 1],"m",1,0);
		writes--;
		fired++;
	}
	
	if (fired == reads)
	{
		loop->quit();
	}
}


std::pair<int32_t, int32_t> runOnce()
{
	TimeStamp beforeInit(TimeStamp::now());
	for(int32_t i = 0; i < numPipes ; i++)
	{
		std::shared_ptr<Channel> channel = channels[i];
		channel->setReadCallback(std::bind(readCallback,channel->getfd(),i));		
		channel->enableReading();
	}

	int32_t space = numPipes / numActive;
	space *= 2;
	for (int32_t i = 0; i < numActive; ++i)
	{
		::send(pipes[i * space + 1], "m", 1, 0);
	}

	fired = numActive;
	reads = 0;
	writes = numWrites;
	TimeStamp beforeLoop(TimeStamp::now());
	loop->run();

	TimeStamp end(TimeStamp::now());

	int32_t iterTime = static_cast<int32_t>(end.getMicroSecondsSinceEpoch() - beforeInit.getMicroSecondsSinceEpoch());
	int32_t loopTime = static_cast<int32_t>(end.getMicroSecondsSinceEpoch() - beforeLoop.getMicroSecondsSinceEpoch());
	return std::make_pair(iterTime,loopTime);
}

int main(int argc,char* argv[])
{
	numPipes = 100;
	numActive = 1;
	numWrites = 100;
	int32_t c;

	while ((c = getopt(argc,argv,"n:a:w:")) != -1)
	{
		switch (c)
		{
		  case 'n':
		    numPipes = atoi(optarg);
		    break;
		  case 'a':
		    numActive = atoi(optarg);
		    break;
		  case 'w':
		    numWrites = atoi(optarg);
		    break;
		  default:
		    fprintf(stderr, "Illegal argument \"%c\"\n", c);
		    return 1;
		}
	}

	struct rlimit rl;
	rl.rlim_cur = rl.rlim_max = numPipes * 2 + 50;
	if(::setrlimit(RLIMIT_NOFILE,&rl) == -1)
	{
		perror("setrlimit");
	}
	
	pipes.resize(2 * numPipes);
	for(int32_t i = 0; i < numPipes; ++i)
	{
		if (::socketpair(AF_UNIX,SOCK_STREAM,0,&pipes[i*2]) == -1)
		{
			perror("pipe");
			return 1;
		}
	}

	EventLoop lop;
	loop = &lop;

	for(int32_t i = 0 ; i < numPipes; i ++)
	{
		std::shared_ptr<Channel> channel(new Channel(loop,pipes[i*2]));
    	channels.push_back(channel);
	}

	for (int32_t i = 0; i < 25; ++i)
	{
		std::pair<int32_t,int32_t> t = runOnce();
		printf("%8d %8d\n",t.first,t.second);
	}

	for(auto &it : channels)
	{
		it.disableAll();
		it.remove();
	}
	   
	channels.clear();
	return 0;
}




