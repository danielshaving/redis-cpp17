#pragma once
#include "xAll.h"
#include "xTcpconnection.h"
#include "xTcpServer.h"
#include "xEventLoop.h"
#include "xLog.h"
#include "xTimer.h"


std::vector<int> pipes;
int numPipes;
int numActive;
int numWrites;
xEventLoop * loop;
std::vector<std::shared_ptr<xChannel>> channels;
int reads,writes,fired;

void readCallback(int fd, int idx)
{
	char ch;

	reads += static_cast<int>(::recv(fd, &ch, sizeof(ch), 0));
	if (writes > 0)
	{
		int widx = idx+1;
		if (widx >= numPipes)
		{
			widx -= numPipes;
		}
		::send(pipes[2 * widx + 1], "m", 1, 0);
		writes--;
		fired++;
	}
	
	if (fired == reads)
	{
		loop->quit();
	}
}


std::pair<int, int> runOnce()
{
	xTimestamp beforeInit(xTimestamp::now());
	for(int i = 0; i < numPipes ; i ++)
	{
		std::shared_ptr<xChannel>   channel  = channels[i];
		channel->setReadCallback(std::bind(readCallback,channel->getfd(),i));		
		channel->enableReading();
	}

	int space = numPipes / numActive;
	space *= 2;
	for (int i = 0; i < numActive; ++i)
	{
		::send(pipes[i * space + 1], "m", 1, 0);
	}

	fired = numActive;
	reads = 0;
	writes = numWrites;
	xTimestamp beforeLoop(xTimestamp::now());
	loop->run();

	xTimestamp end(xTimestamp::now());

	int iterTime = static_cast<int>(end.getMicroSecondsSinceEpoch() - beforeInit.getMicroSecondsSinceEpoch());
	int loopTime = static_cast<int>(end.getMicroSecondsSinceEpoch() - beforeLoop.getMicroSecondsSinceEpoch());
	return std::make_pair(iterTime, loopTime);
}

int main(int argc, char* argv[])
{
	numPipes = 100;
	numActive = 1;
	numWrites = 100;
	int c;

	while ((c = getopt(argc, argv, "n:a:w:")) != -1)
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
	for(int i = 0; i < numPipes; ++i)
	{
		if (::socketpair(AF_UNIX, SOCK_STREAM, 0, &pipes[i*2]) == -1)
		{
			perror("pipe");
			return 1;
		}
	}

	xEventLoop lop;
	loop = &lop;

	
	for(int i = 0 ; i < numPipes; i ++)
	{
		std::shared_ptr<xChannel> channel ( new xChannel(loop, pipes[i*2]));
    		channels.push_back(channel);
	}

	for (int i = 0; i < 25; ++i)
	{
		std::pair<int, int> t = runOnce();
		printf("%8d %8d\n", t.first, t.second);
	}

	for (auto it = channels.begin();it != channels.end(); ++it)
	{
		(*it)->disableAll();
		(*it)->remove();
	}
	   
	 channels.clear();
	return 0;
}




