#pragma once

#include "all.h"
#include "xChannel.h"
#include "xCallback.h"
#include "xPriorityQueue.h"

class xEventLoop;
class xTimer;
class xTimerId;



class xTimerQueue
{
public:
	xTimerQueue(xEventLoop *loop);
	~xTimerQueue();
	void handleRead();
	void addTimerInLoop(xTimer* timer);
  	void addTimer(int64_t when,int64_t key,int8_t type,xTimerCallback&& cb);

private:
	xEventLoop *loop;
	xPriorityQueue pqueue;
	const int timerfd;
	xChannel timerfdChannel;
};
