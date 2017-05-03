#pragma once

#include "all.h"
#include "xChannel.h"
#include "xCallback.h"
#include "xTimer.h"
class xEventLoop;
class xTimerId;

class xPriorityQueue
{
public:
	xPriorityQueue();
	~xPriorityQueue();
	void dtor();
	bool push(xTimer *e);
	xTimer *pop();
	bool  erase(xTimer *e);
	int   size();
	xTimer *head();
	void clear();
	xTimer *top();
	void reserve();
	void shiftUp(int hole_index, xTimer *e);
	void shiftDown(int hole_index, xTimer *e);

	xTimer **p;
	int n,a;
};



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
