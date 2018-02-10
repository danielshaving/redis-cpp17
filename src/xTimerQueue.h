#pragma once

#include "all.h"
#include "xChannel.h"
#include "xCallback.h"
#include "xTimer.h"
class xEventLoop;
class xTimerId;

class xPriorityQueue : noncopyable
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

class xTimerQueue : noncopyable
{
public:
	xTimerQueue(xEventLoop *loop);
	~xTimerQueue();
	void handleRead();
	void cancelTimer(xTimer *timer);
	void cancelInloop(xTimer *timer);
	void addTimerInLoop(xTimer* timer);
  	xTimer  *addTimer(double  when, const std::any &context, bool repeat, xTimerCallback&& cb);
  	static const int kMicroSecondsPerSecond = 1000 * 1000;
  	xPriorityQueue *getPriority(){ return &pqueue; }
	
private:
	xEventLoop *loop;
	xPriorityQueue pqueue;
	int timerfd;
#ifdef __linux__
	xChannel timerfdChannel;
#endif
	std::vector<xTimer *> vectors;
};
