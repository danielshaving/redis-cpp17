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

	void reserve();
	void shiftUp(int64_t index,xTimer *e);
	void shiftDown(int64_t index,xTimer *e);
	void dtor();
	bool push(xTimer *e);
	bool erase(xTimer *e);
	void clear();
	int64_t size();
	bool empty();

	xTimer *pop();
	xTimer *head();
	xTimer *top();

private:
	xTimer **p;
	int64_t n,a;
};

class xTimerQueue : noncopyable
{
public:
	xTimerQueue(xEventLoop *loop);
	~xTimerQueue();

	void handleRead();
	void cancelTimer(xTimer *timer);
	void cancelInloop(xTimer *timer);
	void addTimerInLoop(xTimer *timer);

  	xTimer *addTimer(double when,const std::any &context,bool repeat,xTimerCallback &&cb);
  	xPriorityQueue *getPriority() { return &queue; }

	static const int kMicroSecondsPerSecond = 1000 * 1000;
	
private:
	xEventLoop *loop;
	xPriorityQueue queue;
	int timerfd;
#ifdef __linux__
	xChannel timerfdChannel;
#endif
	std::vector<xTimer*> vectors;
};
