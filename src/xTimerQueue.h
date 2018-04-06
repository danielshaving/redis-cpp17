#pragma once

#include "xAll.h"
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

  	xTimer *addTimer(double when,bool repeat,xTimerCallback &&cb);
  	xPriorityQueue *getPriority() { return &queue; }
	
private:
	xEventLoop *loop;
	xPriorityQueue queue;
	int timerfd;
#ifdef __linux__
	xChannel timerfdChannel;
#endif
	std::vector<xTimer*> vectors;

	typedef std::pair<xTimeStamp,xTimer*> Entry;
	typedef std::set<Entry> TimerList;
	typedef std::pair<xTimer*,int64_t> ActiveTimer;
	typedef std::set<ActiveTimer> ActiveTimerSet;
	TimerList timers;

	static const int kMicroSecondsPerSecond = 1000 * 1000;
};


