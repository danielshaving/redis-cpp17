#pragma once

#include "xAll.h"
#include "xChannel.h"
#include "xCallback.h"
#include "xTimer.h"

class xEventLoop;
class xTimerId;

class xPriorityQueue : boost::noncopyable
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

class xTimerQueue : boost::noncopyable
{
public:
	xTimerQueue(xEventLoop *loop);
	~xTimerQueue();

	void cancelTimer(xTimer *timer);
  	xTimer *addTimer(double when,bool repeat,xTimerCallback &&cb);
  	void handleRead();
  	auto *getPriority() { return &queue; }
  	xTimer *getTimerBegin();

private:
	xEventLoop *loop;
	xPriorityQueue queue;
	int32_t timerfd;
#ifdef __linux__
	xChannel timerfdChannel;
#endif
	std::vector<xTimer*> timers;
	int64_t sequence;

	void cancelInloop(xTimer *timer);
	void addTimerInLoop(xTimer *timer);

	typedef std::pair<xTimeStamp,xTimer*> Entry;
	typedef std::set<Entry> TimerList;
	typedef std::pair<xTimer*,int64_t> ActiveTimer;
	typedef std::set<ActiveTimer> ActiveTimerSet;
	TimerList timerLists;

	std::vector<Entry> getExpired(xTimeStamp now);
	void reset(const std::vector<Entry> &expired,xTimeStamp now);
	bool insert(xTimer *timer);

	ActiveTimerSet activeTimers;
	bool callingExpiredTimers;
	ActiveTimerSet cancelingTimers;
};


