#pragma once

#include "xAll.h"
#include "xChannel.h"
#include "xCallback.h"
#include "xTimer.h"

class xEventLoop;
class xTimerId;

class xTimerQueue
{
public:
	xTimerQueue(xEventLoop *loop);
	~xTimerQueue();

	void cancelTimer(xTimer *timer);
  	xTimer *addTimer(double when,bool repeat,xTimerCallback &&cb);
  	void handleRead();
  	xTimer *getTimerBegin();

private:
  	xTimerQueue(const xTimerQueue&);
  	void operator=(const xTimerQueue&);

	xEventLoop *loop;
	int32_t timerfd;
#ifdef __linux__
	xChannel timerfdChannel;
#endif
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


