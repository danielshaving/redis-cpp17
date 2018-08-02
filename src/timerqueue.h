#pragma once
#include "all.h"
#include "channel.h"
#include "callback.h"
#include "timer.h"

class EventLoop;
class TimerId;

class TimerQueue
{
public:
	TimerQueue(EventLoop *loop);
	~TimerQueue();

	void cancelTimer(const TimerPtr &timer);
	TimerPtr addTimer(double when,bool repeat,TimerCallback &&cb);
  	void handleRead();
  	TimerPtr getTimerBegin();

private:
  	TimerQueue(const TimerQueue&);
  	void operator=(const TimerQueue&);

	EventLoop *loop;
	int32_t timerfd;
#ifdef __linux__
	Channel timerfdChannel;
#endif
	int64_t sequence;

	void cancelInloop(const TimerPtr &timer);
	void addTimerInLoop(const TimerPtr &timer);

	typedef std::pair<TimeStamp,TimerPtr> Entry;
	typedef std::set<Entry> TimerList;
	typedef std::pair<TimerPtr,int64_t> ActiveTimer;
	typedef std::set<ActiveTimer> ActiveTimerSet;

	std::vector<Entry> expired;
	TimerList timerLists;

	void getExpired(const TimeStamp &now);
	void reset(const TimeStamp &now);
	bool insert(const TimerPtr &timer);

	ActiveTimerSet activeTimers;
	bool callingExpiredTimers;
	ActiveTimerSet cancelingTimers;
};


