//
// Created by zhanghao on 2018/6/17.
//

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

	void cancelTimer(Timer *timer);
  	Timer *addTimer(double when,bool repeat,TimerCallback &&cb);
  	void handleRead();
  	Timer *getTimerBegin();

private:
  	TimerQueue(const TimerQueue&);
  	void operator=(const TimerQueue&);

	EventLoop *loop;
	int32_t timerfd;
#ifdef __linux__
	Channel timerfdChannel;
#endif
	int64_t sequence;

	void cancelInloop(Timer *timer);
	void addTimerInLoop(Timer *timer);

	typedef std::pair<TimeStamp,Timer*> Entry;
	typedef std::set<Entry> TimerList;
	typedef std::pair<Timer*,int64_t> ActiveTimer;
	typedef std::set<ActiveTimer> ActiveTimerSet;
	TimerList timerLists;

	std::vector<Entry> getExpired(TimeStamp now);
	void reset(const std::vector<Entry> &expired,TimeStamp now);
	bool insert(Timer *timer);

	ActiveTimerSet activeTimers;
	bool callingExpiredTimers;
	ActiveTimerSet cancelingTimers;
};


