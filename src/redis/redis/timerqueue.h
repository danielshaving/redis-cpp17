#pragma once
#include "all.h"
#include "channel.h"
#include "callback.h"

class EventLoop;
class TimeStamp
{
public:
	TimeStamp()
		:microSecondsSinceEpoch(0)
	{

	}

	explicit TimeStamp(int64_t microSecondsSinceEpochArg)
		: microSecondsSinceEpoch(microSecondsSinceEpochArg)
	{

	}

	int64_t getMicroSecondsSinceEpoch() const
	{
		return microSecondsSinceEpoch;
	}

	time_t secondsSinceEpoch() const
	{
		return static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond);
	}

	bool valid() const { return microSecondsSinceEpoch > 0; }
	std::string toFormattedString(bool showMicroseconds = true) const;
	static TimeStamp now()
	{
		auto timeNow = std::chrono::system_clock::now();
		auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(timeNow.time_since_epoch());
		return TimeStamp(microseconds.count());
	}

	std::string toString() const;

	static TimeStamp invalid() { return TimeStamp(); }
	static const int32_t kMicroSecondsPerSecond = 1000 * 1000;

private:
	int64_t microSecondsSinceEpoch;
};

inline bool operator<(const TimeStamp &lhs, const TimeStamp &rhs)
{
	return lhs.getMicroSecondsSinceEpoch() < rhs.getMicroSecondsSinceEpoch();
}

inline bool operator==(const TimeStamp &lhs, const TimeStamp &rhs)
{
	return lhs.getMicroSecondsSinceEpoch() == rhs.getMicroSecondsSinceEpoch();
}

inline TimeStamp addTime(const TimeStamp &timestamp, double seconds)
{
	int64_t delta = static_cast<int64_t>(seconds * TimeStamp::kMicroSecondsPerSecond);
	return TimeStamp(timestamp.getMicroSecondsSinceEpoch() + delta);
}

inline double timeDifference(const TimeStamp &high, const TimeStamp &low)
{
	int64_t diff = high.getMicroSecondsSinceEpoch() - low.getMicroSecondsSinceEpoch();
	return static_cast<double>(diff) / TimeStamp::kMicroSecondsPerSecond;
}

class Timer
{
public:
	Timer(TimerCallback &&cb, TimeStamp &&expiration,
		bool repeat, double interval);
	~Timer();

	void run();

	int64_t getSequence();
	int64_t getWhen();
	TimeStamp &getExpiration();

	bool getRepeat();
	void setSequence(int64_t seq);
	void restart(const TimeStamp &now);
	double getInterval();

private:
	Timer(const Timer&);
	void operator=(const Timer&);

	bool repeat;
	double interval;
	int64_t sequence;
	TimeStamp expiration;
	TimerCallback callback;
	static std::atomic<int64_t> numCreated;
};

class TimerQueue
{
public:
	TimerQueue(EventLoop *loop);
	~TimerQueue();

	void cancelTimer(const TimerPtr &timer);
	void handleRead();

	TimerPtr addTimer(double when, bool repeat, TimerCallback &&cb);
	TimerPtr getTimerBegin();
	int64_t getTimeout() const;
	size_t getTimerSize();

private:
	TimerQueue(const TimerQueue&);
	void operator=(const TimerQueue&);

	EventLoop *loop;
	int32_t timerfd;
#ifdef __linux__
	Channel timerfdChannel;
#endif

	void cancelInloop(const TimerPtr &timer);
	void addTimerInLoop(const TimerPtr &timer);

	void getExpired(const TimeStamp &now);
	void reset(const TimeStamp &now);
	bool insert(const TimerPtr &timer);

	typedef std::multimap<int64_t, TimerPtr> TimerList;
	typedef std::map<int64_t, TimerPtr> ActiveTimer;

	ActiveTimer activeTimers;
	ActiveTimer cancelingTimers;
	TimerList expired;
	TimerList timers;
	bool callingExpiredTimers;
};


