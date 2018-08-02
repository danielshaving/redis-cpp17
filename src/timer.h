#pragma once
#include "all.h"
#include "zmalloc.h"
#include "callback.h"

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
		struct timeval tv;
		gettimeofday(&tv, nullptr);
		int64_t seconds = tv.tv_sec;
		return TimeStamp(seconds * kMicroSecondsPerSecond + tv.tv_usec);
	}

	std::string toString() const;

	static TimeStamp invalid() { return TimeStamp(); }
	static const int32_t kMicroSecondsPerSecond = 1000 * 1000;

private:
	int64_t microSecondsSinceEpoch;
};

inline bool operator<(const TimeStamp &lhs,const TimeStamp &rhs)
{
	return lhs.getMicroSecondsSinceEpoch() < rhs.getMicroSecondsSinceEpoch();
}

inline bool operator==(const TimeStamp &lhs,const TimeStamp &rhs)
{
	return lhs.getMicroSecondsSinceEpoch() == rhs.getMicroSecondsSinceEpoch();
}

inline TimeStamp addTime(const TimeStamp &timestamp,double seconds)
{
	int64_t delta = static_cast<int64_t>(seconds * TimeStamp::kMicroSecondsPerSecond);
	return TimeStamp(timestamp.getMicroSecondsSinceEpoch() + delta);
}

inline double timeDifference(const TimeStamp &high,const TimeStamp &low)
{
	int64_t diff = high.getMicroSecondsSinceEpoch() - low.getMicroSecondsSinceEpoch();
	return static_cast<double>(diff) / TimeStamp::kMicroSecondsPerSecond;
}

class Timer
{
public:
	Timer(TimerCallback &&cb,TimeStamp &&expiration,
			bool repeat,double interval);
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



