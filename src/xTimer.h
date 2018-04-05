#pragma once
#include "all.h"
#include "xZmalloc.h"
#include "xCallback.h"

class xTimeStamp
{
public:
	xTimeStamp()
	: microSecondsSinceEpoch(0)
	{
	}

	explicit xTimeStamp(int64_t microSecondsSinceEpochArg)
	: microSecondsSinceEpoch(microSecondsSinceEpochArg)
	{

	}

	int64_t getMicroSecondsSinceEpoch() const { return microSecondsSinceEpoch; }
	time_t secondsSinceEpoch() const
	{ return static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond); }

	std::string toFormattedString(bool showMicroseconds = true) const;
	static xTimeStamp now()
	{
		struct timeval tv;
		gettimeofday(&tv, nullptr);
		int64_t seconds = tv.tv_sec;
		return xTimeStamp(seconds * kMicroSecondsPerSecond + tv.tv_usec);
	}

	std::string toString() const;
	static xTimeStamp invalid()
	{
		return xTimeStamp();
	}

	static const int kMicroSecondsPerSecond = 1000 * 1000;
private:
	int64_t microSecondsSinceEpoch;
};

inline xTimeStamp addTime(xTimeStamp timestamp,double seconds)
{
	int64_t delta = static_cast<int64_t>(seconds * xTimeStamp::kMicroSecondsPerSecond);
	return xTimeStamp(timestamp.getMicroSecondsSinceEpoch() + delta);
}

inline double timeDifference(xTimeStamp high,xTimeStamp low)
{
	int64_t diff = high.getMicroSecondsSinceEpoch() - low.getMicroSecondsSinceEpoch();
	return static_cast<double>(diff) / xTimeStamp::kMicroSecondsPerSecond;
}

class xTimer : noncopyable
{
public:
	xTimer(xTimerCallback &&cb,xTimeStamp &&expiration,bool repeat,double interval,const std::any &context);
	~xTimer();

	xTimeStamp getExpiration() const { return expiration; }
	int64_t getWhen() { return expiration.getMicroSecondsSinceEpoch(); };
	void restart(xTimeStamp now);
	void run();

public:	
	int64_t index;
	bool repeat;
	double interval;
	xTimeStamp expiration;
	xTimerCallback callback;
	std::any context;
};



