#pragma once
#include "all.h"
#include "xZmalloc.h"
#include "xCallback.h"

class xTimestamp
{
public:
	xTimestamp()
	: microSecondsSinceEpoch(0)
	{
	}

	explicit xTimestamp(int64_t microSecondsSinceEpochArg)
	: microSecondsSinceEpoch(microSecondsSinceEpochArg)
	{
	}

	int64_t getMicroSecondsSinceEpoch() const { return microSecondsSinceEpoch; }
	time_t secondsSinceEpoch() const
	{ return static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond); }

	std::string toFormattedString(bool showMicroseconds = true) const;
	static xTimestamp now()
	{
		struct timeval tv;
		gettimeofday(&tv, nullptr);
		int64_t seconds = tv.tv_sec;
		return xTimestamp(seconds * kMicroSecondsPerSecond + tv.tv_usec);
	}

	  static xTimestamp invalid()
	  {
	    	return xTimestamp();
	  }

	static const int kMicroSecondsPerSecond = 1000 * 1000;
private:
	int64_t microSecondsSinceEpoch;
};

inline xTimestamp addTime(xTimestamp timestamp, double seconds)
{
	int64_t delta = static_cast<int64_t>(seconds * xTimestamp::kMicroSecondsPerSecond);
	return xTimestamp(timestamp.getMicroSecondsSinceEpoch() + delta);
}



inline double timeDifference(xTimestamp high, xTimestamp low)
{
	int64_t diff = high.getMicroSecondsSinceEpoch() - low.getMicroSecondsSinceEpoch();
	return static_cast<double>(diff) / xTimestamp::kMicroSecondsPerSecond;
}


class xTimer
{
public:
	xTimer();
	xTimer(xTimerCallback && cb, xTimestamp && expiration,bool	repeat,double interval,const std::any &context);
	~xTimer();

	xTimestamp getExpiration() const  { return expiration;}
	int64_t getWhen() { return expiration.getMicroSecondsSinceEpoch(); };
	void restart(xTimestamp now);
	void run();

public:	
	int32_t index;
	bool	repeat;
	double interval;
	xTimestamp expiration;
	xTimerCallback callback;
	std::any context;
};



