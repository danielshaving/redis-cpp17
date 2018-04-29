#pragma once
#include "xAll.h"
#include "xZmalloc.h"
#include "xCallback.h"

class xTimeStamp
{
public:
	xTimeStamp()
	:microSecondsSinceEpoch(0)
	{

	}

	explicit xTimeStamp(int64_t microSecondsSinceEpochArg)
	: microSecondsSinceEpoch(microSecondsSinceEpochArg)
	{

	}

	int64_t getMicroSecondsSinceEpoch() const { return microSecondsSinceEpoch; }
	time_t secondsSinceEpoch() const
	{ 
		return static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond); 
	}

	bool valid() const { return microSecondsSinceEpoch > 0; }
	std::string toFormattedString(bool showMicroseconds = true) const;
	static xTimeStamp now()
	{
		struct timeval tv;
		gettimeofday(&tv, nullptr);
		int64_t seconds = tv.tv_sec;
		return xTimeStamp(seconds * kMicroSecondsPerSecond + tv.tv_usec);
	}

	std::string toString() const;

	static xTimeStamp invalid() { return xTimeStamp(); }
	static const int32_t kMicroSecondsPerSecond = 1000 * 1000;

private:
	int64_t microSecondsSinceEpoch;
};

inline bool operator<(const xTimeStamp &lhs,const xTimeStamp &rhs)
{
	return lhs.getMicroSecondsSinceEpoch() < rhs.getMicroSecondsSinceEpoch();
}

inline bool operator==(const xTimeStamp &lhs,const xTimeStamp &rhs)
{
	return lhs.getMicroSecondsSinceEpoch() == rhs.getMicroSecondsSinceEpoch();
}

inline xTimeStamp addTime(const xTimeStamp &timestamp,double seconds)
{
	int64_t delta = static_cast<int64_t>(seconds * xTimeStamp::kMicroSecondsPerSecond);
	return xTimeStamp(timestamp.getMicroSecondsSinceEpoch() + delta);
}

inline double timeDifference(const xTimeStamp &high,const xTimeStamp &low)
{
	int64_t diff = high.getMicroSecondsSinceEpoch() - low.getMicroSecondsSinceEpoch();
	return static_cast<double>(diff) / xTimeStamp::kMicroSecondsPerSecond;
}

class xTimer
{
public:
	xTimer(xTimerCallback &&cb,xTimeStamp &&expiration,bool repeat,double interval);
	~xTimer();

	int64_t getSequence() { return sequence; }
	xTimeStamp &getExpiration() { return expiration; }
	int64_t getWhen() { return expiration.getMicroSecondsSinceEpoch(); };
	bool getRepeat() { return repeat; }
	void setSequence(int64_t seq) { sequence = seq; }
	void restart(const xTimeStamp &now);
	void run();
	double getInterval() { return interval; }

private:	
	int64_t index;
	bool repeat;
	double interval;
	int64_t sequence;
	xTimeStamp expiration;
	xTimerCallback callback;
	static std::atomic<int64_t> numCreated;
};



