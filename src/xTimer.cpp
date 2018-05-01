#include "xTimer.h"
std::atomic<int64_t> xTimer::numCreated = 0;

xTimer::xTimer(xTimerCallback &&cb,xTimeStamp &&expiration,bool repeat,double interval)
:index(-1),
repeat(repeat),
interval(interval),
expiration(std::move(expiration)),
callback(std::move(cb)),
sequence(++numCreated)
{
	
}

xTimer::~xTimer()
{

}

void xTimer::run()
{
	callback();
}

void xTimer::restart(const xTimeStamp &now)
{
	if (repeat)
	{
		expiration = addTime(now,interval);
	}
	else
	{
		expiration = xTimeStamp::invalid();
	}
}

std::string xTimeStamp::toString() const
{
	char buf[32] = {0};
	int64_t seconds = microSecondsSinceEpoch / kMicroSecondsPerSecond;
	int64_t microseconds = microSecondsSinceEpoch % kMicroSecondsPerSecond;
	snprintf(buf, sizeof(buf)-1, "%" PRId64 ".%06" PRId64 "", seconds, microseconds);
	return buf;
}

std::string xTimeStamp::toFormattedString(bool showMicroseconds) const
{
	char buf[32] = {0};
	time_t seconds = static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond);
	struct tm tm_time;
	gmtime_r(&seconds, &tm_time);

	if (showMicroseconds)
	{
		int microseconds = static_cast<int>(microSecondsSinceEpoch % kMicroSecondsPerSecond);
		snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d.%06d",
				 tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday,
				 tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec,
				 microseconds);
	}
	else
	{
		snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d",
				 tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday,
				 tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
	}
	return buf;
}



