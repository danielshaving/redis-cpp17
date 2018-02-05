#include "xTimer.h"

xTimer::xTimer()
{

}

xTimer::xTimer(xTimerCallback && cb, xTimestamp && expiration,bool repeat,double interval,const  std::any &context)
:index(-1),
repeat(repeat),
interval(interval),
expiration(std::move(expiration)),
callback(std::move(cb)),
context(context)
{
	
}


xTimer::~xTimer()
{

}


void xTimer::run()
{
	if(callback)
	{
		callback(context);
	}
}


void xTimer::restart(xTimestamp now)
{
	if (repeat)
	{
		expiration = addTime(now, interval);
	}
	else
	{
		expiration = xTimestamp::invalid();
	}
}



std::string xTimestamp::toFormattedString(bool showMicroseconds) const
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



