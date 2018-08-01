#include "timer.h"
std::atomic<int64_t> Timer::numCreated = 0;

Timer::Timer(TimerCallback &&cb,
		TimeStamp &&expiration,bool repeat,double interval)
:repeat(repeat),
interval(interval),
expiration(std::move(expiration)),
callback(std::move(cb)),
sequence(++numCreated)
{
	
}

Timer::~Timer()
{

}

int64_t Timer::getSequence()
{
	return sequence;
}

TimeStamp &Timer::getExpiration()
{
	return expiration;
}

int64_t Timer::getWhen()
{
	return expiration.getMicroSecondsSinceEpoch();
}

bool Timer::getRepeat()
{
	return repeat;
}

void Timer::setSequence(int64_t seq)
{
	sequence = seq;
}

double Timer::getInterval()
{
	return interval;
}

void Timer::run()
{
	assert(callback);
	callback();
}

void Timer::restart(const TimeStamp &now)
{
	if (repeat)
	{
		expiration = std::move(addTime(now,interval));
	}
	else
	{
		expiration = std::move(TimeStamp::invalid());
	}
}


std::string TimeStamp::toString() const
{
	char buf[32] = { 0 };
	int64_t seconds = microSecondsSinceEpoch / kMicroSecondsPerSecond;
	int64_t microseconds = microSecondsSinceEpoch % kMicroSecondsPerSecond;
	snprintf(buf,sizeof(buf) - 1,"%" PRId64 ".%06" PRId64 "",seconds,microseconds);
	return buf;
}

std::string TimeStamp::toFormattedString(bool showMicroseconds) const
{
	char buf[32] = { 0 };
	time_t seconds = static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond);
	struct tm tm_time;
	gmtime_r(&seconds,&tm_time);

	if (showMicroseconds)
	{
		int microseconds = static_cast<int>(microSecondsSinceEpoch % kMicroSecondsPerSecond);
		snprintf(buf, sizeof(buf),"%4d%02d%02d %02d:%02d:%02d.%06d",
				 tm_time.tm_year + 1900, tm_time.tm_mon + 1,tm_time.tm_mday,
				 tm_time.tm_hour,tm_time.tm_min, tm_time.tm_sec,
				 microseconds);
	}
	else
	{
		snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d",
				 tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday,
				 tm_time.tm_hour,tm_time.tm_min,tm_time.tm_sec);
	}
	return buf;
}



