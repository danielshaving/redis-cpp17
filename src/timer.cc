#include "timer.h"
std::atomic<int64_t> Timer::numCreated = 0;

Timer::Timer(TimerCallback &&cb,
	TimeStamp &&expiration, bool repeat, double interval)
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
	assert(callback != nullptr);
	callback();
}

void Timer::restart(const TimeStamp &now)
{
	if (repeat)
	{
		expiration = std::move(addTime(now, interval));
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
	snprintf(buf, sizeof(buf) - 1, "%" PRId64 ".%06" PRId64 "", seconds, microseconds);
	return buf;
}

std::string TimeStamp::toFormattedString(bool showMicroseconds) const
{
	char buf[32] = { 0 };
	time_t seconds = static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond);
	struct tm tm;
	time_t now = time(0);
#ifdef _WIN32
	tm = *(localtime(&now));
#else
	gmtime_r(&now, &tm);
#endif
	if (showMicroseconds)
	{
		int microseconds = static_cast<int>(microSecondsSinceEpoch % kMicroSecondsPerSecond);
		snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d.%06d",
			tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
			tm.tm_hour, tm.tm_min, tm.tm_sec,
			microseconds);
	}
	else
	{
		snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d",
			tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
			tm.tm_hour, tm.tm_min, tm.tm_sec);
	}
	return buf;
}



