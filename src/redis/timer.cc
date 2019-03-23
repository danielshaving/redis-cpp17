#include "timer.h"
#include "eventloop.h"

std::atomic <int64_t> Timer::numCreated = 0;

Timer::Timer(TimerCallback &&cb,
             TimeStamp &&expiration, bool repeat, double interval)
        : repeat(repeat),
          interval(interval),
          expiration(std::move(expiration)),
          callback(std::move(cb)),
          sequence(++numCreated) {

}

Timer::~Timer() {

}

int64_t Timer::getSequence() {
    return sequence;
}

TimeStamp &Timer::getExpiration() {
    return expiration;
}

int64_t Timer::getWhen() {
    return expiration.getMicroSecondsSinceEpoch();
}

bool Timer::getRepeat() {
    return repeat;
}

void Timer::setSequence(int64_t seq) {
    sequence = seq;
}

double Timer::getInterval() {
    return interval;
}

void Timer::run() {
    assert(callback != nullptr);
    callback();
}

void Timer::restart(const TimeStamp &now) {
    if (repeat) {
        expiration = std::move(addTime(now, interval));
    } else {
        expiration = std::move(TimeStamp::invalid());
    }
}

std::string TimeStamp::toString() const {
    char buf[32] = {0};
    int64_t seconds = microSecondsSinceEpoch / kMicroSecondsPerSecond;
    int64_t microseconds = microSecondsSinceEpoch % kMicroSecondsPerSecond;
    snprintf(buf, sizeof(buf) - 1, "%"
    PRId64
    ".%06"
    PRId64
    "", seconds, microseconds);
    return buf;
}

std::string TimeStamp::toFormattedString(bool showMicroseconds) const {
    char buf[32] = {0};
    time_t seconds = static_cast<time_t>(microSecondsSinceEpoch / kMicroSecondsPerSecond);
    struct tm tm;
    time_t now = time(0);
    tm = *(localtime(&now));
    if (showMicroseconds) {
        int microseconds = static_cast<int>(microSecondsSinceEpoch % kMicroSecondsPerSecond);
        snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d.%06d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec,
                 microseconds);
    } else {
        snprintf(buf, sizeof(buf), "%4d%02d%02d %02d:%02d:%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec);
    }
    return buf;
}

#ifdef __linux__
int64_t createTimerfd()
{
    int64_t timerfd = ::timerfd_create(CLOCK_MONOTONIC,
        TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd < 0)
    {
        assert(false);
    }

    return timerfd;
}
#endif

int64_t howMuchTimeFrom(const TimeStamp &when) {
    int64_t microseconds = when.getMicroSecondsSinceEpoch()
                           - TimeStamp::now().getMicroSecondsSinceEpoch();
    if (microseconds < 1000) {
        microseconds = 1000;
    }
    return static_cast<int>(microseconds / 1000);
}


int64_t TimerQueue::getTimeout() const {
    loop->assertInLoopThread();
    if (timers.empty()) {
        return 1000;
    } else {
        return howMuchTimeFrom(timers.begin()->second->getExpiration());
    }
}

#ifdef __linux__
struct timespec howMuchTimeFromNow(const TimeStamp &when)
{
    int64_t microseconds = when.getMicroSecondsSinceEpoch()
        - TimeStamp::now().getMicroSecondsSinceEpoch();
    if (microseconds < 100)
    {
        microseconds = 100;
    }

    struct timespec ts;
    ts.tv_sec = static_cast<time_t>(microseconds / TimeStamp::kMicroSecondsPerSecond);
    ts.tv_nsec = static_cast<int64_t>((microseconds % TimeStamp::kMicroSecondsPerSecond) * 1000);
    return ts;
}

void resetTimerfd(int64_t timerfd, const TimeStamp &expiration)
{
    struct itimerspec newValue;
    struct itimerspec oldValue;
    bzero(&newValue, sizeof newValue);
    bzero(&oldValue, sizeof oldValue);
    newValue.it_value = howMuchTimeFromNow(expiration);
    int64_t net = ::timerfd_settime(timerfd, 0, &newValue, &oldValue);
    if (net < 0)
    {
        assert(false);
    }
}

void readTimerfd(int64_t timerfd, const TimeStamp &now)
{
    uint64_t howmany;
    ssize_t n = ::read(timerfd, &howmany, sizeof howmany);
    if (n != sizeof howmany)
    {
        assert(false);
    }
}
#endif

TimerQueue::TimerQueue(EventLoop *loop)
        : loop(loop),
#ifdef __linux__
timerfd(createTimerfd()),
timerfdChannel(loop, timerfd),
#endif
          callingExpiredTimers(false) {
#ifdef __linux__
    timerfdChannel.setReadCallback(std::bind(&TimerQueue::handleRead, this));
    timerfdChannel.enableReading();
#endif
}

TimerQueue::~TimerQueue() {
#ifdef __linux__
    timerfdChannel.disableAll();
    timerfdChannel.remove();
    ::close(timerfd);
#endif
}

TimerPtr TimerQueue::addTimer(double when, bool repeat, TimerCallback &&cb) {
    TimeStamp time(addTime(TimeStamp::now(), when));
    TimerPtr timer(new Timer(std::move(cb), std::move(time), repeat, when));
    loop->runInLoop(std::bind(&TimerQueue::addTimerInLoop, this, timer));
    return timer;
}

TimerPtr TimerQueue::addTimer(TimeStamp &&stamp, double when, bool repeat, TimerCallback &&cb) {
    TimerPtr timer(new Timer(std::move(cb), std::move(stamp), repeat, when));
    loop->runInLoop(std::bind(&TimerQueue::addTimerInLoop, this, timer));
    return timer;
}

void TimerQueue::cancelTimer(const TimerPtr &timer) {
    loop->runInLoop(std::bind(&TimerQueue::cancelInloop, this, timer));
}

void TimerQueue::cancelInloop(const TimerPtr &timer) {
    loop->assertInLoopThread();
    assert(timers.size() == activeTimers.size());

    auto it = activeTimers.find(timer->getSequence());
    if (it != activeTimers.end()) {
        auto iter = timers.find(timer->getWhen());
        while (iter != timers.end()) {
            if (timer->getSequence() == iter->second->getSequence()) {
                timers.erase(iter);
                break;
            } else {
                ++iter;
            }
        }
        activeTimers.erase(it);
    } else if (callingExpiredTimers) {
        cancelingTimers.insert(std::make_pair(timer->getSequence(), timer));
    }
    assert(timers.size() == activeTimers.size());
}

void TimerQueue::addTimerInLoop(const TimerPtr &timer) {
    loop->assertInLoopThread();
    bool earliestChanged = insert(timer);
    if (earliestChanged) {
#ifdef __linux__
        resetTimerfd(timerfd, timer->getExpiration());
#endif
    }
}

TimerPtr TimerQueue::getTimerBegin() {
    if (timers.empty()) {
        return nullptr;
    }
    return timers.begin()->second;
}

void TimerQueue::handleRead() {
    loop->assertInLoopThread();
    assert(timers.size() == activeTimers.size());
    TimeStamp now(TimeStamp::now());

#ifdef __linux__
    readTimerfd(timerfd, now);
#endif
    getExpired(now);

    callingExpiredTimers = true;
    cancelingTimers.clear();
    // safe to callback outside critical section

    for (auto &it : expired) {
        it.second->run();
    }

    callingExpiredTimers = false;
    reset(now);
}

bool TimerQueue::insert(const TimerPtr &timer) {
    loop->assertInLoopThread();
    assert(timers.size() == activeTimers.size());

    bool earliestChanged = false;
    int64_t microseconds = timer->getExpiration().getMicroSecondsSinceEpoch();
    auto it = timers.begin();
    if (it == timers.end() || microseconds < it->first) {
        earliestChanged = true;
    }

    timers.insert(std::make_pair(microseconds, timer));
    activeTimers.insert(std::make_pair(timer->getSequence(), timer));

    if (timers.size() != activeTimers.size()) {
        assert(false);
    }
    assert(timers.size() == activeTimers.size());
    return earliestChanged;
}

void TimerQueue::reset(const TimeStamp &now) {
    TimeStamp nextExpire;
    for (auto &it : expired) {
        if (it.second->getRepeat() &&
            cancelingTimers.find(it.second->getSequence()) == cancelingTimers.end()) {
            it.second->restart(now);
            insert(it.second);
        } else {

        }
    }

    expired.clear();
    if (!timers.empty()) {
        nextExpire = timers.begin()->second->getExpiration();
    }

    if (nextExpire.valid()) {
#ifdef __linux__
        resetTimerfd(timerfd, nextExpire);
#endif
    }
}

size_t TimerQueue::getTimerSize() {
    loop->assertInLoopThread();
    assert(timers.size() == activeTimers.size());
    return timers.size();
}

void TimerQueue::getExpired(const TimeStamp &now) {
    assert(timers.size() == activeTimers.size());
    auto end = timers.lower_bound(now.getMicroSecondsSinceEpoch());
    assert(end == timers.end() || now.getMicroSecondsSinceEpoch() <= end->first);
    expired.insert(timers.begin(), end);
    timers.erase(timers.begin(), end);

    for (auto &it : expired) {
        size_t n = activeTimers.erase(it.second->getSequence());
        assert(n == 1);
        (void) n;
    }
    assert(timers.size() == activeTimers.size());
}
