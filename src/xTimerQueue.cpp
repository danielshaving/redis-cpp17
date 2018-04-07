#include "xTimerQueue.h"
#include "xEventLoop.h"
#include "xTimer.h"

xPriorityQueue::xPriorityQueue()
{
	p = nullptr;
	n = 0;
	a = 0;
}

void xPriorityQueue::dtor()
{
	if(p)
	{
		zfree(p);
	}
}

xPriorityQueue::~xPriorityQueue()
{
	dtor();
}

bool xPriorityQueue::push(xTimer *e)
{
	if(a < n + 1)
	{
		reserve();
	}

	shiftUp(n++, e);
	return true;
}

xTimer *xPriorityQueue::pop()
{
	if (n)
	{
		xTimer *e = *p;
		shiftDown(0, p[--n]);
		*(int64_t*)e = -1;
		return e;
	}
	return nullptr;
}

int64_t xPriorityQueue::size()
{
	return n;
}

bool xPriorityQueue::empty()
{
	return n <= 0;
}

bool xPriorityQueue::erase(xTimer *e)
{
	if (-1 != *(int64_t *)e)
	{
		xTimer *last = p[--n];
		int64_t parent = (*(int64_t *)e - 1) / 2;
		if (*(int64_t *)e > 0 && (p[parent]->getWhen() > last->getWhen()) > 0)
			shiftUp(*(int64_t *)e,last);
		else
			shiftDown(*(int64_t *)e,last);
		*(int64_t *)e = -1;

		return 0;
	}

	return -1;
}

xTimer *xPriorityQueue::head()
{
	return p[0];
}

void xPriorityQueue::clear()
{
	p = 0;
	n = 0;
	a = 0;
}

xTimer *xPriorityQueue::top()
{
	return n ? *p:nullptr;
}

void xPriorityQueue::reserve()
{
	int64_t aa = a ? a * 2:8;
	xTimer **pp = (xTimer**)zrealloc(p,aa * sizeof * pp);

	a = aa;
	p = pp;
}

void xPriorityQueue::shiftUp(int64_t index,xTimer *e)
{
	int64_t parent = (index - 1) / 2;
	while (index && ((p[parent])->getWhen() > e->getWhen()) > 0)
	{
		*(int64_t *)(p[index] = p[parent]) = index;
		index = parent;
		parent = (index - 1) / 2;
	}
	*(int64_t *)(p[index] = e) = index;
}

void xPriorityQueue::shiftDown(int64_t index,xTimer *e)
{
	int64_t min_child = 2 * (index + 1);
	while (min_child <= n)
	{
		min_child -= min_child == n || (p[min_child]->getWhen() > p[min_child - 1]->getWhen()) > 0;
		if (!((e->getWhen() >  p[min_child]->getWhen() ) > 0))
			break;
		*(int64_t *)(p[index] = p[min_child]) = index;
		index = min_child;
		min_child = 2 * (index + 1);
	}
	*(int64_t *)(p[index] = e) = index;
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


struct timespec howMuchTimeFromNow(xTimeStamp when)
{
	int64_t  microseconds = when.getMicroSecondsSinceEpoch()
						 - xTimeStamp::now().getMicroSecondsSinceEpoch();
	if (microseconds < 100)
	{
		microseconds = 100;
	}

	struct timespec ts;
	ts.tv_sec = static_cast<time_t>(microseconds / xTimeStamp::kMicroSecondsPerSecond);
	ts.tv_nsec = static_cast<int64_t>((microseconds % xTimeStamp::kMicroSecondsPerSecond) * 1000);
	return ts;
}

void resetTimerfd(int64_t timerfd,xTimeStamp expiration)
{
#ifdef __linux__
	struct itimerspec newValue;
	struct itimerspec oldValue;
	bzero(&newValue,sizeof newValue);
	bzero(&oldValue,sizeof oldValue);
	newValue.it_value = howMuchTimeFromNow(expiration);
	int64_t net = ::timerfd_settime(timerfd,0,&newValue,&oldValue);
	if(net < 0 )
	{
		LOG_ERROR<<"timerfd_settime error";
	}
#endif
  
}

void readTimerfd(int64_t timerfd,xTimeStamp now)
{
  uint64_t howmany;
  ssize_t n = ::read(timerfd,&howmany,sizeof howmany);
  if (n != sizeof howmany)
  {
    assert(false);
  }
}

xTimerQueue::xTimerQueue(xEventLoop *loop)
:loop(loop),
#ifdef __linux__
timerfd(createTimerfd()),
timerfdChannel(loop,timerfd),
#endif
sequence(0)
{
#ifdef __linux__
	timerfdChannel.setReadCallback(std::bind(&xTimerQueue::handleRead,this));
	timerfdChannel.enableReading();
#endif
}


xTimerQueue::~xTimerQueue()
{
#ifdef __linux__
	timerfdChannel.disableAll();
	timerfdChannel.remove();
	::close(timerfd);
#endif

	for (auto &it : timerLists)
	{
		zfree(it.second);
	}

}

xTimer *xTimerQueue::addTimer(double when,bool repeat,xTimerCallback &&cb)
{
	xTimeStamp time(addTime(xTimeStamp::now(),when));
	xTimer *timer = (xTimer*)zmalloc(sizeof(xTimer));
	new(timer)xTimer(std::move(cb),std::move(time),repeat,when);
	loop->runInLoop(std::bind(&xTimerQueue::addTimerInLoop,this,timer));
	return timer;
}

void xTimerQueue::cancelTimer(xTimer *timer)
{
	loop->runInLoop(std::bind(&xTimerQueue::cancelInloop,this,timer));
}

void xTimerQueue::cancelInloop(xTimer *timer)
{
	// if(queue.empty())
	// {
	// 	return ;
	// }

	// assert(queue.erase(timer) == 0);
	// timer->~xTimer();
	// zfree(timer);

	// if (!queue.empty())
	// {
	// 	resetTimerfd(timerfd,queue.head()->getExpiration());
	// }

	loop->assertInLoopThread();

	assert(timerLists.size() == activeTimers.size());
	ActiveTimer atimer(timer,timer->getSequence());
	auto it = activeTimers.find(atimer);
	if (it != activeTimers.end())
	{
		size_t n = timerLists.erase(Entry(it->first->getExpiration(),it->first));
		assert(n == 1); (void)n;
		zfree(it->first);
		activeTimers.erase(it);
	}
	else if (callingExpiredTimers)
	{
		cancelingTimers.insert(atimer);
	}

	assert(timerLists.size() == activeTimers.size());
}

void xTimerQueue::addTimerInLoop(xTimer *timer)
{
	loop->assertInLoopThread();

	bool earliestChanged = insert(timer);
	// if(queue.empty())
	// {
	// 	earliestChanged = true;
	// }
	// else
	// {
	// 	if(timer->getWhen() < queue.head()->getWhen())
	// 	{
	// 		earliestChanged = true;
	// 	}
	// }

	// queue.push(timer);

	if(earliestChanged)
	{
		resetTimerfd(timerfd,timer->getExpiration());
	}
}

void xTimerQueue::handleRead()
{
	loop->assertInLoopThread();
	xTimeStamp now(xTimeStamp::now());

#ifdef __linux__
	readTimerfd(timerfd,now);
#endif
	// while(queue.size() > 0 )
	// {
	// 	if(now.getMicroSecondsSinceEpoch() >= queue.head()->getWhen())
	// 	{
	// 		xTimer *timer = queue.pop();
	// 		assert(timer != nullptr);
	// 		timer->run();
	// 		timers.push_back(timer);
	// 	}
	// 	else
	// 	{
	// 		resetTimerfd(timerfd,queue.head()->getExpiration());
	// 		break;
	// 	}
	// }

	// for(auto &it : timers)
	// {
	// 	if(it->getRepeat())
	// 	{
	// 		it->restart(now);
	// 		resetTimerfd(timerfd, now);
	// 		queue.push(it);
	// 	}
	// 	else
	// 	{
	// 		it->~xTimer();
	// 		zfree(it);
	// 		it = nullptr;
	// 	}
	// }

	// timers.clear();

	std::vector<Entry> expired = getExpired(now);

	callingExpiredTimers = true;
	cancelingTimers.clear();
	// safe to callback outside critical section
	for (auto &it : expired)
	{
		it.second->run();
	}

	callingExpiredTimers = false;

	reset(expired,now);

}

bool xTimerQueue::insert(xTimer *timer)
{
	loop->assertInLoopThread();
	assert(timerLists.size() == activeTimers.size());
	bool earliestChanged = false;
	xTimeStamp when = timer->getExpiration();

	auto it = timerLists.begin();
	if (it == timerLists.end() || when < it->first)
	{
		earliestChanged = true;
	}

	{
		std::pair<TimerList::iterator,bool> result = timerLists.insert(Entry(when,timer));
		assert(result.second); (void)result;
	}

	{
		std::pair<ActiveTimerSet::iterator,bool> result  = activeTimers.insert(ActiveTimer(timer,timer->getSequence()));
		assert(result.second); (void)result;
	}

	return earliestChanged;
}

void xTimerQueue::reset(const std::vector<Entry> &expired,xTimeStamp now)
{
	xTimeStamp nextExpire;

	for (auto &it : expired)
	{
		ActiveTimer timer(it.second,it.second->getSequence());
		if (it.second->getRepeat()
		&& cancelingTimers.find(timer) == cancelingTimers.end())
		{
			it.second->restart(now);
			insert(it.second);
		}
		else
		{
			zfree(it.second);
		}
	}

	if (!timerLists.empty())
	{
		nextExpire = timerLists.begin()->second->getExpiration();
	}

	if (nextExpire.valid())
	{
		resetTimerfd(timerfd,nextExpire);
	}
}

std::vector<xTimerQueue::Entry> xTimerQueue::getExpired(xTimeStamp now)
{
	assert(timerLists.size() == activeTimers.size());
	std::vector<Entry> expired;
	Entry sentry(now,reinterpret_cast<xTimer*>(UINTPTR_MAX));
	TimerList::iterator end = timerLists.lower_bound(sentry);
	assert(end == timerLists.end() || now < end->first);
	std::copy(timerLists.begin(),end,back_inserter(expired));
	timerLists.erase(timerLists.begin(),end);

	for (auto &it : expired)
	{
		ActiveTimer timer(it.second,it.second->getSequence());
		size_t n = activeTimers.erase(timer);
		assert(n == 1); (void)n;
	}

	assert(timerLists.size() == activeTimers.size());
	return expired;

}
