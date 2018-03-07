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
		*(int *)e = -1;
		return e;
	}
	return nullptr;
}


int  xPriorityQueue::size()
{
	return n;
}

bool   xPriorityQueue::erase(xTimer *e)
{
	if (-1 != *(int *)e)
	{
		xTimer *last = p[--n];
		int parent = (*(int *)e - 1) / 2;
		if (*(int *)e > 0 && (p[parent]->getWhen()  >  last->getWhen()) > 0)
			shiftUp(*(int *)e, last);
		else
			shiftDown(*(int *)e, last);
		*(int *)e = -1;
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
	return n?*p:nullptr;
}

void  xPriorityQueue::reserve()
{
	int aa = a ? a * 2:8;
	xTimer **pp = (xTimer**)zrealloc(p,aa * sizeof * pp);

	a = aa;
	p = pp;

}


void xPriorityQueue::shiftUp(int hole_index, xTimer *e)
{
	int parent = (hole_index - 1) / 2;
	while (hole_index && ((p[parent])->getWhen() >  e->getWhen()) > 0)
	{
		*(int *)(p[hole_index] = p[parent]) = hole_index;
		hole_index = parent;
		parent = (hole_index - 1) / 2;
	}
	*(int *)(p[hole_index] = e) = hole_index;
}


void xPriorityQueue::shiftDown(int hole_index, xTimer *e)
{
	int min_child = 2 * (hole_index + 1);
	while (min_child <= n)
	{
		min_child -= min_child == n || (p[min_child]->getWhen()  >  p[min_child - 1]->getWhen()) > 0;
		if (!((e->getWhen() >  p[min_child]->getWhen() ) > 0))
			break;
		*(int *)(p[hole_index] = p[min_child]) = hole_index;
		hole_index = min_child;
		min_child = 2 * (hole_index + 1);
	}
	*(int *)(p[hole_index] = e) = hole_index;
}

#ifdef __linux__
int createTimerfd()
{
  int timerfd = ::timerfd_create(CLOCK_MONOTONIC,
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
	ts.tv_sec = static_cast<time_t>(
	  microseconds / xTimeStamp::kMicroSecondsPerSecond);
	ts.tv_nsec = static_cast<long>(
	  (microseconds % xTimeStamp::kMicroSecondsPerSecond) * 1000);
	return ts;
}



void resetTimerfd(int timerfd, xTimeStamp expiration)
{
#ifdef __linux__
	struct itimerspec newValue;
	struct itimerspec oldValue;
	bzero(&newValue, sizeof newValue);
	bzero(&oldValue, sizeof oldValue);
	newValue.it_value = howMuchTimeFromNow(expiration);
	int net = ::timerfd_settime(timerfd, 0, &newValue, &oldValue);
	if(net < 0 )
	{
		LOG_ERROR<<"timerfd_settime error";
	}
#endif
  
}

void readTimerfd(int timerfd,xTimeStamp now)
{
  uint64_t howmany;
  ssize_t n = ::read(timerfd, &howmany, sizeof howmany);
  if (n != sizeof howmany)
  {
    assert(false);
  }
}


xTimerQueue::xTimerQueue(xEventLoop *loop)
:loop(loop)
#ifdef __linux__
,timerfd(createTimerfd()),
timerfdChannel(loop,timerfd)
#endif
{
#ifdef __linux__
	timerfdChannel.setReadCallback(std::bind(&xTimerQueue::handleRead, this));
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
}

xTimer  * xTimerQueue::addTimer(double when, const std::any &context,bool repeat,xTimerCallback &&cb)
{
	xTimeStamp time(addTime(xTimeStamp::now(), when));
	xTimer * timer = (xTimer*)zmalloc(sizeof(xTimer));
	new(timer)xTimer(std::move(cb), std::move(time), repeat, when, context);
	loop->runInLoop(std::bind(&xTimerQueue::addTimerInLoop,this,timer));
	return timer;
}

void xTimerQueue::cancelTimer(xTimer *timer)
{
	loop->runInLoop(std::bind(&xTimerQueue::cancelInloop,this,timer));
}

void xTimerQueue::cancelInloop(xTimer *timer)
{
	if(pqueue.size() == 0 )
	{
		return ;
	}

	pqueue.erase(timer);
	timer->~xTimer();
	zfree(timer);
	if (pqueue.size() > 1)
	{
		resetTimerfd(timerfd, pqueue.head()->getExpiration());
	}
}

void   xTimerQueue::addTimerInLoop(xTimer *timer)
{
	bool earliestChanged = false;
	loop->assertInLoopThread();
	if(pqueue.size() == 0 )
	{
		 earliestChanged = true;
	}
	else
	{
		if(timer->getWhen() < pqueue.head()->getWhen())
		{
			earliestChanged = true;
		}
	}

	pqueue.push(timer);

	if(earliestChanged)
	{
		resetTimerfd(timerfd,timer->getExpiration());
	}

}

void  xTimerQueue::handleRead()
{
	loop->assertInLoopThread();
	xTimeStamp now(xTimeStamp::now());

#ifdef __linux__
	readTimerfd(timerfd,now);
#endif
	while(pqueue.size() > 0 )
	{
		if(now.getMicroSecondsSinceEpoch() >= pqueue.head()->getWhen())
		{
			xTimer *timer = pqueue.pop();
			assert(timer != nullptr);
			timer->run();
			vectors.push_back(timer);
		}
		else
		{
			resetTimerfd(timerfd,pqueue.head()->getExpiration());
			break;
		}
	}
	

	for(auto &it : vectors)
	{
		if(it->repeat)
		{
			it->restart(now);
			resetTimerfd(timerfd, now);
			pqueue.push(it);
		}
		else
		{
			it->~xTimer();
			zfree(it);
			it = nullptr;
		}
	}

	vectors.clear();


}

