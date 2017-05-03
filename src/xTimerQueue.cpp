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
		for(int i = 0 ; i < a; i ++)
		{
			if(p[i] == nullptr)
			{
				continue;
			}
			delete p[i];
		}
		delete []p;
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
		if (*(int *)e > 0 && (p[parent]->when  >  last->when) > 0)
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
	xTimer **pp = new xTimer * [aa];

	for(int i = 0; i < a; i++)
	{
		pp[i] = new xTimer();
		pp[i]->callback = std::move(p[i]->callback);
		pp[i]->index = p[i]->index;
		pp[i]->key = p[i]->key;
		pp[i]->when = p[i]->when;
		pp[i]->type = p[i]->type;

	}

	if(p)
	{
		for(int i = 0 ; i < a; i ++)
		{
			delete p[i];
			p[i] = nullptr;
		}
		delete []p;
	}

	a = aa;
	p = pp;

}


void xPriorityQueue::shiftUp(int hole_index, xTimer *e)
{
	int parent = (hole_index - 1) / 2;
	while (hole_index && ((p[parent])->when >  e->when) > 0)
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
		min_child -= min_child == n || (p[min_child]->when  >  p[min_child - 1]->when) > 0;
		if (!((e->when >  p[min_child]->when ) > 0))
			break;
		*(int *)(p[hole_index] = p[min_child]) = hole_index;
		hole_index = min_child;
		min_child = 2 * (hole_index + 1);
	}
	*(int *)(p[hole_index] = e) = hole_index;
}



int createTimerfd()
{
  int timerfd = ::timerfd_create(CLOCK_REALTIME,
                                 TFD_NONBLOCK | TFD_CLOEXEC);
  if (timerfd < 0)
  {	
  	assert(false);
  }
  return timerfd;
}

struct timespec howMuchTimeFromNow(int64_t when)
{
  struct timespec ts;
  ts.tv_sec = when;
  ts.tv_nsec = 0;
  return ts;
}

void resetTimerfd(int timerfd, int64_t when)
{
	struct itimerspec newValue;
	struct itimerspec oldValue;
	bzero(&newValue, sizeof newValue);
	bzero(&oldValue, sizeof oldValue);
	newValue.it_value = howMuchTimeFromNow(when);
	int net = ::timerfd_settime(timerfd, 1, &newValue, &oldValue);
	if(net < 0 )
	{
		assert(false);
	}
  
}




void readTimerfd(int timerfd, int64_t  now)
{
  uint64_t howmany;
  ssize_t n = ::read(timerfd, &howmany, sizeof howmany);
  if (n != sizeof howmany)
  {
    assert(false);
  }
}


xTimerQueue::xTimerQueue(xEventLoop *loop)
:loop(loop),
timerfd(createTimerfd()),
timerfdChannel(loop,timerfd)
{
	timerfdChannel.setReadCallback(std::bind(&xTimerQueue::handleRead, this));
	timerfdChannel.enableReading();
}


xTimerQueue::~xTimerQueue()
{
	timerfdChannel.disableAll();
	timerfdChannel.remove();
	::close(timerfd);
}

void  xTimerQueue::addTimer(int64_t when,int64_t key,int8_t type, xTimerCallback&& cb)
{
	xTimer * timer = new xTimer(std::move(cb),when,key,type);
	loop->runInLoop(std::bind(&xTimerQueue::addTimerInLoop,this,timer));
}



void   xTimerQueue::addTimerInLoop(xTimer* timer)
{
	bool earliestChanged = false;
	loop->assertInLoopThread();
	if(pqueue.size() == 0 )
	{
		 earliestChanged = true;
	}
	else
	{
		if(timer->when < pqueue.head()->when)
		{
			earliestChanged = true;	
		}
	}
	
	pqueue.push(timer);

	if(earliestChanged)
	{
		resetTimerfd(timerfd,timer->when);	
	}
	
}

void  xTimerQueue::handleRead()
{
	loop->assertInLoopThread();
	int64_t curTime = time(0);
	readTimerfd(timerfd,curTime);
	while(pqueue.size() > 0 )
	{
		if(curTime >= pqueue.head()->when )
		{	
			xTimer *timer = pqueue.pop();
			if(timer == nullptr)
			{
				assert(false);
			}
			
			timer->run();
			delete timer;
			timer = nullptr;
		}
		else
		{
			resetTimerfd(timerfd,pqueue.head()->when);
			break;
		}
	}
	
}

