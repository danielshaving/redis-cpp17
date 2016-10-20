#include "xTimerQueue.h"
#include "xEventLoop.h"
#include "xTimer.h"

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
	for(int i = 0 ; i < pqueue.size(); i ++)
	{
		delete pqueue.p[i];
	}
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
		}
		else
		{
			resetTimerfd(timerfd,pqueue.head()->when);
			break;
		}
	}
	
}

