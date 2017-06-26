#include "all.h"

#include "xEventLoop.h"


const int maxCount = 65535;

int createEventfd()
{
  int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (evtfd < 0)
  {
    assert(false);
  }
  return evtfd;
}


xEventLoop::xEventLoop()
:wakeupFd(createEventfd()),
 threadId(xCurrentThread::tid()),
 epoller(new xEpoll(this)),
 timerQueue(new xTimerQueue(this)),
 wakeupChannel(new xChannel(this,wakeupFd)),
 currentActiveChannel(nullptr),
 running(false),
 eventHandling(false),
 callingPendingFunctors(false)
{
	signal(SIGPIPE, SIG_IGN);
	signal(SIGHUP, SIG_IGN);
	wakeupChannel->setReadCallback(std::bind(&xEventLoop::handleRead, this));
	wakeupChannel->enableReading();
}


void xEventLoop::abortNotInLoopThread()
{
	assert(false);
}

xEventLoop::~xEventLoop()
{
	wakeupChannel->disableAll();
	wakeupChannel->remove();
	::close(wakeupFd);
}



void xEventLoop::updateChannel(xChannel* channel)
{
	assert(channel->ownerLoop() == this);
	assertInLoopThread();
	epoller->updateChannel(channel);
}


void xEventLoop::removeChannel(xChannel* channel)
{
	assert(channel->ownerLoop() == this);
	assertInLoopThread();
	if (eventHandling)
	{
		assert(currentActiveChannel == channel ||
		std::find(activeChannels.begin(), activeChannels.end(), channel) == activeChannels.end());
	}
	epoller->removeChannel(channel);
}




void xEventLoop::cancelAfter(xTimer * timer)
{
	timerQueue->cancelTimer(timer);
}

xTimer * xEventLoop::runAfter(double  when, bool repeat,xTimerCallback&& cb)
{
	return timerQueue->addTimer(when,repeat,std::move(cb));
}



bool xEventLoop::hasChannel(xChannel* channel)
{
	assert(channel->ownerLoop() == this);
	assertInLoopThread();
	return epoller->hasChannel(channel);
}

void  xEventLoop::handleRead()
{
	uint64_t one = 1;
	ssize_t n = ::read(wakeupFd, &one, sizeof one);
	if (n != sizeof one)
	{
		//TRACE("xEventLoop::handleRead() reads error");
	}
}


void xEventLoop::quit()
{
	running = false;
	if (!isInLoopThread())
	{
		wakeup();
	}
}



void xEventLoop::wakeup()
{
  uint64_t one = 1;
  ssize_t n = ::write(wakeupFd, &one, sizeof one);
  if (n != sizeof one)
  {
    //TRACE("EventLoop::wakeup() wrties error");
  }
}


void xEventLoop::runInLoop(Functor&& cb)
{
	if (isInLoopThread())
	{
		cb();
	}
	else
	{
		queueInLoop(std::move(cb));
	}

}
void xEventLoop::queueInLoop(Functor&& cb)
{
	{
	 std::unique_lock<std::mutex> lk(mutex);
	 pendingFunctors.push_back(std::move(cb));

	}

	if (!isInLoopThread() || callingPendingFunctors)
	{
		wakeup();
	}
}




void xEventLoop::doPendingFunctors()
{
	std::vector<Functor> functors;
	callingPendingFunctors = true;

	{
		std::unique_lock<std::mutex> lk(mutex);
		functors.swap(pendingFunctors);
		//functors = std::move(pendingFunctors);
	}

	for (size_t i = 0; i < functors.size(); ++i)
	{
		functors[i]();
	}
	callingPendingFunctors = false;
}


void xEventLoop::run()
{
	running = true;
	while (running)
	{
		activeChannels.clear();
		epoller->epollWait(&activeChannels);

		// TODO sort channel by priority
		eventHandling = true;
		for (ChannelList::iterator it = activeChannels.begin();
			it != activeChannels.end(); ++it)
		{
		  currentActiveChannel = *it;
		  currentActiveChannel->handleEvent();
		}
		currentActiveChannel = nullptr;
		eventHandling = false;
		doPendingFunctors();
	}
}


