#pragma once
#include "xAll.h"

#include "xChannel.h"	
#include "xSocket.h"
#ifdef __APPLE__
#include "xPoll.h"
#endif

#ifdef __linux__
#include "xEpoll.h"
#endif

#include "xTimerQueue.h"
#include "xCallback.h"

class xEventLoop : boost::noncopyable
{
public:
	typedef std::function<void()> Functor;

	xEventLoop();
	~xEventLoop();

	void quit();

	void run();
	void handleRead();
	void runInLoop(Functor &&cb);
	void queueInLoop(Functor &&cb);
	void wakeup();
	void updateChannel(xChannel *channel);
	void removeChannel(xChannel *channel);
	bool hasChannel(xChannel *channel);
	void cancelAfter(xTimer *timer);
	void assertInLoopThread()
    {
	  if (!isInLoopThread())
	  {
	      abortNotInLoopThread();
	  }
    }

    xTimer *runAfter(double when,bool repeat,xTimerCallback &&cb);
    void handlerTimerQueue() { timerQueue->handleRead(); }
    TimerQueuePtr getTimerQueue() { return timerQueue; }
    bool isInLoopThread() const { return threadId == std::this_thread::get_id(); }
    bool geteventHandling() const { return eventHandling; }
    std::thread::id getThreadId() const { return threadId; }

private:
    void abortNotInLoopThread();
    void doPendingFunctors();

    std::thread::id threadId;
    mutable std::mutex mutex;
#ifdef __APPLE__
    PollPtr epoller;
    int op;
    int wakeupFd[2];
#endif

#ifdef __linux__
    EpollPtr epoller;
    int wakeupFd;
#endif

    TimerQueuePtr timerQueue;
    ChannelPtr wakeupChannel;

    typedef std::vector<xChannel*> ChannelList;
    ChannelList activeChannels;
    xChannel *currentActiveChannel;
	
    bool running;
    bool eventHandling;
    bool callingPendingFunctors;
    std::vector<Functor> functors;
    std::vector<Functor> pendingFunctors;
    xSocket socket;
};

