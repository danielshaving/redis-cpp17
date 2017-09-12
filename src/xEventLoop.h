#pragma once
#include "all.h"

#include "xChannel.h"
#include "xEpoll.h"
#include "xCurrentThread.h"
#include "xTimerQueue.h"
#include "xCallback.h"

class xEventLoop
{
public:
    typedef std::function<void()> Functor;

	xEventLoop();
	~xEventLoop();

    void quit();
    void run();
    void handleRead();
    void runInLoop(Functor&& cb);
    void queueInLoop(Functor&& cb);
    void wakeup();
    void updateChannel(xChannel* channel);
    void removeChannel(xChannel* channel);
    bool hasChannel(xChannel* channel);
    void cancelAfter(xTimer * timer);
    xTimer  *runAfter(double  when,void * data,bool repeat,xTimerCallback&& cb);
    void assertInLoopThread()
    {
	  if (!isInLoopThread())
	  {
	      abortNotInLoopThread();
	  }
    }
    bool isInLoopThread() const { return threadId == xCurrentThread::tid(); }
    bool geteventHandling() const { return eventHandling; }

    int32_t getThreadId() const { return threadId;}
private:

    void abortNotInLoopThread();
    void doPendingFunctors();

    int wakeupFd;
    const int32_t threadId;
    mutable std::mutex mutex;

    std::unique_ptr<xEpoll>   epoller;
    std::unique_ptr<xTimerQueue> timerQueue;
    std::unique_ptr<xChannel> wakeupChannel;
    typedef std::vector<xChannel*> ChannelList;
    ChannelList activeChannels;
    xChannel* currentActiveChannel;
	
    bool running;
    bool eventHandling;
    bool callingPendingFunctors;
    std::vector<Functor> pendingFunctors;

};

