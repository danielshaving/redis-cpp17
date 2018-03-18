#pragma once
#include "all.h"

#include "xChannel.h"	
#include "xSocket.h"
#ifdef __APPLE__
//#include "xKqueue.h"
#include "xPoll.h"
#endif

#ifdef __linux__
#include "xEpoll.h"
#endif

#include "xTimerQueue.h"
#include "xCallback.h"

class xEventLoop : noncopyable
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
    xTimer  *runAfter(double  when,const std::any &context,bool repeat,xTimerCallback &&cb);
    void assertInLoopThread()
    {
		if (!isInLoopThread())
		{
			abortNotInLoopThread();
		}
    }

    void handlerTimerQueue(){ timerQueue->handleRead(); }
    xPriorityQueue * getTimerQueue(){ return timerQueue->getPriority(); }
    bool isInLoopThread() const { return threadId == std::this_thread::get_id(); }
    bool geteventHandling() const { return eventHandling; }

    std::thread::id getThreadId() const { return threadId; }
private:

    void abortNotInLoopThread();
    void doPendingFunctors();

    std::thread::id threadId;
    mutable std::mutex mutex;
#ifdef __APPLE__
    std::unique_ptr<xPoll>   epoller;
    int16_t op;
    int32_t wakeupFd[2];
#endif

#ifdef __linux__
	std::unique_ptr<xEpoll>   epoller;
    int32_t wakeupFd;
#endif

    std::unique_ptr<xTimerQueue> timerQueue;
    std::unique_ptr<xChannel> wakeupChannel;
    typedef std::vector<xChannel*> ChannelList;
    ChannelList activeChannels;
    xChannel* currentActiveChannel;
	
    bool running;
    bool eventHandling;
    bool callingPendingFunctors;
    std::vector<Functor> pendingFunctors;
    std::vector<Functor> pendingPipeFunctors;
    xSocket socket;
};

