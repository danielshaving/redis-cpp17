#include "eventloop.h"
#include "log.h"

#ifdef __linux__
int createEventfd()
{
    int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evtfd < 0)
    {
        assert(false);
    }
    return evtfd;
}
#endif

EventLoop::EventLoop()
        : threadId(std::this_thread::get_id()),
#ifdef __linux__
wakeupFd(createEventfd()),
epoller(new Epoll(this)),
timerQueue(new TimerQueue(this)),
wakeupChannel(new Channel(this, wakeupFd)),
#endif
#ifdef __APPLE__
epoller(new Poll(this)),
op(socketpair(AF_UNIX, SOCK_STREAM, 0, wakeupFd)),
wakeupChannel(new Channel(this, wakeupFd[1])),
timerQueue(new TimerQueue(this)),
#endif
#ifdef _WIN64
epoller(new Select(this)),
op(Socket::pipe(wakeupFd)),
wakeupChannel(new Channel(this, wakeupFd[0])),
timerQueue(new TimerQueue(this)),
#endif
          currentActiveChannel(nullptr),
          running(false),
          eventHandling(false),
          callingPendingFunctors(false) {
    wakeupChannel->setReadCallback(std::bind(&EventLoop::handleRead, this));
    wakeupChannel->enableReading();
}

void EventLoop::abortNotInLoopThread() {
    assert(false);
}

EventLoop::~EventLoop() {
    wakeupChannel->disableAll();
    wakeupChannel->remove();
#ifdef __linux__
    Socket::close(wakeupFd);
#else
    Socket::close(wakeupFd[ 0 ]);
    Socket::close(wakeupFd[ 1 ]);
#endif
}

void EventLoop::assertInLoopThread() {
    if (! isInLoopThread()) {
        abortNotInLoopThread();
    }
}

TimerQueuePtr EventLoop::getTimerQueue() {
    return timerQueue;
}

void EventLoop::handlerTimerQueue() {
    timerQueue->handleRead();
}

bool EventLoop::isInLoopThread() const {
    return threadId == std::this_thread::get_id();
}

bool EventLoop::geteventHandling() const {
    return eventHandling;
}

std::thread::id EventLoop::getThreadId() const {
    return threadId;
}

void EventLoop::updateChannel(Channel *channel) {
    assert(channel->ownerLoop() == this);
    assertInLoopThread();
    epoller->updateChannel(channel);
}

void EventLoop::removeChannel(Channel *channel) {
    assert(channel->ownerLoop() == this);
    assertInLoopThread();
    if (eventHandling) {
        assert(currentActiveChannel == channel ||
               std::find(activeChannels.begin(),
                         activeChannels.end(), channel) == activeChannels.end());
    }
    epoller->removeChannel(channel);
}

void EventLoop::cancelAfter(const TimerPtr &timer) {
    timerQueue->cancelTimer(timer);
}

TimerPtr EventLoop::runAfter(double when, bool repeat, TimerCallback &&cb) {
    return timerQueue->addTimer(when, repeat, std::move(cb));
}

TimerPtr EventLoop::runAt(TimeStamp &&stamp, double when, bool repeat, TimerCallback &&cb) {
	return timerQueue->addTimer(std::move(stamp), when, repeat, std::move(cb));
}

bool EventLoop::hasChannel(Channel *channel) {
    assert(channel->ownerLoop() == this);
    assertInLoopThread();
    return epoller->hasChannel(channel);
}

void EventLoop::handleRead() {
    uint64_t one = 1;
#ifdef __linux__
    ssize_t n =  Socket::read(wakeupFd, &one, sizeof one);
#endif

#ifdef __APPLE__
    ssize_t n = Socket::read(wakeupFd[1], &one, sizeof one);
#endif

#ifdef _WIN64
    ssize_t n = Socket::read(wakeupFd[0], &one, sizeof one);
#endif
    assert(n == sizeof one);
}

void EventLoop::quit() {
    running = false;
    if (! isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
#ifdef __linux__
    ssize_t n = Socket::write(wakeupFd, &one, sizeof one);
#endif

#ifdef __APPLE__
    ssize_t n = Socket::write(wakeupFd[0], &one, sizeof one);
#endif

#ifdef _WIN64
    ssize_t n = Socket::write(wakeupFd[1], &one, sizeof(one));
#endif
    assert(n == sizeof one);
}

void EventLoop::runInLoop(Functor &&cb) {
    if (isInLoopThread()) {
        cb();
    }
    else {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(Functor &&cb) {
    {
        std::unique_lock <std::mutex> lk(mutex);
        pendingFunctors.push_back(std::move(cb));
    }

    if (! isInLoopThread() || callingPendingFunctors) {
        wakeup();
    }
}

void EventLoop::doPendingFunctors() {
    callingPendingFunctors = true;

    {
        std::unique_lock <std::mutex> lk(mutex);
        functors.swap(pendingFunctors);
    }

    for ( size_t i = 0 ; i < functors.size() ; ++ i ) {
        functors[ i ]();
    }

    functors.clear();
    callingPendingFunctors = false;
}

void EventLoop::run() {
    running = true;
    while ( running ) {
        activeChannels.clear();
        epoller->epollWait(&activeChannels);
        eventHandling = true;

        for ( auto &it : activeChannels ) {
            currentActiveChannel = it;
            currentActiveChannel->handleEvent();
        }

        currentActiveChannel = nullptr;
        eventHandling        = false;
        doPendingFunctors();
    }
}


