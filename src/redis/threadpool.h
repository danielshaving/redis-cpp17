#pragma once

#include "all.h"
#include "callback.h"

class EventLoop;

class Thread {
public:
    typedef std::function<void(EventLoop *)> ThreadInitCallback;

    Thread(const ThreadInitCallback &cb = ThreadInitCallback());

    ~Thread();

    EventLoop *startLoop();

private:
    Thread(const Thread &);

    void operator=(const Thread &);

    void threadFunc();

    EventLoop *loop;
    bool exiting;
    mutable std::mutex mutex;
    std::condition_variable condition;
    ThreadInitCallback callback;
};

class ThreadPool {
public:
    typedef std::function<void(EventLoop *)> ThreadInitCallback;

    ThreadPool(EventLoop *baseLoop);

    ~ThreadPool();

    void setThreadNum(int numThreads) { this->numThreads = numThreads; }

    void start(const ThreadInitCallback &cb = ThreadInitCallback());

    EventLoop *getNextLoop();

    EventLoop *getLoopForHash(size_t hashCode);

    std::vector<EventLoop *> getAllLoops();

    bool getStarted() const { return started; }

private:
    ThreadPool(const ThreadPool &);

    void operator=(const ThreadPool &);

    EventLoop *baseLoop;
    bool started;
    int32_t numThreads;
    int32_t next;

    std::vector <ThreadPtr> threads;
    std::vector<EventLoop *> loops;


};
