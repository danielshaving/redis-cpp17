#include "threadpool.h"
#include "eventloop.h"

Thread::Thread(const ThreadInitCallback &cb)
	:loop(nullptr),
	exiting(false),
	callback(std::move(cb))
{

}

Thread::~Thread()
{

}

EventLoop *Thread::startLoop()
{
	std::thread t(std::bind(&Thread::threadFunc, this));
	t.detach();
	{
		std::unique_lock<std::mutex> lk(mutex);
		while (loop == nullptr)
		{
			condition.wait(lk);
		}
	}
	return loop;

}

void Thread::threadFunc()
{
	EventLoop xloop;

	if (callback)
	{
		callback(&xloop);
	}

	{
		std::unique_lock<std::mutex> lk(mutex);
		loop = &xloop;
		condition.notify_one();
	}

	xloop.run();
}

ThreadPool::ThreadPool(EventLoop *baseLoop)
	:baseLoop(baseLoop),
	started(false),
	numThreads(0),
	next(0)
{

}

ThreadPool::~ThreadPool()
{
	threads.clear();
}

void ThreadPool::start(const ThreadInitCallback &cb)
{
	assert(!started);
	baseLoop->assertInLoopThread();

	started = true;

	for (int i = 0; i < numThreads; i++)
	{
		ThreadPtr t(new Thread(cb));
		threads.push_back(t);
		loops.push_back(t->startLoop());
	}

	if (numThreads == 0 && cb)
	{
		cb(baseLoop);
	}
}

EventLoop *ThreadPool::getNextLoop()
{
	assert(started);
	EventLoop *loop = baseLoop;

	if (!loops.empty())
	{
		loop = loops[next];
		++next;
		if (next >= loops.size())
		{
			next = 0;
		}
	}
	return loop;
}

EventLoop *ThreadPool::getLoopForHash(size_t hashCode)
{
	baseLoop->assertInLoopThread();
	EventLoop *loop = baseLoop;

	if (!loops.empty())
	{
		loop = loops[hashCode % loops.size()];
	}
	return loop;
}

std::vector<EventLoop*> ThreadPool::getAllLoops()
{
	baseLoop->assertInLoopThread();
	assert(started);
	if (loops.empty())
	{
		return std::vector<EventLoop*>(1, baseLoop);
	}
	else
	{
		return loops;
	}
}

