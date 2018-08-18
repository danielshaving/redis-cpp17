#include "thread.h"
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
