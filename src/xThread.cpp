#include "xThread.h"
#include "xEventLoop.h"

xThread::xThread(const ThreadInitCallback& cb)
:loop(nullptr),
 exiting(false),
 callback(cb)
{

}


xThread::~xThread()
{

}


xEventLoop *xThread::startLoop()
{
	threads = std::shared_ptr<std::thread>(new std::thread(std::bind(&xThread::threadFunc,this)));
	threads->detach();
	{
		std::unique_lock<std::mutex> lk(mutex);
		while (loop == nullptr)
		{
			condition.wait(lk);
		}

	}
	return loop;

}

void xThread::threadFunc()
{
	xEventLoop xloop;

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
	loop = nullptr;
}
