#include "xThread.h"
#include "xCurrentThread.h"
#include "xEventLoop.h"

namespace xCurrentThread
{

	__thread int t_cachedTid = 0;
	void cacheTid()
	{
		if (t_cachedTid == 0)
		{
			t_cachedTid =  static_cast<pid_t>(::syscall(SYS_gettid));
		}
	}
}



xThread::xThread(const ThreadInitCallback& cb)
:loop(nullptr),
 exiting(false),
 callback(cb)
{

}


xThread::~xThread()
{
	delete xthread;
}


xEventLoop *xThread::startLoop()
{
	xthread = new std::thread(std::bind(&xThread::threadFunc,this));
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
