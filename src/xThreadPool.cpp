#include "xThreadPool.h"
#include "xEventLoop.h"
#include "xThread.h"

xThreadPool::xThreadPool(xEventLoop *baseLoop)
:baseLoop(baseLoop),
 started(false),
 numThreads(0),
 next(0)
{

}
xThreadPool::~xThreadPool()
{
	threads.clear();
}

void xThreadPool::start(const ThreadInitCallback& cb)
{
	assert(!started);
	baseLoop->assertInLoopThread();

	started = true;

	for(int i = 0 ; i < numThreads; i++)
	{
		std::shared_ptr<xThread>  t(new xThread(cb));
		threads.push_back(t);
		loops.push_back(t->startLoop());
	}

	if(numThreads == 0 && cb)
	{
		cb(baseLoop);
	}


}


xEventLoop* xThreadPool::getNextLoop()
{
	baseLoop->assertInLoopThread();
	assert(started);
	xEventLoop* loop = baseLoop;

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

xEventLoop* xThreadPool::getLoopForHash(size_t hashCode)
{
	baseLoop->assertInLoopThread();
	xEventLoop* loop = baseLoop;

	if (!loops.empty())
	{
		loop = loops[hashCode % loops.size()];
	}
	return loop;
}

std::vector<xEventLoop*> xThreadPool::getAllLoops()
{
	//baseLoop->assertInLoopThread();
	assert(started);
	if (loops.empty())
	{
		return std::vector<xEventLoop*>(1, baseLoop);
	}
	else
	{
		return loops;
	}
}

