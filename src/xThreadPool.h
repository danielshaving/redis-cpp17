#pragma once
#include "xAll.h"
#include "xCallback.h"

class xThread;
class xEventLoop;
class xThreadPool
{
public:
	typedef std::function<void(xEventLoop*)> ThreadInitCallback;
	xThreadPool(xEventLoop *baseLoop);
	~xThreadPool();

	void setThreadNum(int numThreads) { this->numThreads = numThreads; }
	void start(const ThreadInitCallback &cb = ThreadInitCallback());

	xEventLoop *getNextLoop();
	xEventLoop *getLoopForHash(size_t hashCode);
	std::vector<xEventLoop*> getAllLoops();
	bool getStarted() const { return started; }

private:
	xThreadPool(const xThreadPool&);
	void operator=(const xThreadPool&);

	xEventLoop *baseLoop;
	bool started;
	int32_t	numThreads;
	int32_t	next;

	std::vector<ThreadPtr> threads;
	std::vector<xEventLoop*> loops;


};
