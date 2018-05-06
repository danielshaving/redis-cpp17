#pragma once
#include "xAll.h"

class xEventLoop;
class xThread
{
public:
	typedef std::function<void(xEventLoop*)> ThreadInitCallback;

	xThread(const ThreadInitCallback &cb = ThreadInitCallback());
	~xThread();

	xEventLoop *startLoop();

private:
	xThread(const xThread&);
	void operator=(const xThread&);

	void threadFunc();
	xEventLoop *loop;
	bool exiting;
	mutable std::mutex mutex;
	std::condition_variable condition;
	ThreadInitCallback callback;
};
