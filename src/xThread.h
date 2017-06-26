#pragma once

#include "all.h"

class xEventLoop;

class xThread: noncopyable
{
public:
	typedef std::function<void(xEventLoop*)> ThreadInitCallback;

	xThread(const ThreadInitCallback& cb = ThreadInitCallback());
	~xThread();

	xEventLoop *startLoop();

private:
	void threadFunc();
	xEventLoop *loop;
	bool exiting;
	std::thread * xthread;
	mutable std::mutex mutex;
	std::condition_variable condition;
	ThreadInitCallback callback;


};
