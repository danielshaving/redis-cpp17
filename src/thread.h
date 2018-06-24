//
// Created by zhanghao on 2018/6/17.
//

#pragma once
#include "all.h"

class EventLoop;
class Thread
{
public:
	typedef std::function<void(EventLoop*)> ThreadInitCallback;

	Thread(const ThreadInitCallback &cb = ThreadInitCallback());
	~Thread();
	EventLoop *startLoop();

private:
	Thread(const Thread&);
	void operator=(const Thread&);

	void threadFunc();
	EventLoop *loop;
	bool exiting;
	mutable std::mutex mutex;
	std::condition_variable condition;
	ThreadInitCallback callback;
};
