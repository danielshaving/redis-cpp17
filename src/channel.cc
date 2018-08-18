#include "channel.h"
#include "eventloop.h"

const int32_t Channel::kNoneEvent = 0;
const int32_t Channel::kReadEvent = POLLIN | POLLPRI;
const int32_t Channel::kWriteEvent = POLLOUT;

Channel::Channel(EventLoop *loop, int32_t fd)
	:loop(loop),
	fd(fd),
	events(0),
	revents(0),
	index(-1),
	tied(false),
	eventHandling(false),
	addedToLoop(false)
{

}

Channel::~Channel()
{
	assert(!eventHandling);
	assert(!addedToLoop);

	if (loop->isInLoopThread())
	{
		assert(!loop->hasChannel(this));
	}
}

void Channel::remove()
{
	assert(isNoneEvent());
	addedToLoop = false;
	loop->removeChannel(this);
}

void Channel::update()
{
	addedToLoop = true;
	loop->updateChannel(this);
}

void Channel::handleEventWithGuard()
{
	eventHandling = true;

	if ((revents & POLLHUP) && !(revents & POLLIN))
	{
		if (logHup)
		{

		}

		if (closeCallback)
		{
			closeCallback();
		}
	}

	if (revents & POLLNVAL)
	{

	}

	if (revents & (POLLERR | POLLNVAL))
	{
		if (errorCallback)
		{
			errorCallback();
		}
	}

#ifndef POLLRDHUP
	const int POLLRDHUP = 0;
#endif

	if (revents & (POLLIN | POLLPRI | POLLRDHUP))
	{
		if (readCallback)
		{
			readCallback();
		}
	}

	if (revents & POLLOUT)
	{
		if (writeCallback)
		{
			writeCallback();
		}
	}
	eventHandling = false;
}

void Channel::handleEvent()
{
	std::shared_ptr<void> guard;
	if (tied)
	{
		guard = tie.lock();
		if (guard)
		{
			handleEventWithGuard();
		}
	}
	else
	{
		handleEventWithGuard();
	}
}

void Channel::setTie(const std::shared_ptr<void> &obj)
{
	tie = obj;
	tied = true;
}
