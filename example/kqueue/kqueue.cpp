#ifdef __APPLE__
#include <kqueue.h>
#include "channel.h"
#include "eventloop.h"

const int kNew = -1;
const int kAdded = 1;
const int kDeleted = 2;

Kqueue::Kqueue(EventLoop *loop)
:events(64),
loop(loop),
kqueueFd(kqueue())
{
	if (kqueueFd < 0)
	{
		LOG_WARN<<"create kqueueFd Failed error " << kqueueFd <<strerror(errno);
	}
}

Kqueue::~Kqueue()
{
	::close(kqueueFd);
}

void Kqueue::epollWait(ChannelList *activeChannels,int32_t msTime)
{
    struct timespec timeout;
	timeout.tv_sec = msTime;
	timeout.tv_nsec = 0;

	auto timerQueue = loop->getTimerQueue();
	auto timer = timerQueue->getTimerBegin();
	if (timer)
	{
		timeout.tv_sec = timer->getInterval();
	}

	int32_t numEvents = ::kevent(kqueueFd,nullptr,0,&*events.begin(),static_cast<int32_t>(events.size()),&timeout);
	int32_t savedErrno = errno;
	if (numEvents > 0)
	{
		fillActiveChannels(numEvents, activeChannels);
		if (numEvents == events.size())
		{
			events.resize(events.size()*2);
		}
	}
	else if (numEvents == 0)
	{

	}
	else
	{
		if (savedErrno != EINTR)
		{
			errno = savedErrno;
		}
	}

	loop->handlerTimerQueue();
}

bool Kqueue::hasChannel(Channel *channel)
{
	loop->assertInLoopThread();
	auto it = channels.find(channel->getfd());
	return it != channels.end() && it->second == channel;
}

void Kqueue::updateChannel(Channel *channel)
{
	loop->assertInLoopThread();
	const int index = channel->getIndex();
	if (index == kNew || index == kDeleted)
	{
		int fd = channel->getfd();
		if (index == kNew)
		{
			assert(channels.find(fd) == channels.end());
			channels[fd] = channel;
		}
		else
		{
			assert(channels.find(fd) != channels.end());
			assert(channels[fd] == channel);
		}

		channel->setIndex(kAdded);
		addUpdate(channel);
	}
	else
	{
		int fd = channel->getfd();
		(void)fd;
		assert(channels.find(fd) != channels.end());
		assert(channels[fd] == channel);
		assert(index == kAdded);
		if(index == kAdded)
		{
			delUpdate(channel);
		}
		channel->setIndex(kDeleted);
	}
}

void Kqueue::removeChannel(Channel *channel)
{
	loop->assertInLoopThread();
	int fd = channel->getfd();
	assert(channels.find(fd) != channels.end());
	assert(channels[fd] == channel);
	int index = channel->getIndex();
	assert(index == kAdded || index == kDeleted);
	size_t n = channels.erase(fd);
	(void)n;
	assert(n == 1);
	delUpdate(channel);
	channel->setIndex(kNew);
}


void Kqueue::delUpdate(Channel *channel)
{
	if (channel->readEnabled())
	{
		struct kevent ev;
		EV_SET(&ev, channel->getfd(), EVFILT_READ, EV_DELETE, 0, 0, channel);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}
	}

	if (channel->writeEnabled())
	{
		struct kevent ev;
		EV_SET(&ev, channel->getfd(), EVFILT_WRITE, EV_DELETE, 0, 0,channel);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}
	}
}


void Kqueue::addUpdate(Channel *channel)
{
	if (channel->readEnabled())
	{
		struct kevent ev;
		EV_SET(&ev, channel->getfd(), EVFILT_READ, EV_ADD, 0, 0, channel);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}
	}

	if (channel->writeEnabled())
	{
		struct kevent ev;
		EV_SET(&ev, channel->getfd(), EVFILT_WRITE, EV_ADD, 0, 0, channel);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}
	}

}

void Kqueue::fillActiveChannels(int numEvents,ChannelList *activeChannels) const
{
	for (int i = 0; i < numEvents; ++i)
	{
		Channel* channel = static_cast<Channel*>(events[i].udata);
		if(channel)
		{
//			 if (events[i].flags & EV_ERROR)
//			 {
//				 LOG_ERROR<<"kevent error";
//				 continue;
//			 }
			int fd = channel->getfd();
			auto  it = channels.find(fd);
			assert(it != channels.end());
			assert(it->second == channel);
			activeChannels->push_back(channel);
		}
	}
}
#endif

























