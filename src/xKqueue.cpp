
#ifdef __APPLE__
#include "xKqueue.h"
#include "xChannel.h"
#include "xEventLoop.h"

const int kNew = -1;
const int kAdded = 1;
const int kDeleted = 2;

xKqueue::xKqueue(xEventLoop * loop)
:events(128),
loop(loop),
kqueueFd(-1)
{
	kqueueFd = kqueue();
	if (kqueueFd < 0)
	{
		LOG_WARN<<"create kqueueFd Failed error " << kqueueFd <<strerror(errno);

	}

}

xKqueue::~xKqueue()
{
	::close(kqueueFd);
}


void  xKqueue::epollWait(ChannelList* activeChannels,int msTime)
{
    struct timespec timeout;
	timeout.tv_sec = msTime;
	timeout.tv_nsec = (msTime % 1000) * 1000 * 1000;

	int numEvents = kevent(kqueueFd, nullptr,0,&*events.begin(), static_cast<int>(events.size()), &timeout);
	int savedErrno = errno;
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

}


bool xKqueue::hasChannel(xChannel* channel)
{
	loop->assertInLoopThread();
	auto it = channels.find(channel->getfd());
	return it != channels.end() && it->second == channel;
}


void xKqueue::updateChannel(xChannel* channel)
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

		if (channel->isNoneEvent())
		{
			delUpdate(channel);
			channel->setIndex(kDeleted);
		}
		else
		{
			addUpdate(channel);
		}
	}
}

void xKqueue::removeChannel(xChannel* channel)
{

	loop->assertInLoopThread();
	int fd = channel->getfd();
	assert(channels.find(fd) != channels.end());
	assert(channels[fd] == channel);
	assert(channel->isNoneEvent());
	int index = channel->getIndex();
	assert(index == kAdded || index == kDeleted);
	size_t n = channels.erase(fd);
	(void)n;
	assert(n == 1);

	if (index == kAdded)
	{
		delUpdate(channel);
	}

	channel->setIndex(kNew);
}


void	xKqueue::delUpdate(xChannel* channel)
{
	struct kevent ev;
	if (channel->readEnabled())
	{
		EV_SET(&ev, channel->getfd(), EVFILT_READ, EV_DELETE, 0, 0, channel);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}

	}

	if (channel->writeEnabled())
	{
		EV_SET(&ev, channel->getfd(), EVFILT_WRITE, EV_DELETE, 0, 0,channel);
		int r = kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}
	}
}


void	xKqueue::addUpdate(xChannel* channel)
{
	struct kevent ev;
	if (channel->readEnabled())
	{
		EV_SET(&ev, channel->getfd(), EVFILT_READ, EV_ADD, 0, 0, channel);
		int r = kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}
	}

	if (channel->writeEnabled())
	{
		EV_SET(&ev, channel->getfd(), EVFILT_WRITE, EV_ADD, 0, 0, channel);
		int r = kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr);
		if(kevent(kqueueFd, &ev, 1, nullptr, 0, nullptr) == -1)
		{
			LOG_ERROR<<"kqueue failred";
		}
	}



}

void xKqueue::fillActiveChannels(int numEvents, ChannelList* activeChannels) const
{
	for (int i = 0; i < numEvents; ++i)
	{
		xChannel* channel = static_cast<xChannel*>(events[i].udata);
		int fd = channel->getfd();
		auto  it = channels.find(fd);
		assert(it != channels.end());
		assert(it->second == channel);
		channel->setRevents(events[i].data);
		activeChannels->push_back(channel);
	}
}

#endif

























