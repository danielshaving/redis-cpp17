
#ifdef __APPLE__
#include "xKqueue.h"
#include "xChannel.h"
#include "xEventLoop.h"

const int kNew = -1;
const int kAdded = 1;
const int kDeleted = 2;

xKqueue::xKqueue(xEventLoop * loop)
:events(64),
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
	timeout.tv_nsec = 0;

	auto timerQueue = loop->getTimerQueue();
	if(timerQueue->size() > 0 )
	{
		auto timer = timerQueue->head();
		timeout.tv_sec = timer->interval;
	}

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

	loop->handlerTimerQueue();
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
		if(index == kAdded)
		{
			delUpdate(channel);
		}
		channel->setIndex(kDeleted);

	}
}

void xKqueue::removeChannel(xChannel* channel)
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


void	xKqueue::delUpdate(xChannel* channel)
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


void	xKqueue::addUpdate(xChannel* channel)
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

void xKqueue::fillActiveChannels(int numEvents, ChannelList* activeChannels) const
{
	for (int i = 0; i < numEvents; ++i)
	{
		xChannel* channel = static_cast<xChannel*>(events[i].udata);
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

























