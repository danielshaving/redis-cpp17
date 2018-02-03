#ifdef __linux__

#include "xEpoll.h"
#include "xChannel.h"
#include "xEventLoop.h"

const int32_t kNew = -1;
const int32_t kAdded = 1;
const int32_t kDeleted = 2;


xEpoll::xEpoll(xEventLoop * loop)
:events(64),
loop(loop),
epollFd(-1)
{
	epollFd = ::epoll_create1(EPOLL_CLOEXEC);

	if (epollFd < 0)
	{
		LOG_WARN<<"create epollFd Failed error " << epollFd <<strerror(errno);
	}

}

xEpoll::~xEpoll()
{
	::close(epollFd);
}


void  xEpoll::epollWait(ChannelList* activeChannels,int32_t msTime)
{
	int32_t numEvents = ::epoll_wait(epollFd, &*events.begin(), static_cast<int32_t>(events.size()), msTime);
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
		  	LOG_WARN<<"wait error"<<errno;
		}
	}

}


bool xEpoll::hasChannel(xChannel* channel)
{
	loop->assertInLoopThread();
	auto  it = channels.find(channel->getfd());
	return it != channels.end() && it->second == channel;
}


void xEpoll::updateChannel(xChannel* channel)
{
	loop->assertInLoopThread();
	const int32_t index = channel->getIndex();
	if (index == kNew || index == kDeleted)
	{
		int32_t fd = channel->getfd();
		if (index == kNew)
		{
#ifdef __DEBUG__
			assert(channels.find(fd) == channels.end());
#endif
			channels[fd] = channel;
		}
		else 
		{
#ifdef __DEBUG__
			assert(channels.find(fd) != channels.end());
			assert(channels[fd] == channel);
#endif
		}
		channel->setIndex(kAdded);
		update(EPOLL_CTL_ADD, channel);
	}
	else
	{
#ifdef __DEBUG__
		int32_t fd = channel->getfd();
		(void)fd;
		assert(channels.find(fd) != channels.end());
		assert(channels[fd] == channel);
		assert(index == kAdded);
#endif
		if (channel->isNoneEvent())
		{
			update(EPOLL_CTL_DEL, channel);
			channel->setIndex(kDeleted);
		}
		else
		{
			update(EPOLL_CTL_MOD, channel);
		}
	}
}




void xEpoll::removeChannel(xChannel* channel)
{
	loop->assertInLoopThread();
	int32_t fd = channel->getfd();
	int32_t index = channel->getIndex();
#ifdef __DEBUG__
	assert(channels.find(fd) != channels.end());
	assert(channels[fd] == channel);
	assert(channel->isNoneEvent());
	assert(index == kAdded || index == kDeleted);
#endif
	size_t n = channels.erase(fd);
	(void)n;
	assert(n == 1);

	if (index == kAdded)
	{
		update(EPOLL_CTL_DEL, channel);
	}
	
	channel->setIndex(kNew);
}

void xEpoll::update(int32_t operation, xChannel* channel)
{
	struct epoll_event event;
	bzero(&event, sizeof event);
	event.events = channel->getEvents();
	event.data.ptr = channel;
	int32_t fd = channel->getfd();
	if (::epoll_ctl(epollFd, operation, fd, &event) < 0)
	{
		LOG_ERROR<<"epoll_ctl "<<fd;
	}
}

void xEpoll::fillActiveChannels(int32_t numEvents, ChannelList* activeChannels) const
{
	for (int32_t i = 0; i < numEvents; ++i)
	{
		xChannel* channel = static_cast<xChannel*>(events[i].data.ptr);
#ifdef __DEBUG__
		int32_t fd = channel->getfd();
		auto  it = channels.find(fd);
		assert(it != channels.end());
		assert(it->second == channel);
#endif
		channel->setRevents(events[i].events);
		activeChannels->push_back(channel);
	}
}
#endif

























