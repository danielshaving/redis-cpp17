#ifdef __linux__
#include "epoll.h"
#include "channel.h"
#include "eventloop.h"

const int32_t kNew = -1;
const int32_t kAdded = 1;
const int32_t kDeleted = 2;

Epoll::Epoll(EventLoop *loop)
:events(64),
loop(loop),
epollFd(::epoll_create1(EPOLL_CLOEXEC))
{
	assert(epollFd >= 0);
}

Epoll::~Epoll()
{
	::close(epollFd);
}

void Epoll::epollWait(ChannelList *activeChannels,int32_t msTime)
{
	int32_t numEvents = ::epoll_wait(epollFd,&*events.begin(),events.size(),msTime);
	int32_t savedErrno = errno;

	if (numEvents > 0)
	{
		fillActiveChannels(numEvents,activeChannels);
		if (numEvents == events.size()) { events.resize(events.size() * 2); }
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

bool Epoll::hasChannel(Channel *channel)
{
	loop->assertInLoopThread();
	auto it = channels.find(channel->getfd());
	return it != channels.end() && it->second == channel;
}

void Epoll::updateChannel(Channel *channel)
{
	loop->assertInLoopThread();
	const int32_t index = channel->getIndex();
	if (index == kNew || index == kDeleted)
	{
		int32_t fd = channel->getfd();
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
		update(EPOLL_CTL_ADD,channel);
	}
	else
	{
		int32_t fd = channel->getfd();
		(void)fd;
		assert(channels.find(fd) != channels.end());
		assert(channels[fd] == channel);
		assert(index == kAdded);
		if (channel->isNoneEvent())
		{
			update(EPOLL_CTL_DEL,channel);
			channel->setIndex(kDeleted);
		}
		else
		{
			update(EPOLL_CTL_MOD,channel);
		}
	}
}

void Epoll::removeChannel(Channel *channel)
{
	loop->assertInLoopThread();
	int32_t fd = channel->getfd();
	int32_t index = channel->getIndex();
	assert(channels.find(fd) != channels.end());
	assert(channels[fd] == channel);
	assert(channel->isNoneEvent());
	assert(index == kAdded || index == kDeleted);
	size_t n = channels.erase(fd);
	(void)n;
	assert(n == 1);

	if (index == kAdded)
	{
		update(EPOLL_CTL_DEL,channel);
	}
	
	channel->setIndex(kNew);
}

void Epoll::update(int32_t operation,Channel *channel)
{
	struct epoll_event event;
	bzero(&event,sizeof event);
	event.events = channel->getEvents();
	event.data.ptr = channel;
	int32_t fd = channel->getfd();
	if (::epoll_ctl(epollFd,operation,fd,&event) < 0)
	{
		LOG_WARN<<"epoll_ctl "<<fd;
	}
}

void Epoll::fillActiveChannels(int32_t numEvents,ChannelList *activeChannels) const
{
	for (int32_t i = 0; i < numEvents; ++i)
	{
		Channel *channel = static_cast<Channel*>(events[i].data.ptr);
		int32_t fd = channel->getfd();
		auto it = channels.find(fd);
		assert(it != channels.end());
		assert(it->second == channel);
		channel->setRevents(events[i].events);
		activeChannels->push_back(channel);
	}
}
#endif

























