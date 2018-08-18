#ifdef _WIN32
#include "select.h"
#include "channel.h"
#include "log.h"
#include "eventloop.h"

Select::Select(EventLoop *loop)
	:loop(loop)
{

}

Select::~Select()
{

}

void Select::epollWait(ChannelList *activeChannels, int32_t msTime)
{
	FD_ZERO(&rfds);
	FD_ZERO(&wfds);
	FD_ZERO(&efds);

	size_t maxfd = 0;
	for (auto &it : events)
	{
		if ((it.events) & (POLLIN | POLLPRI ))
		{
			FD_SET(it.fd, &rfds);
		}

		if ((it.events) & (POLLOUT))
		{
			FD_SET(it.fd, &wfds);
		}

		FD_SET(it.fd,&efds);
		if (it.fd > maxfd)
			maxfd = it.fd;
	}

	auto timerQueue = loop->getTimerQueue();
	msTime = timerQueue->getTimeout();

	timeval timeout;
	timeout.tv_sec = msTime / 1000;
	timeout.tv_usec = static_cast<int>(msTime % 1000) * 1000;

	int numEvents = ::select(maxfd + 1,&rfds,&wfds,&efds,&timeout);
	int saveErrno = GetLastError();

	if (numEvents > 0)
	{
		fillActiveChannels(numEvents,activeChannels);
	}
	else if (numEvents == 0)
	{

	}
	else
	{
		if (saveErrno != EINTR)
		{
			errno = saveErrno;
			LOG_WARN << strerror(errno);
		}
	}
	loop->handlerTimerQueue();
}

bool Select::hasChannel(Channel *channel)
{
	loop->assertInLoopThread();
	auto it = channels.find(channel->getfd());
	return it != channels.end() && it->second == channel;
}

void Select::updateChannel(Channel *channel)
{
	loop->assertInLoopThread();
	if (channel->getIndex() < 0)
	{
		assert(channels.find(channel->getfd()) == channels.end());
		struct pollfd it;
		it.fd = channel->getfd();
		it.events = static_cast<short>(channel->getEvents());
		it.revents = 0;
		events.push_back(it);
		int32_t idx = static_cast<int32_t>(events.size()) - 1;
		channel->setIndex(idx);
		channels[it.fd] = channel;
	}
	else
	{
		assert(channels.find(channel->getfd()) != channels.end());
		assert(channels[channel->getfd()] == channel);
		int32_t idx = channel->getIndex();
		assert(0 <= idx && idx < static_cast<int32_t>(events.size()));
		struct pollfd &it = events[idx];
		assert(it.fd == channel->getfd() || it.fd == -channel->getfd() - 1);
		it.events = static_cast<short>(channel->getEvents());
		it.revents = 0;
		if (channel->isNoneEvent())
		{
			it.fd = -channel->getfd() - 1;
		}
	}
}

void Select::removeChannel(Channel *channel)
{
	loop->assertInLoopThread();
	assert(channels.find(channel->getfd()) != channels.end());
	assert(channels[channel->getfd()] == channel);
	assert(channel->isNoneEvent());
	int32_t idx = channel->getIndex();
	assert(0 <= idx && idx < static_cast<int32_t>(events.size()));
	const struct pollfd &it = events[idx]; (void)it;
	assert(it.fd == -channel->getfd() - 1 && it.events == channel->getEvents());
	size_t n = channels.erase(channel->getfd());
	assert(n == 1); (void)n;
	if (idx == events.size() - 1)
	{
		events.pop_back();
	}
	else
	{
		int32_t channelAtEnd = events.back().fd;
		iter_swap(events.begin() + idx, events.end() - 1);
		if (channelAtEnd < 0)
		{
			channelAtEnd = -channelAtEnd - 1;
		}

		channels[channelAtEnd]->setIndex(idx);
		events.pop_back();
	}
}

void Select::fillActiveChannels(int32_t numEvents, ChannelList *activeChannels) const
{
	for (auto it = events.begin(); it != events.end() && numEvents > 0; ++it)
	{
		int revents = 0;//it->revents is readonly
		if (FD_ISSET(it->fd,&rfds))
		{
			revents |= POLLIN;
		}

		if (FD_ISSET(it->fd,&wfds))
		{
			revents |= POLLOUT;
		}

		if (FD_ISSET(it->fd,&efds))
		{
			revents |= POLLERR;
		}

		if (revents > 0)
		{
			--numEvents;
			auto iter = channels.find(it->fd);
			assert(iter != channels.end());
			Channel *channel = iter->second;
			assert(channel->getfd() == it->fd);
			channel->setRevents(revents);
			activeChannels->push_back(channel);
		}
	}
}
#endif
