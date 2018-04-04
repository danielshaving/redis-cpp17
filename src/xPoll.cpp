#ifdef __APPLE__
#include "xPoll.h"
#include "xChannel.h"
#include "xEventLoop.h"

const int32_t kNew = -1;
const int32_t kAdded = 1;
const int32_t kDeleted = 2;


xPoll::xPoll(xEventLoop *loop)
:loop(loop)
{

}

xPoll::~xPoll()
{

}

void  xPoll::epollWait(ChannelList *activeChannels,int32_t msTime)
{
	auto timerQueue = loop->getTimerQueue();
	if(timerQueue->size() > 0 )
	{
		auto timer = timerQueue->head();
		msTime = timer->interval / 1000;
	}

	int32_t numEvents = ::poll(&*events.begin(),events.size(),msTime);
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

	loop->handlerTimerQueue();

}

bool xPoll::hasChannel(xChannel *channel)
{
	loop->assertInLoopThread();
	auto  it = channels.find(channel->getfd());
	return it != channels.end() && it->second == channel;
}

void xPoll::updateChannel(xChannel *channel)
{
	loop->assertInLoopThread();
	if (channel->getIndex() < 0)
	{
		assert(channels.find(channel->getfd()) == channels.end());
		struct pollfd pfd;
		pfd.fd = channel->getfd();
		pfd.events = static_cast<short>(channel->getEvents());
		pfd.revents = 0;
		events.push_back(pfd);
		int32_t idx = static_cast<int32_t>(events.size())-1;
		channel->setIndex(idx);
		channels[pfd.fd] = channel;
	}
	else
	{
		// update existing one
		assert(channels.find(channel->getfd()) != channels.end());
		assert(channels[channel->getfd()] == channel);
		int32_t idx = channel->getIndex();
		assert(0 <= idx && idx < static_cast<int32_t>(events.size()));
		struct pollfd& pfd = events[idx];
		assert(pfd.fd == channel->getfd() || pfd.fd == -channel->getfd()-1);
		pfd.fd = channel->getfd();
		pfd.events = static_cast<short>(channel->getEvents());
		pfd.revents = 0;
		if (channel->isNoneEvent())
		{
			// ignore this pollfd
			pfd.fd = -channel->getfd()-1;
		}
	}
}

void xPoll::removeChannel(xChannel *channel)
{
	loop->assertInLoopThread();
	assert(channels.find(channel->getfd()) != channels.end());
	assert(channels[channel->getfd()] == channel);
	assert(channel->isNoneEvent());
	int32_t idx = channel->getIndex();
	assert(0 <= idx && idx < static_cast<int32_t>(events.size()));
	const struct pollfd& pfd = events[idx]; (void)pfd;
	assert(pfd.fd == -channel->getfd()-1 && pfd.events == channel->getEvents());
	size_t n = channels.erase(channel->getfd());
	assert(n == 1); (void)n;
	if (idx == events.size()-1)
	{
		events.pop_back();
	}
	else
	{
		int32_t channelAtEnd = events.back().fd;
		iter_swap(events.begin()+idx, events.end()-1);
		if (channelAtEnd < 0)
		{
			channelAtEnd = -channelAtEnd-1;
		}
		channels[channelAtEnd]->setIndex(idx);
		events.pop_back();
	}
}

void xPoll::fillActiveChannels(int32_t numEvents,ChannelList *activeChannels) const
{
	for(auto it = events.begin(); it != events.end() && numEvents > 0 ; ++it)
	{
		if((*it).revents > 0)
		{
			--numEvents;
			auto  iter = channels.find((*it).fd);
			assert(iter != channels.end());
			auto channel = iter->second;
			assert(channel->getfd() == (*it).fd);
			channel->setRevents((*it).revents);
			activeChannels->push_back(channel);
		}
	}

}

#endif

























