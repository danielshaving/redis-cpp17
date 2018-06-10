#pragma once
#ifdef __APPLE__
#include "all.h"
#include "log.h"

class Channel;
class EventLoop;

class Poll
{
public:
	typedef std::vector<struct pollfd> EventList;
	typedef std::vector<Channel*> ChannelList;
	typedef std::unordered_map<int32_t,Channel*> ChannelMap;

	Poll(EventLoop *loop);
	~Poll();

	void epollWait(ChannelList *activeChannels,int32_t msTime = 100);
	bool hasChannel(Channel *channel);
	void updateChannel(Channel *channel);
	void removeChannel(Channel *channel);
	void fillActiveChannels(int32_t numEvents,ChannelList *activeChannels) const;

private:
	Poll(const Poll&);
	void operator=(const Poll&);

	ChannelMap channels;
	EventList events;
	EventLoop *loop;
};

#endif
