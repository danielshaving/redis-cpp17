#pragma once
#ifdef _WIN64
#include "all.h"
#include "util.h"
#include "log.h"
#include "timer.h"

class Channel;
class EventLoop;
class Select
{
public:
	typedef std::vector<Channel*> ChannelList;
	typedef std::unordered_map<int32_t, Channel*> ChannelMap;
	typedef std::vector<struct pollfd> EventList;

	Select(EventLoop *loop);
	~Select();

	void epollWait(ChannelList *activeChannels, int32_t msTime = 100);
	void updateChannel(Channel *channel);
	void removeChannel(Channel *channel);
	bool hasChannel(Channel *channel);

private:
	void fillActiveChannels(int32_t numEvents, ChannelList *activeChannels) const;

	ChannelMap channels;
	EventList events;
	EventLoop *loop;

	fd_set rfds;
	fd_set wfds;
	fd_set efds;
};
#endif