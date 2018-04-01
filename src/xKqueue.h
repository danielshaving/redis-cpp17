#pragma once

#ifdef __APPLE__
#include "all.h"
#include "xLog.h"

class xChannel;
class xEventLoop;

class xKqueue : noncopyable
{
public:
	typedef std::vector<struct kevent> EventList;
	typedef std::vector<xChannel*> ChannelList;
	typedef std::unordered_map<int32_t, xChannel*> ChannelMap;

	xKqueue(xEventLoop *loop);
	~xKqueue();

	void epollWait(ChannelList *activeChannels,int32_t msTime = 10);
	bool hasChannel(xChannel *channel);
	void updateChannel(xChannel *channel);
	void removeChannel(xChannel *channel);
	void delUpdate(xChannel *channel);
	void addUpdate(xChannel *channel);
	void fillActiveChannels(int32_t numEvents,ChannelList *activeChannels) const;

 private:
	ChannelMap channels;
	EventList 	events;
	xEventLoop  *loop;
	int kqueueFd;
};

#endif

