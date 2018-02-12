#pragma once
#ifdef __APPLE__
#include "all.h"
#include "xLog.h"

class xChannel;
class xEventLoop;

class xPoll:noncopyable
{
public:
	typedef std::vector<struct pollfd> EventList;
	typedef std::vector<xChannel*> ChannelList;
	typedef std::unordered_map<int, xChannel*> 	ChannelMap;

	xPoll(xEventLoop * loop);
	~xPoll();

	void epollWait(ChannelList* activeChannels,int msTime = 10);
	bool hasChannel(xChannel* channel);
	void updateChannel(xChannel* channel);
	void removeChannel(xChannel* channel);
	void fillActiveChannels(int numEvents, ChannelList* activeChannels) const;

 private:
	ChannelMap channels;
	EventList events;
	xEventLoop  *loop;
};

#endif
