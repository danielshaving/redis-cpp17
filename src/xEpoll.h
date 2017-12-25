#pragma once

#include "all.h"
#include "xLog.h"

class xChannel;
class xEventLoop;

class xEpoll:noncopyable
{
public:
	typedef std::vector<struct epoll_event> EventList;
	typedef std::vector<xChannel*>          ChannelList;
	typedef std::unordered_map<int, xChannel*> 	ChannelMap;
	
	xEpoll(xEventLoop * loop);
	~xEpoll();

	bool	init(int fdCount);
	void	epollWait(ChannelList* activeChannels,int msTime = 10);
	bool	hasChannel(xChannel* channel);
	void	updateChannel(xChannel* channel);
	void	removeChannel(xChannel* channel);
	void	update(int operation, xChannel* channel);
	void 	fillActiveChannels(int numEvents, ChannelList* activeChannels) const;

 private:
	ChannelMap channels;
	EventList 	events;
	xEventLoop  *loop;
	int maxFd;;
	int epollFd;
};

