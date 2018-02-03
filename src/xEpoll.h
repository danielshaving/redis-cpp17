#pragma once
#ifdef __linux__
#include "all.h"
#include "xLog.h"

class xChannel;
class xEventLoop;

class xEpoll:noncopyable
{
public:
	typedef std::vector<struct epoll_event> EventList;
	typedef std::vector<xChannel*>          ChannelList;
	typedef std::unordered_map<int32_t, xChannel*> 	ChannelMap;
	
	xEpoll(xEventLoop * loop);
	~xEpoll();

	void	epollWait(ChannelList* activeChannels,int32_t msTime = 10);
	bool	hasChannel(xChannel* channel);
	void	updateChannel(xChannel* channel);
	void	removeChannel(xChannel* channel);
	void	update(int32_t operation, xChannel* channel);
	void 	fillActiveChannels(int32_t numEvents, ChannelList* activeChannels) const;

 private:
	ChannelMap channels;
	EventList 	events;
	xEventLoop  *loop;
	int32_t epollFd;
};
#endif
