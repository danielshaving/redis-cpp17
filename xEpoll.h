#pragma once

#include "all.h"

class xChannel;
class xEventLoop;

class xEpoll
{
private:
    typedef std::vector<struct epoll_event> EventList;
    typedef std::vector<xChannel*>          ChannelList;
    typedef std::map<int, xChannel*> 		ChannelMap;
    ChannelMap 				channels;
    EventList 			    events;
    xEventLoop 				*loop;
    int                    		maxFd;;
    int                     		epollFd;

public:
	xEpoll(xEventLoop * loop);
	~xEpoll();

	bool	init(int fdCount);

	void  epollWait(ChannelList* activeChannels,int msTime = 10);

	bool	hasChannel(xChannel* channel);

	void  updateChannel(xChannel* channel);

	void  removeChannel(xChannel* channel);

	void  update(int operation, xChannel* channel);

    void 	fillActiveChannels(int numEvents, ChannelList* activeChannels) const;
};

