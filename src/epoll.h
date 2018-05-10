#pragma once
#ifdef __linux__
#include "all.h"
#include "stringpiece.h"
#include "log.h"

class Channel;
class EventLoop;

class Epoll
{
public:
	typedef std::vector<struct epoll_event> EventList;
	typedef std::vector<Channel*> ChannelList;
	typedef std::unordered_map<int32_t,Channel*> ChannelMap;
	
	Epoll(EventLoop *loop);
	~Epoll();

	void epollWait(ChannelList *activeChannels,int32_t msTime = 100);
	bool hasChannel(Channel *channel);
	void updateChannel(Channel *channel);
	void removeChannel(Channel *channel);
	void update(int32_t operation,Channel *channel);
	void fillActiveChannels(int32_t numEvents,ChannelList *activeChannels) const;

private:
	Epoll(const Epoll&);
	void operator=(const Epoll&);

	ChannelMap channels;
	EventList events;
	EventLoop *loop;
	int32_t epollFd;
};
#endif
