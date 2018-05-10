#pragma once
#ifdef __APPLE__
#include "all.h"
#include "log.h"

class Channel;
class EventLoop;

class Kqueue : boost::noncopyable
{
public:
	typedef std::vector<struct kevent> EventList;
	typedef std::vector<Channel*> ChannelList;
	typedef std::unordered_map<int32_t, Channel*> ChannelMap;

	Kqueue(EventLoop *loop);
	~Kqueue();

	void epollWait(ChannelList *activeChannels,int32_t msTime = 100);
	bool hasChannel(Channel *channel);
	void updateChannel(Channel *channel);
	void removeChannel(Channel *channel);
	void delUpdate(Channel *channel);
	void addUpdate(Channel *channel);
	void fillActiveChannels(int32_t numEvents,ChannelList *activeChannels) const;

 private:
	ChannelMap channels;
	EventList events;
	EventLoop *loop;
	int32_t kqueueFd;
};

#endif

