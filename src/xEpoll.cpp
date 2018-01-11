#include "xEpoll.h"
#include "xChannel.h"
#include "xEventLoop.h"

const int kNew = -1;
const int kAdded = 1;
const int kDeleted = 2;


xEpoll::xEpoll(xEventLoop * loop)
:events(16),
loop(loop),
maxFd(0),
epollFd(-1)
{
	init(-1);
}

xEpoll::~xEpoll()
{
	 ::close(epollFd);
}

bool xEpoll::init(int fdCount)
{
    maxFd = fdCount;
    epollFd = ::epoll_create1(EPOLL_CLOEXEC);

    if (epollFd < 0)
    {
        LOG_WARN<<"create epollFd Failed error " << epollFd <<strerror(errno);
        return false;
    }

    return true;
}

void  xEpoll::epollWait(ChannelList* activeChannels,int msTime)
{
	int numEvents = epoll_wait(epollFd, &*events.begin(), static_cast<int>(events.size()), msTime);
	int savedErrno = errno;

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
		// error happens, log uncommon ones
		if (savedErrno != EINTR)
		{
		  errno = savedErrno;
		  //TRACE("wait error %d",errno);
		}
	}

}


bool xEpoll::hasChannel(xChannel* channel)
{
	loop->assertInLoopThread();
	ChannelMap::const_iterator it = channels.find(channel->getfd());
	return it != channels.end() && it->second == channel;
}



void xEpoll::updateChannel(xChannel* channel)
{
  loop->assertInLoopThread();
  const int index = channel->getIndex();
  if (index == kNew || index == kDeleted)
  {
    // a new one, add with EPOLL_CTL_ADD
    int fd = channel->getfd();
    if (index == kNew)
    {
      assert(channels.find(fd) == channels.end());
      channels[fd] = channel;
    }
    else // index == kDeleted
    {
      assert(channels.find(fd) != channels.end());
      assert(channels[fd] == channel);
    }

    channel->setIndex(kAdded);
    update(EPOLL_CTL_ADD, channel);
  }
  else
  {
    // update existing one with EPOLL_CTL_MOD/DEL
    int fd = channel->getfd();
    (void)fd;
    assert(channels.find(fd) != channels.end());
    assert(channels[fd] == channel);
    assert(index == kAdded);
    if (channel->isNoneEvent())
    {
      update(EPOLL_CTL_DEL, channel);
      channel->setIndex(kDeleted);
    }
    else
    {
      update(EPOLL_CTL_MOD, channel);
    }
  }
}




void xEpoll::removeChannel(xChannel* channel)
{
  loop->assertInLoopThread();
  int fd = channel->getfd();
  assert(channels.find(fd) != channels.end());
  assert(channels[fd] == channel);
  assert(channel->isNoneEvent());
  int index = channel->getIndex();
  assert(index == kAdded || index == kDeleted);
  size_t n = channels.erase(fd);
  (void)n;
  assert(n == 1);

  if (index == kAdded)
  {
    update(EPOLL_CTL_DEL, channel);
  }
  channel->setIndex(kNew);
}

void xEpoll::update(int operation, xChannel* channel)
{
  struct epoll_event event;
  bzero(&event, sizeof event);
  event.events = channel->getEvents();
  event.data.ptr = channel;
  int fd = channel->getfd();
  if (::epoll_ctl(epollFd, operation, fd, &event) < 0)
  {
	  //TRACE("epoll_ctl op %d\n",fd );
  }
}




void xEpoll::fillActiveChannels(int numEvents, ChannelList* activeChannels) const
{
	 for (int i = 0; i < numEvents; ++i)
	  {
	    xChannel* channel = static_cast<xChannel*>(events[i].data.ptr);
	    int fd = channel->getfd();
	    ChannelMap::const_iterator it = channels.find(fd);
	    assert(it != channels.end());
	    assert(it->second == channel);
	    channel->setRevents(events[i].events);
	    activeChannels->push_back(channel);
	  }
}


























