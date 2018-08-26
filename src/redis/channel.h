#pragma once
#include "all.h"
class EventLoop;
class Channel
{
public:
	typedef std::function<void()> EventCallback;
	Channel(EventLoop *loop, int32_t fd);
	~Channel();

	void handleEvent();
	void setTie(const std::shared_ptr<void> &);
	void setRevents(int32_t revt) { revents = revt; }
	void setEvents(int32_t revt) { events = revt; }
	void setIndex(int32_t idx) { index = idx; }

	void setReadCallback(const EventCallback &&cb)
	{
		readCallback = std::move(cb);
	}

	void setWriteCallback(const EventCallback &&cb)
	{
		writeCallback = std::move(cb);
	}

	void setCloseCallback(const EventCallback &&cb)
	{
		closeCallback = std::move(cb);
	}

	void setErrorCallback(const EventCallback &&cb)
	{
		errorCallback = std::move(cb);
	}

	bool readEnabled() { return events & kReadEvent; }
	bool writeEnabled() { return events & kWriteEvent; }
	bool isNoneEvent() const { return events == kNoneEvent; }

	void enableReading() { events |= kReadEvent; update(); }
	void disableReading() { events &= ~kReadEvent; update(); }

	void enableWriting() { events |= kWriteEvent; update(); }
	void disableWriting() { events &= ~kWriteEvent; update(); }
	void disableAll() { events = kNoneEvent; update(); }

	bool isWriting() const { return events & kWriteEvent; }
	bool isReading() const { return events & kReadEvent; }

	int32_t getEvents() { return events; }
	int32_t getfd() { return fd; }
	void remove();
	int32_t getIndex() { return index; }
	EventLoop *ownerLoop() { return loop; }

private:
	Channel(const Channel&);
	void operator=(const Channel&);

	void update();
	void handleEventWithGuard();
	EventCallback readCallback;
	EventCallback writeCallback;
	EventCallback closeCallback;
	EventCallback errorCallback;

	static const int kNoneEvent;
	static const int kReadEvent;
	static const int kWriteEvent;

	EventLoop *loop;
	int32_t fd;
	int32_t events;
	int32_t revents;
	int32_t index;
	bool tied;
	bool eventHandling;
	bool addedToLoop;
	bool logHup;
	std::weak_ptr<void> tie;

};
