#pragma once
#include "all.h"

class xEventLoop;
class xChannel : noncopyable
{
public:
	typedef std::function<void()> EventCallback;
	xChannel(xEventLoop *loop,int fd);
	~xChannel();

	void handleEvent();
	void setTie(const std::shared_ptr<void>&);
	void setRevents(int revt) { revents = revt; }
	void setEvents(int revt) { events = revt; }
	void setIndex(int idx) { index = idx; }
	void setReadCallback(EventCallback&& cb) { readCallback  = std::move(cb); }
	void setWriteCallback(EventCallback&& cb){ writeCallback = std::move(cb); }
	void setCloseCallback(EventCallback&& cb){ closeCallback = std::move(cb); }
	void setErrorCallback(EventCallback&& cb){ errorCallback = std::move(cb); }

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

	int  getEvents() { return events; }
	int  getfd(){ return fd;}
	void remove();
	int  getIndex(){ return index; }
	xEventLoop *ownerLoop() { return loop; }

private:
	void update();
	void handleEventWithGuard();
	EventCallback  readCallback;
	EventCallback  writeCallback;
	EventCallback  closeCallback;
	EventCallback  errorCallback;

	static const int kNoneEvent;
	static const int kReadEvent;
	static const int kWriteEvent;

	xEventLoop *loop;
	int fd;
	int events;
	int revents;
	int index;
	bool tied;
	bool eventHandling;
	bool addedToLoop;
	std::weak_ptr<void> tie;

};
