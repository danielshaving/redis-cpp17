#pragma once
#include "all.h"
#include "xEventLoop.h"
#include "xChannel.h"
#include "xSocket.h"
#include "xLog.h"

class xConnector : noncopyable, public std::enable_shared_from_this<xConnector>
{
public:
	typedef std::function<void (int32_t sockfd)> NewConnectionCallback;
	typedef std::function<void()> ErrorConnectionCallback;

	xConnector(xEventLoop *loop);
	~xConnector();

	void setNewConnectionCallback(const NewConnectionCallback &&cb) { newConnectionCallback = std::move(cb); }
	void setConnectionErrorCallBack(const ErrorConnectionCallback &&cb) { errorConnectionCallback = std::move(cb); }

	void asyncStart(const char *ip,int16_t port);
	void syncStart(const char *ip,int16_t port);
	void stop();

	void syncStartInLoop(const char *ip,int16_t port);
	void asyncStartInLoop(const char *ip,int16_t port);
	void stopInLoop();
	void asyncConnect(const char *ip,int16_t port);
	void syncConnect(const char *ip,int16_t port);
	void connecting(int32_t sockfd);
	void resetChannel();
	int  removeAndResetChannel();

private:
  	enum States { kDisconnected, kConnecting, kConnected };
private:
	void setState(States  s) { state = s; }
	xEventLoop* loop;
	std::string ip;
	int16_t port;

	States state;
	std::unique_ptr<xChannel> channel;
	bool isconnect;
	ErrorConnectionCallback	errorConnectionCallback;
	NewConnectionCallback newConnectionCallback;
	xSocket    socket;
};
