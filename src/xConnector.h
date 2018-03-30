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

	xConnector(xEventLoop *loop,const char *ip,int16_t port);
	~xConnector();

	void setNewConnectionCallback(const NewConnectionCallback &&cb) { newConnectionCallback = std::move(cb); }
	void setConnectionErrorCallBack(const ErrorConnectionCallback &&cb) { errorConnectionCallback = std::move(cb); }

	void asyncStart();
	void syncStart();

	void restart();
	void stop();

	void syncStartInLoop();
	void asyncStartInLoop();
	void stopInLoop();

	void asyncConnect();
	void syncConnect();

	void connecting(int32_t sockfd);
	void resetChannel();
	void retry(int32_t sockfd);

	void handleWrite();
	void handleError();

	int32_t  removeAndResetChannel();

private:
  	enum States { kDisconnected, kConnecting, kConnected };
	void setState(States  s) { state = s; }
	static const int kMaxRetryDelayMs = 30*1000;
	static const int kInitRetryDelayMs = 500;

	xEventLoop *loop;
	const char *ip;
	int16_t port;
	bool connect;
	int32_t retryDelayMs;
	States state;
	std::unique_ptr<xChannel> channel;

	ErrorConnectionCallback	errorConnectionCallback;
	NewConnectionCallback newConnectionCallback;
	xSocket socket;

};
