#pragma once
#include "all.h"
#include "eventloop.h"
#include "channel.h"
#include "socket.h"
#include "log.h"

class Connector : public std::enable_shared_from_this<Connector>
{
public:
	typedef std::function<void(int32_t)> NewConnectionCallback;
	typedef std::function<void()> ErrorConnectionCallback;

	Connector(EventLoop *loop,const char *ip,int16_t port);
	~Connector();

	void setNewConnectionCallback(const NewConnectionCallback &&cb) 
	{ newConnectionCallback = std::move(cb); }

	void setConnectionErrorCallBack(const ErrorConnectionCallback &&cb) 
	{ errorConnectionCallback = std::move(cb); }

	void asyncStart();
	bool syncStart();
	void restart();
	void stop();

	bool syncStartInLoop();
	void asyncStartInLoop();

private:
	Connector(const Connector&);
	void operator=(const Connector&);

	void startInLoop();
	void stopInLoop();

	void asyncConnect();
	bool syncConnect();
	void connecting(int32_t sockfd);
	void resetChannel();
	void retry(int32_t sockfd);

	void handleWrite();
	void handleError();

	int32_t removeAndResetChannel();

  	enum States { kDisconnected,kConnecting,kConnected };
	void setState(States s) { state = s; }
	static const int kMaxRetryDelayMs = 30 * 1000;
	static const int kInitRetryDelayMs = 1000;

	EventLoop *loop;
	const char *ip;
	int16_t port;
	bool connect;
	int32_t retryDelayMs;
	States state;
	std::unique_ptr<Channel> channel;

	ErrorConnectionCallback	errorConnectionCallback;
	NewConnectionCallback newConnectionCallback;
	Socket socket;

};
