#pragma once

#include "all.h"
#include "eventloop.h"
#include "channel.h"
#include "socket.h"
#include "log.h"

class Connector : public std::enable_shared_from_this<Connector> {
public:
	typedef std::function<void(int32_t)> NewConnectionCallback;
	typedef std::function<void()> ErrorConnectionCallback;

	Connector(EventLoop *loop, const char *ip, int16_t port, bool retry);

	~Connector();

	void setNewConnectionCallback(const NewConnectionCallback &&cb) {
		newConnectionCallback = std::move(cb);
	}

	void setConnectionErrorCallBack(const ErrorConnectionCallback &&cb) {
		errorConnectionCallback = std::move(cb);
	}

	void start(bool s);

	void restart();

	void stop();

	void enableRetry() { r = true; }

	void closeRetry() { r = false; }

private:
	Connector(const Connector &);

	void operator=(const Connector &);

	void startInLoop(bool s);

	void stopInLoop();

	void connecting(bool s);

	void connecting(bool s, int32_t sockfd);

	void resetChannel();

	void retry(int32_t sockfd);

	void handleWrite();

	void handleError();

	int32_t removeAndResetChannel();

	enum States {
		kDisconnected, kConnecting, kConnected
	};

	void setState(States s) { state = s; }

	static const int kMaxRetryDelayMs = 30 * 1000;
	static const int kInitRetryDelayMs = 1000;
	static const int kHeart = 5;

	EventLoop *loop;
	std::string ip;
	int16_t port;
	bool connect;
	bool r;
	int32_t retryDelayMs;
	States state;
	std::unique_ptr <Channel> channel;

	ErrorConnectionCallback errorConnectionCallback;
	NewConnectionCallback newConnectionCallback;
};
