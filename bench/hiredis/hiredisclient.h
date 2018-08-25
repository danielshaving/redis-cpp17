#pragma once
#include "eventloop.h"
#include <hiredis/hiredis.h>
#include <hiredis/async.h>

struct redisAsyncContext;

class HiredisClient :public std::enable_shared_from_this<HiredisClient>
{
public:
	typedef std::function<void(HiredisClient *, int)> ConnectCallback;
	typedef std::function<void(HiredisClient *, int)> DisconnectCallback;
	typedef std::function<void(HiredisClient *, redisReply*)> CommandCallback;

	HiredisClient(EventLoop *loop, const std::string &ip, uint16_t port);
	~HiredisClient();

	bool connected() const;
	const char *errstr() const;

	void setConnectCallback(const ConnectCallback &cb) { connectCb = cb; }
	void setDisconnectCallback(const DisconnectCallback &cb) { disconnectCb = cb; }

	void connect();
	void disconnect();

	int command(const CommandCallback& cb, StringArg cmd, ...);
	int ping();

	void handleRead();
	void handleWrite();

	int getFd()const;

	void setChannel();
	void removeChannel();

	void connectCallback(int status);
	void disconnectCallback(int status);
	void commandCallback(redisReply *reply, CommandCallback *cb);

	static HiredisClient *getHiredis(const redisAsyncContext *ac);
	static void connectCallback(const redisAsyncContext *ac, int status);
	static void disconnectCallback(const redisAsyncContext *ac, int status);
	static void commandCallback(redisAsyncContext *ac, void *, void *);

	static void addRead(void *privdata);
	static void delRead(void *privdata);
	static void addWrite(void *privdata);
	static void delWrite(void *privdata);
	static void cleanup(void *privdata);

	void pingCallback(HiredisClient *msg, redisReply *reply);

private:
	EventLoop * loop;
	std::string ip;
	uint16_t port;
	redisAsyncContext *context;
	std::shared_ptr<Channel> channel;
	ConnectCallback connectCb;
	DisconnectCallback disconnectCb;
};

