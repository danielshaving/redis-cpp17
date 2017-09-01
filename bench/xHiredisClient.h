#pragma once
#include "xEventLoop.h"
#include <hiredis/hiredis.h>
#include <hiredis/async.h>


struct redisAsyncContext;

class xHiredisClient :public std::enable_shared_from_this<xHiredisClient>,noncopyable
{
public:
	typedef std::function<void (xHiredisClient *,int)> ConnectCallback;
	typedef std::function<void (xHiredisClient *,int)>   DisconnectCallback;
	typedef std::function<void (xHiredisClient *,redisReply*)> CommandCallback;

	xHiredisClient(xEventLoop * loop,const std::string  &ip, uint16_t port);
	~xHiredisClient();
	
	bool connected() const;
	const char * errstr() const;

	void setConnectCallback(const ConnectCallback &cb) { connectCb = cb; }
	void setDisconnectCallback(const DisconnectCallback &cb ) { disconnectCb = cb; }

	void connect();
	void disconnect();

	int command(const CommandCallback& cb, stringArg  cmd, ...);
	int ping();
	 
	void handleRead();
	void handleWrite();


	int getFd()const;

	void setChannel();
	void removeChannel();
	
	void connectCallback(int status);
	void disconnectCallback(int status);
	void commandCallback(redisReply * reply,CommandCallback * cb);


	static xHiredisClient * getHiredis(const redisAsyncContext * ac);
	static void connectCallback(const redisAsyncContext * ac, int status);
	static void disconnectCallback(const redisAsyncContext * ac, int status);
	static void commandCallback(redisAsyncContext * ac, void *, void *);

	static void addRead(void * privdata);
	static void delRead(void * privdata);
	static void addWrite(void * privdata);
	static void delWrite(void * privdata);
	static void cleanup(void * privdata);	

	void pingCallback(xHiredisClient * msg, redisReply * reply);

private:
	xEventLoop *loop;
	std::string ip;
	uint16_t port;
	redisAsyncContext * context;
	std::shared_ptr<xChannel> channel;
	ConnectCallback connectCb;
	DisconnectCallback disconnectCb;
};

