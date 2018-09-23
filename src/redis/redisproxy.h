#pragma once
#include "all.h"
#include "hiredis.h"
#include "tcpserver.h"
#include "proxysession.h"

class RedisProxy
{
public:
	RedisProxy(const char *ip, int16_t port,
			int16_t redisPort, int16_t threadCount, int16_t sessionCount);
	~RedisProxy();
	
	void proxyConnCallback(const TcpConnectionPtr &conn);
	void processCommand(const TcpConnectionPtr &conn, const std::string_view &view);
	void reset();
	void run();
	
	void redisConnCallback(const TcpConnectionPtr &conn);
	void redisDisconnCallback(const TcpConnectionPtr &conn);
	
	void proxyCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply,const std::any &privdata);
	
private:
	EventLoop loop;
	TcpServer server;
	Hiredis hiredis;
	std::mutex mutex;
	std::map<int32_t, ProxySessionPtr> sessions;
	sds unlockScript;
	std::string reply;
	
};
