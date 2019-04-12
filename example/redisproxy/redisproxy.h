#pragma once

#include "all.h"
#include "hiredis.h"
#include "tcpserver.h"
#include "proxysession.h"

class RedisProxy {
public:
	RedisProxy(const char *ip, int16_t port, const char *redisIp,
		int16_t redisPort, int16_t threadCount, int16_t sessionCount,
		const char *logPath, bool cluster);

	~RedisProxy();

	void proxyConnCallback(const TcpConnectionPtr &conn);

	void processCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const TcpConnectionPtr &conn, const char *buf, size_t len);

	void run();

	void initRedisPorxy();

	void initRedisAsync();

	void initRedisCommand();

	RedisAsyncContextPtr checkCommandToCluster(const RedisObjectPtr &command, const TcpConnectionPtr &conn, bool pubsub = false);

	RedisAsyncContextPtr checkCommandToCluster(const TcpConnectionPtr &conn, bool pubsub = false);

	bool getRedisCommand(const RedisObjectPtr &command);

	bool handleRedisCommand(const RedisObjectPtr &command,
		const ProxySessionPtr &session,
		const std::vector <RedisObjectPtr> &commands,
		const TcpConnectionPtr &conn,
		const char *buf,
		const size_t len);

	bool subscribeCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool monitorCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool pingCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool selectCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool dbsizeCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool flushdbCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool asyncCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool clusterCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool msetCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool debugCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	bool keysCommand(const RedisObjectPtr &command, const std::vector <RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn, const char *buf,
		const size_t len);

	void highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent);

	void writeCompleteCallback(const TcpConnectionPtr &conn);

	void redisConnCallback(const TcpConnectionPtr &conn);

	void redisDisconnCallback(const TcpConnectionPtr &conn);

	void monitorConnCallback(const TcpConnectionPtr &conn);

	void monitorDisconnCallback(const TcpConnectionPtr &conn);

	void subscribeConnCallback(const TcpConnectionPtr &conn);

	void subscribeDisConnCallback(const TcpConnectionPtr &conn);

	void monitorCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata);

	void subscribeCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata);

	void proxyCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata);

	void delCallback(const std::thread::id &threadId, const int32_t sockfd,
		const TcpConnectionPtr &conn, int32_t commandCount);

	void mgetCallback(const std::thread::id &threadId, const int32_t sockfd,
		const TcpConnectionPtr &conn, int32_t commandCount);

	void msetCallback(const std::thread::id &threadId, const int32_t sockfd,
		const TcpConnectionPtr &conn, int32_t commandCount);

	void clearCurrentThread(const std::thread::id &threadId);

	void clearProxy(const std::thread::id &threadId, const int32_t sockfd);

	void clearProxyReply(const std::thread::id &threadId, const int32_t sockfd);

	void clearProxyCount(const std::thread::id &threadId, const int32_t sockfd);

	void clearProxySend(const std::thread::id &threadId, const int32_t sockfd);

	void eraseProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count);

	void insertProxyReply(const std::thread::id &threadId,
		const int32_t sockfd, const int64_t count, const RedisReplyPtr &reply);

	int64_t insertProxyCount(const std::thread::id &threadId, const int32_t sockfd);

	void insertProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count);

	void insertCommandReply(const std::thread::id &threadId, const int32_t sockfd, const RedisReplyPtr &reply);

	void clearCommandReply(const std::thread::id &threadId, const int32_t sockfd);

	int32_t getCommandReplyCount(const std::thread::id &threadId, const int32_t sockfd);

	void processCommandReply(const RedisObjectPtr &command, int32_t commandCount,
		const std::thread::id &threadId, const int32_t sockfd, const TcpConnectionPtr &conn);

	bool findRedisReplyCommand(const RedisObjectPtr &command);

	void asyncOutput(const char *msg, int32_t len);
private:
	RedisProxy(const RedisProxy &);

	void operator=(const RedisProxy &);

	EventLoop loop;
	TcpServer server;

	const char *ip;
	const char *redisIp;
	int16_t port;
	int16_t redisPort;
	int16_t threadCount;
	int16_t sessionCount;
	bool clusterEnabled;

	static const int32_t kHeart = 10;
	static const int32_t kHighWaterBytes = 1024 * 1024 * 64;
	static const int32_t kRollSize = 5000 * 100;

	std::shared_ptr <AsyncLogging> asyncLog;
	std::unordered_map <std::thread::id, std::unordered_map <int32_t, TcpConnectionPtr>> threadMonitors;
	std::unordered_map <std::thread::id, std::unordered_map <int32_t, ProxySessionPtr>> threadSessions;
	std::unordered_map <std::thread::id, std::shared_ptr<Hiredis>> threadMonitorHierdis;
	std::unordered_map <std::thread::id, std::unordered_map <int32_t, std::shared_ptr<Hiredis>>> threadSubscribeHiredis;
	std::unordered_map <std::thread::id, std::shared_ptr<Hiredis>> threadHiredis;
	std::unordered_map <std::thread::id, std::unordered_map <int32_t, std::map <int64_t, RedisReplyPtr>>> threadProxyReplys;
	std::unordered_map <std::thread::id, std::unordered_map <int32_t, std::set <int64_t>>> threadProxySends;
	std::unordered_map <std::thread::id, std::unordered_map <int32_t, std::vector <RedisReplyPtr>>> threadCommandReplys;
	std::unordered_map <std::thread::id, std::unordered_map <int32_t, int64_t>> threadProxyCounts;
	std::unordered_map <std::thread::id, std::vector <RedisObjectPtr>> threadProxyCommands;
	typedef std::function<bool(const RedisObjectPtr &, const std::vector <RedisObjectPtr> &,
		const ProxySessionPtr &, const TcpConnectionPtr &, const char *,
		const size_t)> CommandFunc;
	std::unordered_map <RedisObjectPtr, CommandFunc, Hash, Equal> redisCommands;
	typedef std::function<void(const std::thread::id &,
		const int32_t, const TcpConnectionPtr &, int32_t commandCount)> CommandReplyFuc;
	std::unordered_map <RedisObjectPtr, CommandReplyFuc, Hash, Equal> redisReplyCommands;
};
