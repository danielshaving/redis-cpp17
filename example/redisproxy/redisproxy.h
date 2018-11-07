#pragma once
#include "all.h"
#include "hiredis.h"
#include "tcpserver.h"
#include "proxysession.h"

class RedisProxy
{
public:
	RedisProxy(const char *ip, int16_t port, const char *redisIp,
		int16_t redisPort, int16_t threadCount, int16_t sessionCount);
	~RedisProxy();

	void proxyConnCallback(const TcpConnectionPtr &conn);
	void processCommand(const RedisObjectPtr &command, const TcpConnectionPtr &conn, const char *buf, size_t len);
	void run();

	void initRedisPorxy();
	void initRedisAsync();
	void initRedisCommand();
	void initRedisTimer();
	void redisContextTimer(const std::thread::id &threadId);
	RedisAsyncContextPtr checkReply(const RedisObjectPtr &command, const TcpConnectionPtr &conn);
	RedisAsyncContextPtr checkReply(const TcpConnectionPtr &conn);

	bool getRedisCommand(const RedisObjectPtr &command);
	bool handleRedisCommand(const RedisObjectPtr &command,
		const ProxySessionPtr &session,
		const std::vector<RedisObjectPtr> &commands,
		const TcpConnectionPtr &conn);
	bool selectCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);
	bool dbsizeCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);
	bool flushdbCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);
	bool delCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);
	bool mgetCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);
	bool subScribeCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);
	bool unSubscribeCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);
	bool debugCommand(const RedisObjectPtr &command, const std::vector<RedisObjectPtr> &commands,
		const ProxySessionPtr &session, const TcpConnectionPtr &conn);

	void highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent);
	void writeCompleteCallback(const TcpConnectionPtr &conn);
	void redisConnCallback(const TcpConnectionPtr &conn);
	void redisDisconnCallback(const TcpConnectionPtr &conn);
	void proxyCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata);
	void flushdbCallback(const std::thread::id &threadId, const int32_t sockfd,
		const TcpConnectionPtr &conn);
	void dbsizeCallback(const std::thread::id &threadId, const int32_t sockfd,
		const TcpConnectionPtr &conn);
	void delCallback(const std::thread::id &threadId, const int32_t sockfd,
		const TcpConnectionPtr &conn);
	void mgetCallback(const std::thread::id &threadId, const int32_t sockfd,
		const TcpConnectionPtr &conn);
	void subScribeCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata);
	void unsubScribeCallback(const RedisAsyncContextPtr &c,
		const RedisReplyPtr &reply, const std::any &privdata);

	void clearSubscribe(const TcpConnectionPtr &conn);
	void clearProxy(const std::thread::id &threadId, const int32_t sockfd);
	void clearProxyReply(const std::thread::id &threadId, const int32_t sockfd);
	void clearProxyCount(const std::thread::id &threadId, const int32_t sockfd);
	void clearProxySend(const std::thread::id &threadId, const int32_t sockfd);
	void clearthreadProxyRedis(const std::thread::id &threadId, const int32_t sockfd);
	void clearthreadProxyRedisClient(const std::thread::id &threadId, const int32_t sockfd);
	void eraseProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count);
	void foreachProxyReply(const std::thread::id &threadId, const int32_t sockfd, int64_t begin);
	void insertProxyReply(const std::thread::id &threadId,
		const int32_t sockfd, const int64_t count, const RedisReplyPtr &reply);
	int64_t insertProxyCount(const std::thread::id &threadId, const int32_t sockfd);
	void insertProxySend(const std::thread::id &threadId, const int32_t sockfd, const int64_t count);
	void insertCommandReply(const std::thread::id &threadId, const int32_t sockfd, const RedisReplyPtr &reply);
	void clearCommandReply(const std::thread::id &threadId, const int32_t sockfd);
	int32_t getCommandReplyCount(const std::thread::id &threadId, const int32_t sockfd);
	void processCommandReply(const RedisObjectPtr &command,
		const std::thread::id &threadId, const int32_t sockfd, const TcpConnectionPtr &conn);

private:
	RedisProxy(const RedisProxy&);
	void operator=(const RedisProxy&);

	EventLoop loop;
	TcpServer server;
	std::mutex mutex;

	const char *ip;
	const char *redisIp;
	int16_t port;
	int16_t redisPort;
	int16_t threadCount;
	int16_t sessionCount;
	bool clusterEnabled;

	static const int32_t kHeart = 10;
	static const int32_t kHighWaterBytes = 1024 * 1024 * 64;

	std::unordered_map<int32_t, ProxySessionPtr> sessions;
	std::unordered_map<std::thread::id, std::shared_ptr<Hiredis>> threadHiredis;
	std::unordered_map<std::thread::id, std::unordered_map<int32_t, std::map<int64_t, RedisReplyPtr>>> threadProxyReplys;
	std::unordered_map<std::thread::id, std::unordered_map<int32_t, std::set<int64_t>>> threadProxySends;
	std::unordered_map<std::thread::id, std::unordered_map<int32_t, std::vector<RedisReplyPtr>>> threadCommandReplys;
	std::unordered_map<std::thread::id, std::unordered_map<int32_t, int64_t>> threadProxyCounts;
	std::unordered_map<std::thread::id, std::unordered_map<int32_t, std::unordered_set<int32_t>>> threadProxyRedis;
	std::unordered_map<std::thread::id, std::vector<RedisObjectPtr>> threadProxyCommands;
	std::unordered_map<std::thread::id, std::unordered_map<int32_t, std::vector<RedisObjectPtr>>> threadPubSubCommands;
	typedef std::function<bool(const RedisObjectPtr &, const std::vector<RedisObjectPtr> &,
		const ProxySessionPtr &, const TcpConnectionPtr &)> CommandFunc;
	std::unordered_map<RedisObjectPtr, CommandFunc, Hash, Equal> redisCommands;
	typedef std::function<void(const std::thread::id &,
			const int32_t, const TcpConnectionPtr &)> CommandReplyFuc;
	std::unordered_map<RedisObjectPtr, CommandReplyFuc, Hash, Equal> redisReplyCommands;
};
