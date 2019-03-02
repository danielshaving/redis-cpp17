#pragma once
#include "all.h"
#include "eventloop.h"
#include "tcpconnection.h"
#include "buffer.h"
#include "tcpserver.h"
#include "sds.h"
#include "session.h"
#include "object.h"
#include "rdb.h"
#include "log.h"
#include "socket.h"
#include "replication.h"
#include "cluster.h"
#include "util.h"

class Redis
{
public:
	Redis(const char *ip, int16_t port, int16_t threadCount, bool enbaledCluster = false);
	~Redis();

	void initConfig();
	void scriptingInit();
	void timeOut();
	void serverCron();
	void bgsaveCron();
	void slaveRepliTimeOut(int32_t context);
	void setExpireTimeOut(const RedisObjectPtr &expire);
	void forkWait();

	void run();
	void connCallBack(const TcpConnectionPtr &conn);
	void highWaterCallBack(const TcpConnectionPtr &conn, size_t bytesToSent);
	void writeCompleteCallBack(const TcpConnectionPtr &conn);
	
	void replyCheck();
	void loadDataFromDisk();
	void flush();
	
#ifdef _LUA
	void luaLoadLib(lua_State *lua, const char *libname, lua_CFunction luafunc);
	static int32_t luaRedisCallCommand(lua_State *lua);
	static int32_t luaRedisPCallCommand(lua_State *lua);
	int32_t luaRedisGenericCommand(lua_State *lua, int32_t raise);
	void luaLoadLibraries(lua_State *lua);
	void luaRemoveUnsupportedFunctions(lua_State *lua);
	void scriptingEnableGlobalsProtection(lua_State *lua);
	int32_t luaCreateFunction(Buffer *buffer, lua_State *lua,
		char *funcname, const RedisObjectPtr &body);
	void luaSetGlobalArray(lua_State *lua, char *var,
		const std::deque<RedisObjectPtr> &elev, int32_t start, int32_t elec);
	bool evalCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	void luaReplyToRedisReply(Buffer *buffer, lua_State *lua);
	void luaPushError(lua_State *lua, char *error);
#endif
	bool saveCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool pingCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool debugCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool flushdbCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool dbsizeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool quitCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool delCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool setCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool getCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool hkeysCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool hlenCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool hsetCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool hgetCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool hgetallCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool zaddCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool zrangeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool zcardCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool zrevrangeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool zrangeGenericCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn, int reverse);
	bool lpushCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool lpopCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool lrangeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool rpushCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool rpopCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool llenCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool scardCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool saddCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool subscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool unsubscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool psubscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool punsubscribeCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool publishCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool pubsubCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool existsCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool dumpCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool restoreCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool slaveofCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool syncCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool psyncCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool commandCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool clusterCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool authCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool configCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool infoCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool clientCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool echoCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool keysCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool bgsaveCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool memoryCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool sentinelCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool migrateCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool ttlCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool incrCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool decrCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
	bool incrDecrCommand(const RedisObjectPtr &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn, int64_t incr);
	bool monitorCommand(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const TcpConnectionPtr &conn);
public:
#ifndef _WIN64
	int32_t rdbSaveBackground(bool enabled = false);
	bool bgsave(const SessionPtr &session,
		const TcpConnectionPtr &conn, bool enabled = false);
#endif
	bool save(const SessionPtr &session, const TcpConnectionPtr &conn);
	bool removeCommand(const RedisObjectPtr &obj);

	bool clearClusterMigradeCommand();
	void clearFork();
	void clearCommand();
	void clearSessionState(int32_t sockfd);
	void clearRepliState(int32_t sockfd);
	void clearClusterState(int32_t sockfd);
	void clearPubSubState(int32_t sockfd);
	void clearMonitorState(int32_t sockfd);
	void clearCommand(std::deque<RedisObjectPtr> &commands);

	RedisObjectPtr createDumpPayload(const RedisObjectPtr &dump);
	void feedMonitor(const std::deque<RedisObjectPtr> &obj, int32_t sockfd);
	void structureRedisProtocol(Buffer &buffer, std::deque<RedisObjectPtr> &robjs);
	void setExpire(const RedisObjectPtr &key, double when);
	bool checkCommand(const RedisObjectPtr &cmd);

	EventLoop *getEventLoop() { return &loop; }
	Rdb *getRdb() { return &rdb; }
	Cluster *getCluster() { return &clus; }
	Replication *getReplication() { return &repli; }
	size_t getDbsize();
	size_t getExpireSize();
	int64_t getExpire(const RedisObjectPtr &obj);
	std::string &getIp() { return ip; }
	int16_t getPort() { return port; }
	bool getClusterMap(const RedisObjectPtr &command);
	auto &getHandlerCommandMap() { return handlerCommands; }

	auto &getRedisShards() { return redisShards; }
	auto &getSession() { return sessions; }
	auto &getSessionConn() { return sessionConns; }
	auto &getClusterConn() { return clusterConns; }
	auto &getRepliTimer() { return repliTimers; }
	auto &getSlaveConn() { return slaveConns; }
	auto &getExpireTimer() { return expireTimers; }

	auto &getClusterMutex() { return clusterMutex; }
	auto &getSlaveMutex() { return slaveMutex; }
	auto &getExpireMutex() { return expireMutex; }
	auto &getMutex() { return mtx; }
	auto &getForkMutex() { return forkMutex; }
	auto &pubSubMutex() { return pubsubMutex; }

public:
	const static int32_t kShards = 1024;
	typedef std::function<bool(const std::deque<RedisObjectPtr> &,
		const SessionPtr &, const TcpConnectionPtr &)> CommandFunc;
	typedef std::unordered_map<RedisObjectPtr,
		RedisObjectPtr, Hash, Equal> StringMap;
	typedef std::unordered_map<RedisObjectPtr, std::unordered_map<RedisObjectPtr,
		RedisObjectPtr, Hash, Equal>, Hash, Equal> HashMap;
	typedef std::unordered_map<RedisObjectPtr,
		std::deque<RedisObjectPtr>, Hash, Equal> ListMap;
	typedef std::unordered_map<RedisObjectPtr, double, Hash, Equal> SortIndexMap;
	typedef std::multimap<double, RedisObjectPtr> SortMap;
	typedef std::unordered_map<RedisObjectPtr,
		std::pair<SortIndexMap, SortMap>, Hash, Equal> ZsetMap;
	typedef std::unordered_map<RedisObjectPtr,
		std::unordered_set<RedisObjectPtr, Hash, Equal>, Hash, Equal> SetMap;
	typedef std::unordered_set<RedisObjectPtr, Hash, Equal> RedisMap;
	typedef std::unordered_set<RedisObjectPtr, Hash, Equal> Command;

private:
	Redis(const Redis&);
	void operator=(const Redis&);

	std::unordered_map<int32_t, SessionPtr> sessions;
	std::unordered_map<int32_t, TcpConnectionPtr> sessionConns;
	std::unordered_map<int32_t, TcpConnectionPtr> slaveConns;
	std::unordered_map<int32_t, TcpConnectionPtr> clusterConns;
	std::unordered_map<int32_t, TimerPtr> repliTimers;
	std::unordered_map<RedisObjectPtr, TimerPtr, Hash, Equal> expireTimers;
	std::unordered_map<RedisObjectPtr,
		std::unordered_map<int32_t, TcpConnectionPtr>, Hash, Equal> pubSubs;
	std::unordered_map<int32_t, TcpConnectionPtr> monitorConns;
	std::unordered_map<RedisObjectPtr, CommandFunc, Hash, Equal> handlerCommands;
	std::unordered_map<RedisObjectPtr, RedisObjectPtr, Hash, Equal> luaScipts;

	Command checkCommands;
	Command stopReplis;
	Command replyCommands;
	Command cluterCommands;

	struct RedisMapLock
	{
		RedisMap redisMap;
		StringMap stringMap;
		HashMap hashMap;
		ListMap listMap;
		ZsetMap zsetMap;
		SetMap setMap;
		std::mutex mtx;
	};

	std::array<RedisMapLock, kShards> redisShards;

	EventLoop loop;
	TcpServer server;

	std::mutex mtx;
	std::mutex slaveMutex;
	std::mutex expireMutex;
	std::mutex sentinelMutex;
	std::mutex clusterMutex;
	std::mutex forkMutex;
	std::mutex pubsubMutex;
	std::mutex monitorMutex;
public:
	std::atomic<bool> clusterEnabled;
	std::atomic<bool> slaveEnabled;
	std::atomic<bool> authEnabled;
	std::atomic<bool> repliEnabled;
	std::atomic<bool> sentinelEnabled;
	std::atomic<bool> clusterSlotEnabled;
	std::atomic<bool> clusterRepliMigratEnabled;
	std::atomic<bool> clusterRepliImportEnabeld;
	std::atomic<bool> forkEnabled;
	std::atomic<bool> monitorEnabled;

	std::atomic<int32_t> forkCondWaitCount;
	std::atomic<int32_t> rdbChildPid;
	std::atomic<int32_t> salveCount;

	std::condition_variable expireCondition;
	std::condition_variable forkCondition;

	Buffer slaveCached;
	Buffer clusterMigratCached;
	Buffer clusterImportCached;

	std::string ip;
	std::string password;
	std::string masterHost;
	std::string ipPort;
	std::string master;
	std::string slave;

#ifdef _LUA
	lua_State *lua;
#endif
	int16_t port;
	int16_t threadCount;
	int32_t masterPort;
	int32_t dbnum;
	int32_t slavefd;
	int32_t masterfd;
private:
	Replication repli;
	Cluster clus;
	Rdb rdb;
};


