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
	Redis(const char *ip,int16_t port,int16_t threadCount,bool enbaledCluster = false);
	~Redis();

	void initConfig();
	void timeOut() { loop.quit(); }
	void serverCron();
	void bgsaveCron();
	void slaveRepliTimeOut(int32_t context);
	void setExpireTimeOut(const RedisObjectPtr &expire);
   	void forkWait();

	void run() { loop.run(); }
	void connCallBack(const TcpConnectionPtr &conn);
	void replyCheck();
	void loadDataFromDisk();
	void flush();

	bool saveCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool pingCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool debugCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool flushdbCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool dbsizeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool quitCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool delCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);

	bool setCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool getCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);

	bool hkeysCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool hlenCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool hsetCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool hgetCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool hgetallCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);

	bool zaddCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool zrangeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool zcardCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool zrevrangeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool zrangeGenericCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session,int reverse);

	bool lpushCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool lpopCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool lrangeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool rpushCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool rpopCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool llenCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	
	bool scardCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool saddCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	
	bool subscribeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool unsubscribeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool psubscribeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool punsubscribeCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool publishCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool pubsubCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);

	bool existsCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool dumpCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool restoreCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool slaveofCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool syncCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool psyncCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool commandCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool clusterCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool authCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool configCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool infoCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool clientCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool echoCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool keysCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool bgsaveCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool memoryCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool sentinelCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool migrateCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool ttlCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool incrCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool decrCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);
	bool incrDecrCommand(const RedisObjectPtr &obj,const SessionPtr &session,int64_t incr);
	bool monitorCommand(const std::deque<RedisObjectPtr> &obj,const SessionPtr &session);

public:
	int32_t rdbSaveBackground(bool enabled = false);
	bool bgsave(const SessionPtr &session,bool enabled = false);
	bool save(const SessionPtr &session);
	bool removeCommand(const RedisObjectPtr &obj);

	bool clearClusterMigradeCommand();
	void clearFork();
	void clearCommand();
	void clearRepliState(int32_t sockfd);
	void clearClusterState(int32_t sockfd);
	void clearPubSubState(int32_t sockfd);
	void clearMonitorState(int32_t sockfd);
	void clearCommand(std::deque<RedisObjectPtr> &commands);

	RedisObjectPtr createDumpPayload(const RedisObjectPtr &dump);
	void feedMonitor(const std::deque<RedisObjectPtr> &obj,int32_t sockfd);

	size_t getDbsize();
	size_t getExpireSize();
	int64_t getExpire(const RedisObjectPtr &obj);
	void structureRedisProtocol(Buffer &buffer,std::deque<RedisObjectPtr> &robjs);
	bool getClusterMap(const RedisObjectPtr &command);
	auto &getHandlerCommandMap() { return handlerCommands; }
	void setExpire(const RedisObjectPtr &key,double when);
	bool checkCommand(const RedisObjectPtr &cmd);

	EventLoop *getEventLoop() { return &loop; }
	Rdb *getRdb() { return &rdb; }
	Cluster *getCluster() { return &clus; }
	Replication *getReplication() { return &repli; }

	std::string &getIp() { return ip; }
	int16_t getPort() { return port; }

	auto &getRedisShards() { return redisShards; }
	auto &getSession() { return sessions; }
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
	typedef std::function<bool(const std::deque<RedisObjectPtr> &,const SessionPtr &)> CommandFunc;
	typedef std::unordered_map<RedisObjectPtr,RedisObjectPtr,Hash,Equal> StringMap;
	typedef std::unordered_map<RedisObjectPtr,std::unordered_map<RedisObjectPtr,
			RedisObjectPtr,Hash,Equal>,Hash,Equal> HashMap;
	typedef std::unordered_map<RedisObjectPtr,std::deque<RedisObjectPtr>,Hash,Equal> ListMap;
	typedef std::unordered_map<RedisObjectPtr,double,Hash,Equal> SortIndexMap;
	typedef std::multimap<double,RedisObjectPtr> SortMap;
	typedef std::unordered_map<RedisObjectPtr,std::pair<SortIndexMap,SortMap>,Hash,Equal> ZsetMap;
	typedef std::unordered_map<RedisObjectPtr,std::unordered_set<RedisObjectPtr,Hash,Equal>,Hash,Equal> SetMap;
	typedef std::unordered_set<RedisObjectPtr,Hash,Equal> RedisMap;

	std::unordered_set<RedisObjectPtr,Hash,Equal> checkCommands;
	std::unordered_set<RedisObjectPtr,Hash,Equal> stopReplis;
	std::unordered_set<RedisObjectPtr,Hash,Equal> replyCommands;
	std::unordered_set<RedisObjectPtr,Hash,Equal> cluterCommands;
	std::unordered_map<RedisObjectPtr,CommandFunc,Hash,Equal> handlerCommands;

	const static int32_t kShards = 1024;
	
private:
	Redis(const Redis&);
	void operator=(const Redis&);

	std::unordered_map<int32_t,SessionPtr> sessions;
	/* Tcp connection maintenance */
	std::unordered_map<int32_t,TcpConnectionPtr> slaveConns;
	/* Tcp master-> salve node tconnection maintenance */
	std::unordered_map<int32_t,TcpConnectionPtr> clusterConns;
	/* Tcp cluster node tcpconnection maintenance */
	std::unordered_map<int32_t,Timer*> repliTimers;
	/*Slaveof commonad -> replication timer maintenace  */
	std::unordered_map<RedisObjectPtr,Timer*,Hash,Equal> expireTimers;
	/* Setex expire timer maintenace*/
	std::unordered_map<RedisObjectPtr,
		std::unordered_map<int32_t,TcpConnectionPtr>,Hash,Equal> pubsubs;
	/* Pubsub commnand maintenace*/
	std::unordered_map<int32_t,TcpConnectionPtr> monitorConns;
    /* Whenever a message is received in a hash index lock granularity*/
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

   /*All datatype maintenace */
	std::array<RedisMapLock,kShards> redisShards;

	EventLoop loop; /*Current main loop  */
	TcpServer server; /* Maintain multiple event loops */

    /* Different locks for each module*/
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

	/* master slave replcation ，To make data consistency,
	 * use cross-thread calls to maintain conditional variable counts*/
	std::atomic<int32_t> forkCondWaitCount;
	/*bgsave fork  Conditional judgment */
	std::atomic<int32_t> rdbChildPid;
	/* salve */
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
	Socket socket;
};


