#pragma once
#include "xAll.h"
#include "xEventLoop.h"
#include "xTcpConnection.h"
#include "xBuffer.h"
#include "xTcpServer.h"
#include "xSds.h"
#include "xSession.h"
#include "xObject.h"
#include "xRdb.h"
#include "xLog.h"
#include "xSocket.h"
#include "xReplication.h"
#include "xSentinel.h"
#include "xCluster.h"
#include "xRdb.h"
#include "xUtil.h"

class xRedis : noncopyable
{
public:
	xRedis(const char *ip,int16_t port,int16_t threadCount,bool enbaledCluster = false);
	~xRedis();
	
	void initConfig();
	void timeOut();
	void serverCron();
	void bgsaveCron();
	void slaveRepliTimeOut(int32_t context);
	void setExpireTimeOut(rObj *context);
   	void forkWait();

	void run();
	void connCallBack(const TcpConnectionPtr &conn);
	void replyCheck();
	void loadDataFromDisk();
	void flush();

	bool saveCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool pingCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool debugCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool flushdbCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool dbsizeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool quitCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool delCommand(const std::deque<rObj*> &obj,const SessionPtr &session);

	bool setCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool getCommand(const std::deque<rObj*> &obj,const SessionPtr &session);

	bool hkeysCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool hlenCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool hsetCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool hgetCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool hgetallCommand(const std::deque<rObj*> &obj,const SessionPtr &session);

	bool zaddCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool zrangeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool zcardCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool zrevrangeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool zrangeGenericCommand(const std::deque<rObj*> &obj,const SessionPtr &session,int reverse);

	bool lpushCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool lpopCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool lrangeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool rpushCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool rpopCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool llenCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	
	bool scardCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool saddCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	
	bool subscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool unsubscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool psubscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool punsubscribeCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool publishCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool pubsubCommand(const std::deque<rObj*> &obj,const SessionPtr &session);

	bool existsCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool dumpCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool restoreCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool slaveofCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool syncCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool psyncCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool commandCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool clusterCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool authCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool configCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool infoCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool clientCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool echoCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool keysCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool bgsaveCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool memoryCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool sentinelCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool migrateCommand(const std::deque<rObj*> &obj,const SessionPtr &session);
	bool ttlCommand(const std::deque<rObj*> &obj,const SessionPtr &session);

	int32_t rdbSaveBackground(bool enabled = false);
	bool bgsave(const SessionPtr &session,bool enabled = false);
	bool save(const SessionPtr &session);
	bool removeCommand(rObj *obj);
	bool clearClusterMigradeCommand();
	void clearFork();
	void clearCommand();
	void clear();
	void clearRepliState(int32_t sockfd);
	void clearClusterState(int32_t sockfd);
	void clearPubSubState(int32_t sockfd);
	void clearCommand(std::deque<rObj*> &commands);
	size_t getDbsize();
	size_t getExpireSize();
	int64_t getExpire(rObj *obj);
	void structureRedisProtocol(xBuffer &buffer,std::deque<rObj*> &robjs);
	bool getClusterMap(rObj *command);
	auto &getHandlerCommandMap() { return handlerCommands; }
	rObj *createDumpPayload(rObj *dump);
	void setExpire(rObj *key,double when);

public:
	auto *getEventLoop() { return &loop; }
	auto *getObject() { return &object; }
	auto *getRdb() { return &rdb; }
	auto *getCluster() { return &clus; }
	auto *getReplication() { return &repli; }

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

	std::string getIp() { return ip; }
	int16_t getPort() { return port; }

public:
	const static int32_t kShards = 4096;

	typedef std::function<bool(const std::deque<rObj*>&,SessionPtr)> CommandFunc;
	typedef std::unordered_map<rObj*,rObj*,Hash,Equal> StringMap;
	typedef std::unordered_map<rObj*,std::unordered_map<rObj*,rObj*,Hash,Equal>,Hash,Equal> HashMap;
	typedef std::unordered_map<rObj*,std::deque<rObj*>,Hash, Equal> ListMap;
	typedef std::unordered_map<rObj*,double,Hash,Equal> KeyMap;
	typedef std::multimap<double,rObj*> SortMap;
	typedef struct SortSet
	{
		KeyMap keyMap; 
		SortMap sortMap;
	};
	typedef std::unordered_map<rObj*,SortSet,Hash,Equal> ZsetMap;
	typedef std::unordered_map<rObj*,std::unordered_set<rObj*,Hash,Equal>,Hash,Equal> SetMap;
	typedef std::unordered_set<rObj*,Hash,Equal> RedisMap;

	std::unordered_set<rObj*,Hash,Equal> commands;
	std::unordered_set<rObj*,Hash,Equal> stopReplis;
	std::unordered_set<rObj*,Hash,Equal> replyCommands;
	std::unordered_set<rObj*,Hash,Equal> cluterCommands;
	std::unordered_map<rObj*,CommandFunc,Hash,Equal> handlerCommands;

private:
	std::unordered_map<int32_t,SessionPtr> sessions;
	std::unordered_map<int32_t,TcpConnectionPtr> slaveConns;
	std::unordered_map<int32_t,TcpConnectionPtr> clusterConns;
	std::unordered_map<int32_t,xTimer*> repliTimers;
	std::unordered_map<rObj*,xTimer*,Hash,Equal> expireTimers;
	std::unordered_map<rObj*,std::unordered_map<int32_t,TcpConnectionPtr>,Hash,Equal> pubsubs;

	struct RedisMapLock
	{		
		RedisMap redis;
		StringMap stringMap;
		HashMap hashMap;
		ListMap listMap;
		ZsetMap zsetMap;
		SetMap setMap;
		std::mutex mtx;
	};

	std::array<RedisMapLock,kShards> redisShards;

	xEventLoop loop;
	xTcpServer server;
	
	std::mutex mtx;
	std::mutex slaveMutex;
	std::mutex expireMutex;
	std::mutex sentinelMutex;
	std::mutex clusterMutex;
	std::mutex forkMutex;
	std::mutex pubsubMutex;

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
	std::atomic<int32_t> forkCondWaitCount;
	std::atomic<int32_t> rdbChildPid;
	std::atomic<int32_t> salveCount;

	int32_t slavefd;
	int32_t masterfd;

	std::condition_variable expireCondition;
	std::condition_variable forkCondition;

	xBuffer slaveCached;
	xBuffer clusterMigratCached;
	xBuffer clusterImportCached;

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

private:
	xObjects object;
	xReplication repli;
	xSentinel senti;
	xCluster clus;
	xRdb rdb;
	xSocket socket;


};


