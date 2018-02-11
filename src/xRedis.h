#pragma once
#include "all.h"
#include "xEventLoop.h"
#include "xTcpconnection.h"
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
	xRedis(const char * ip, int16_t port,int16_t threadCount,bool enbaledCluster = false);
	~xRedis();
	
	void initConfig();
	void handleTimeOut(const std::any & context);
	void serverCron(const std::any & context);
	void handleSalveRepliTimeOut(const std::any & context);
	void handleSetExpire(const std::any & context);
   	void handleForkTimeOut();

	void run();
	void connCallBack(const xTcpconnectionPtr& conn);
	bool deCodePacket(const xTcpconnectionPtr& conn,xBuffer *recvBuf);
	void replyCheck();
	void loadDataFromDisk();
	void flush();

	bool saveCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool pingCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool debugCommand(const std::deque <rObj*> & obj, const xSeesionPtr &session);
	bool flushdbCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool dbsizeCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool quitCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool delCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);

	bool setCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool getCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);

	bool hlenCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool hsetCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool hgetCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool hgetallCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	
	bool zaddCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool zrangeCommand(const std::deque<rObj*> &obj,const xSeesionPtr &session);
	bool zcardCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool zrevrangeCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool zrangeGenericCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session,int reverse);

	bool lpushCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
	bool lpopCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
	bool lrangeCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
	bool rpushCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
	bool rpopCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
	bool llenCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);

	bool slaveofCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool syncCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool psyncCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool commandCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool clusterCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool authCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool configCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool infoCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool clientCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool echoCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool keysCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool bgsaveCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool memoryCommand(const std::deque <rObj*> & obj,const xSeesionPtr &session);
	bool sentinelCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
	bool migrateCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
	bool ttlCommand(const std::deque<rObj*> & obj, const xSeesionPtr &session);
		
	int32_t rdbSaveBackground(const xSeesionPtr &session, bool enabled);
	bool bgsave(const xSeesionPtr &session, bool enabled = false);
	bool save(const xSeesionPtr &session);
	bool removeCommand(rObj * obj);
	bool clearClusterMigradeCommand(void * data);
	void clearFork();
	void clearCommand();
	void clear();
	void clearRepliState(int32_t sockfd);
	void clearClusterState(int32_t sockfd);
	void clearDeques(std::deque<rObj*> & robj);
	size_t getDbsize();
	void structureRedisProtocol(xBuffer &  sendBuf, std::deque<rObj*> &robjs);
	bool getClusterMap(rObj * command);
	auto  & getHandlerCommandMap() { return handlerCommandMaps; }

public:
	typedef std::function<bool(const std::deque<rObj*> &,xSeesionPtr)> CommandFunc;	
	typedef std::unordered_map<rObj*,rObj*,Hash,Equal> StringMap;
	typedef std::unordered_map<rObj*,std::unordered_map<rObj*,rObj*,Hash,Equal>,Hash,Equal> HashMap;
	typedef std::unordered_map<rObj*,std::deque<rObj*>, Hash, Equal> ListMap;
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

	std::unordered_set<rObj*,Hash,Equal> unorderedmapCommands;
	std::unordered_set<rObj*,Hash,Equal> stopRepliMaps;
	std::unordered_map<rObj*,CommandFunc,Hash,Equal> handlerCommandMaps;
	std::unordered_set<rObj*,Hash, Equal> replyCommandMaps;
	std::unordered_map<int32_t,xSeesionPtr> sessionMaps;
	std::unordered_set<rObj*,Hash,Equal> cluterMaps;
	

	const static int32_t kShards = 4096;
	struct RedisMapLock
	{
		RedisMapLock()
		{
			redis.reserve(kShards );
			stringMap.reserve(kShards);
			hashMap.reserve(kShards);
			listMap.reserve(kShards);
			zsetMap.reserve(kShards);
			setMap.reserve(kShards);
		}
		
		RedisMap redis;
		StringMap stringMap;
		HashMap hashMap;
		ListMap listMap;
		ZsetMap zsetMap;
		SetMap setMap;
		mutable std::mutex mtx;
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
	std::atomic<int32_t> slavefd;
	std::atomic<int32_t> salveCount;

	std::condition_variable  expireCondition;
	std::condition_variable  forkCondition;

	xBuffer	 slaveCached;
	xBuffer	 clusterMigratCached;
	xBuffer	 clusterImportCached;
	
	xObjects object;
	xReplication repli;
	xSentinel senti;
	xCluster clus;
	xRdb rdb;
	xSocket socket;
	
	std::unique_ptr<std::thread> repliThreads;
	std::unique_ptr<std::thread> sentiThreads;
	std::unique_ptr<std::thread> clusterThreads;

	std::unordered_map<int32_t,xTcpconnectionPtr>     salvetcpconnMaps;
	std::unordered_map<int32_t,xTcpconnectionPtr>     clustertcpconnMaps;
	std::unordered_map<int32_t,xTimer*> repliTimers;
	std::unordered_map<rObj*,xTimer*,Hash,Equal>     expireTimers;
	
	std::string ip;
	std::string password;
 	std::string masterHost;
	std::string ipPort;
	std::string master;
	std::string slave;

	int16_t port;
	int16_t threadCount;
	int32_t masterPort;
	int32_t masterfd;
};


