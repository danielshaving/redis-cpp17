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

class xRedis : noncopyable
{
public:
	xRedis(const char * ip, int16_t port,int16_t threadCount,bool enbaledCluster = false);
	~xRedis();
	
	void test();
	void initConfig();
	void handleTimeOut(void *data);
	void serverCron(void * data);
	void handleSalveRepliTimeOut(void * data);
	void handleSetExpire(void * data);
   	void handleForkTimeOut();

	void run();
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	bool deCodePacket(const xTcpconnectionPtr& conn,xBuffer *recvBuf,void  *data);
	void replyCheck();
	void loadDataFromDisk();
	void flush();

	bool saveCommand(const std::deque <rObj*> & obj,xSession * session);
	bool pingCommand(const std::deque <rObj*> & obj,xSession * session);
	bool debugCommand(const std::deque <rObj*> & obj, xSession * session);
	bool flushdbCommand(const std::deque <rObj*> & obj,xSession * session);
	bool dbsizeCommand(const std::deque <rObj*> & obj,xSession * session);
	bool quitCommand(const std::deque <rObj*> & obj,xSession * session);
	bool delCommand(const std::deque <rObj*> & obj,xSession * session);
	bool setCommand(const std::deque <rObj*> & obj,xSession * session);
	bool getCommand(const std::deque <rObj*> & obj,xSession * session);
	bool hsetCommand(const std::deque <rObj*> & obj,xSession * session);

	bool hgetCommand(const std::deque <rObj*> & obj,xSession * session);
	bool hgetallCommand(const std::deque <rObj*> & obj,xSession * session);
	bool hlenCommand(const std::deque <rObj*> & obj,xSession * session);
	bool slaveofCommand(const std::deque <rObj*> & obj,xSession * session);
	bool syncCommand(const std::deque <rObj*> & obj,xSession * session);
	bool psyncCommand(const std::deque <rObj*> & obj,xSession * session);

	bool commandCommand(const std::deque <rObj*> & obj,xSession * session);
	bool clusterCommand(const std::deque <rObj*> & obj,xSession * session);
	bool authCommand(const std::deque <rObj*> & obj,xSession * session);
	bool configCommand(const std::deque <rObj*> & obj,xSession * session);
	bool infoCommand(const std::deque <rObj*> & obj,xSession * session);
	bool clientCommand(const std::deque <rObj*> & obj,xSession * session);
	bool echoCommand(const std::deque <rObj*> & obj,xSession * session);
	bool hkeysCommand(const std::deque <rObj*> & obj,xSession * session);
	bool keysCommand(const std::deque <rObj*> & obj,xSession * session);

	bool bgsaveCommand(const std::deque <rObj*> & obj,xSession * session);
	bool memoryCommand(const std::deque <rObj*> & obj,xSession * session);
	bool sentinelCommand(const std::deque<rObj*> & obj, xSession * session);
	bool migrateCommand(const std::deque<rObj*> & obj, xSession * session);
	bool ttlCommand(const std::deque<rObj*> & obj, xSession * session);
	bool lpushCommand(const std::deque<rObj*> & obj, xSession * session);
	bool lpopCommand(const std::deque<rObj*> & obj, xSession * session);
	bool lrangeCommand(const std::deque<rObj*> & obj, xSession * session);
	bool rpushCommand(const std::deque<rObj*> & obj, xSession * session);
	bool rpopCommand(const std::deque<rObj*> & obj, xSession * session);
	bool llenCommand(const std::deque<rObj*> & obj, xSession * session);
	bool scardCommand(const std::deque <rObj*> & obj,xSession * session);
	bool saddCommand(const std::deque <rObj*> & obj,xSession * session);

	int rdbSaveBackground(xSession * session, bool enabled);
	bool bgsave(xSession * session, bool enabled = false);
	bool save(xSession * session);
	int removeCommand(rObj * obj,int &count);

	bool clearClusterMigradeCommand(void * data);
	void forkClear();
	void clearCommand();
	void clear();
	void clearRepliState(int32_t sockfd);
	void clearClusterState(int32_t sockfd);
	void clearDeques(std::deque<rObj*> & robj);

	size_t getDbsize();
	void structureRedisProtocol(xBuffer &  sendBuf, std::deque<rObj*> &robjs);


public:
	std::unordered_set<rObj*,Hash,EEqual>  unorderedmapCommands;
	std::unordered_set<rObj*,Hash,EEqual>  stopRepliCached;
	typedef std::function<bool(const std::deque<rObj*> &,xSession *)> commandFunction;
	std::unordered_map<rObj*,commandFunction,Hash,EEqual> handlerCommandMap;
	std::unordered_set<rObj*,Hash, EEqual> replyCommandMap;
	std::unordered_map<int32_t,std::shared_ptr<xSession>> sessions;
	std::unordered_set<rObj*,Hash,EEqual> cluterMaps;
	
	typedef std::unordered_map<rObj*,rObj*,Hash,Equal> SetMap;
	typedef std::unordered_map<rObj*,std::unordered_map<rObj*,rObj*,Hash,Equal> ,Hash,Equal> HsetMap;
	typedef std::unordered_map<rObj*, std::deque<rObj*>, Hash, Equal> ListMap;
	typedef std::unordered_map<rObj*,std::unordered_set<rObj*,Hash,Equal>,Hash,Equal> Set;

	struct SetMapLock
	{		
		SetMap setMap;
		mutable std::mutex mtx;
	};

	struct HsetMapLock
	{
		HsetMap hsetMap;
		mutable std::mutex mtx;
	};

	struct ListMapLock
	{
		ListMap listMap;
		mutable std::mutex mtx;
	};

	struct SetLock
	{
		Set set;
		mutable std::mutex mtx;
	};


	const static int kShards = 4096;
	std::array<SetMapLock, kShards> setMapShards;
	std::array<HsetMapLock, kShards> hsetMapShards;
	std::array<ListMapLock, kShards> listMapShards;
	std::array<SetLock, kShards> setShards;

	xEventLoop loop;
	xTcpServer server;
    std::mutex mtx;
    std::mutex slaveMutex;
    std::mutex expireMutex;
    std::mutex sentinelMutex;
    std::mutex clusterMutex;
    std::mutex forkMutex;


	std::atomic<int>   salveCount;
	std::atomic<bool>  clusterEnabled;
	std::atomic<bool>  slaveEnabled;
	std::atomic<bool>  authEnabled;
	std::atomic<bool>  repliEnabled;
	std::atomic<bool>  sentinelEnabled;
	std::atomic<bool>  clusterSlotEnabled;
	std::atomic<bool>  clusterRepliMigratEnabled;
	std::atomic<bool>  clusterRepliImportEnabeld;
	std::atomic<int>   rdbChildPid;
	std::atomic<int>   slavefd;
	std::atomic<bool>  forkEnabled;
	std::atomic<int>   forkCondWaitCount;

    std::condition_variable  expireCondition;
    std::condition_variable  forkCondition;

	xBuffer	slaveCached;
	xBuffer	clusterMigratCached;
	xBuffer	clusterImportCached;
	
	xReplication   repli;
	xSentinel	   senti;
	xCluster	   clus;
	xRdb		   rdb;

	std::unique_ptr<std::thread > repliThreads;
	std::unique_ptr<std::thread > sentiThreads;
	std::unique_ptr<std::thread>  clusterThreads;

	std::unordered_map<int32_t,xTcpconnectionPtr>  salvetcpconnMaps;
	std::unordered_map<int32_t,xTcpconnectionPtr> clustertcpconnMaps;
	std::unordered_map<int32_t,xTimer*> repliTimers;
	std::unordered_map<rObj*,xTimer*,Hash,Equal> expireTimers;
	
	xSocket socket;
	std::string  host;
	std::string password;
	std::atomic<int64_t>  count;
	std::atomic<bool>	pingPong;
	int16_t port;
	int16_t threadCount;
	std::string masterHost;
	std::atomic<int>	masterPort;
	std::atomic<int>	masterfd;
	std::string ipPort;
	int curSelect;

	rObj * rIp;
	rObj * rPort;
};


