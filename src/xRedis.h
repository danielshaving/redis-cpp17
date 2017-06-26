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
#include "xPosix.h"
#include "xLog.h"
#include "xSocket.h"
#include "xReplication.h"

class xRedis : noncopyable
{
public:
	xRedis() {}
	xRedis(const char * ip,int32_t port,int32_t threadCount,bool enbaledCluster);
	~xRedis();
	void handleTimeout();
	void run();
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	bool deCodePacket(const xTcpconnectionPtr& conn,xBuffer *recvBuf,void  *data);

	void loadDataFromDisk();
	void flush();

	bool saveCommond(const std::deque <rObj*> & obj,xSession * session);
	bool pingCommond(const std::deque <rObj*> & obj,xSession * session);
	bool flushdbCommond(const std::deque <rObj*> & obj,xSession * session);
	bool dbsizeCommond(const std::deque <rObj*> & obj,xSession * session);
	bool quitCommond(const std::deque <rObj*> & obj,xSession * session);
	bool delCommond(const std::deque <rObj*> & obj,xSession * session);
	bool setCommond(const std::deque <rObj*> & obj,xSession * session);
	bool getCommond(const std::deque <rObj*> & obj,xSession * session);
	bool hsetCommond(const std::deque <rObj*> & obj,xSession * session);
	bool hgetCommond(const std::deque <rObj*> & obj,xSession * session);
	bool hgetallCommond(const std::deque <rObj*> & obj,xSession * session);
	bool slaveofCommond(const std::deque <rObj*> & obj,xSession * session);
	bool syncCommond(const std::deque <rObj*> & obj,xSession * session);
	bool psyncCommond(const std::deque <rObj*> & obj,xSession * session);
	bool commandCommond(const std::deque <rObj*> & obj,xSession * session);
	bool clusterCommond(const std::deque <rObj*> & obj,xSession * session);

public:

	std::unordered_map<rObj*,int,Hash,EEqual>  unorderedmapCommonds;
	typedef std::function<bool (const std::deque<rObj*> &,xSession *)> commondFunction;
	std::unordered_map<rObj*,commondFunction,Hash,EEqual> handlerCommondMap;
	std::unordered_map<int32_t , std::shared_ptr<xSession>> sessions;
	typedef std::unordered_map<rObj*,rObj*,Hash,Equal> SetMap;
	typedef std::unordered_map<rObj*,std::unordered_map<rObj*,rObj*,Hash,Equal> ,Hash,Equal> HsetMap;


	struct SetLock
	{
		SetMap setMap;
		mutable MutexLock mutex;
	};

	struct HsetLock
	{
		HsetMap hsetMap;
		mutable MutexLock mutex;
	};

	const static int kShards = 4096;
	std::array<SetLock, kShards> setShards;
	std::array<HsetLock, kShards> hsetShards;
	xEventLoop loop;
	xTcpServer server;
	mutable MutexLock mutex;
	std::string host;
	int32_t port;
	int32_t threadCount;
	std::string masterHost;
	int32_t masterPort ;
	std::atomic<bool>  clusterEnabled;
	std::atomic<bool>  slaveEnabled;
	std::atomic<bool>  repliEnabled;
	std::atomic<xTimer*> timer;
	xBuffer		slaveCached;
	xReplication  repli;
	std::map<int32_t,xTcpconnectionPtr> tcpconnMaps;
	xSocket socket;
	int32_t count = 0;
};


