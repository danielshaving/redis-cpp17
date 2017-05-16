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
#include "xReplication.h"


class xRedis : boost::noncopyable
{
public:
	xRedis(const char * ip,int32_t port,int32_t threadCount);
	~xRedis();
	void handleTimeout();
	void run();
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	bool deCodePacket(const xTcpconnectionPtr& conn,xBuffer *recvBuf,void  *data);

	void loadDataFromDisk();
	void flush();

	bool saveCommond(const std::vector<rObj*> & obj,xSession * session);
	bool pingCommond(const std::vector<rObj*> & obj,xSession * session);
	bool flushdbCommond(const std::vector<rObj*> & obj,xSession * session);
	bool dbsizeCommond(const std::vector<rObj*> & obj,xSession * session);
	bool quitCommond(const std::vector<rObj*> & obj,xSession * session);
	bool delCommond(const std::vector<rObj*> & obj,xSession * session);
	bool setCommond(const std::vector<rObj*> & obj,xSession * session);
	bool getCommond(const std::vector<rObj*> & obj,xSession * session);
	bool hsetCommond(const std::vector<rObj*> & obj,xSession * session);
	bool hgetCommond(const std::vector<rObj*> & obj,xSession * session);
	bool hgetallCommond(const std::vector<rObj*> & obj,xSession * session);
	bool slaveofCommond(const std::vector<rObj*> & obj,xSession * session);
	bool syncCommond(const std::vector<rObj*> & obj,xSession * session);
	bool psyncCommond(const std::vector<rObj*> & obj,xSession * session);
	bool commandCommond(const std::vector<rObj*> & obj,xSession * session);

public:
	std::map<std::string,int> vectorCommonds;
	typedef std::function<bool (const std::vector<rObj*> &,xSession *)> commondFunction;
	std::unordered_map<std::string,commondFunction> handlerCommondMap;
	std::unordered_map<int32_t , std::shared_ptr<xSession>> sessions;
	typedef std::unordered_map<rObj*,rObj*,Hash,Equal> SetMap;
      typedef std::unordered_map<rObj*,std::unordered_map<rObj*,rObj*,Hash,Equal> ,Hash,Equal> HsetMap;
	  

	struct SetMapLock
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
	std::array<SetMapLock, kShards> setShards;
	std::array<HsetLock, kShards> hsetShards;
	
	xEventLoop loop;
	xTcpServer server;
	mutable MutexLock mutex;

	std::string host;
	int32_t port;
	int32_t threadCount;
	std::string masterHost;
	int32_t masterPort;
	bool clusterEnabled;
	bool slaveEnabled;
	
	std::vector<std::shared_ptr<xReplication>> vectors;
	std::map<int32_t,xTcpconnectionPtr> tcpconnMaps;
	

};


