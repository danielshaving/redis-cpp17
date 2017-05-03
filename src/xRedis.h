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

class xRedis : boost::noncopyable
{
public:
	xRedis();
	~xRedis();
	void handleTimeout();
	void run();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	bool deCodePacket(const xTcpconnectionPtr& conn,xBuffer *recvBuf,void  *data);

	void loadDataFromDisk();
	void flush();
	void disconnect();
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

public:
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
	mutable MutexLock fmutex;

private:
	xRdb rdb;
	xTimerQueue timerQueue;
	

};


