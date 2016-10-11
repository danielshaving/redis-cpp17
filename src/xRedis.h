#ifndef _XREDIS_H_
#define _XREDIS_H_
#include "all.h"
#include "xEventLoop.h"
#include "xTcpconnection.h"
#include "xBuffer.h"
#include "xTcpServer.h"
#include "xSds.h"
#include "xSession.h"
#include "xObject.h"
#include "xRdb.h"

class xRedis : boost::noncopyable
{
private:
	xRdb rdb;
public:
	typedef std::function<bool (const std::vector<rObj*> &,xSession *)> commondFunction;
	std::unordered_map<std::string,commondFunction> handlerCommondMap;
	std::unordered_map<int32_t , std::shared_ptr<xSession>> sessions;;
	xEventLoop loop;
	xTcpServer server;
	mutable std::mutex mutex;
public:
	xRedis();
	~xRedis();
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

};

#endif

