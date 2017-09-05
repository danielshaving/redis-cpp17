#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xSds.h"


class xSentinel;
class xRedis;
class xSession: noncopyable, public std::enable_shared_from_this<xSession>
{
public:
	xSession(xRedis *redis,const xTcpconnectionPtr & conn);
	~xSession();
public:
	 void resetVlaue();
	 void clearObj();
	 void reset();
	 void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	 int processMultibulkBuffer(xBuffer *recvBuf);
	 int processInlineBuffer(xBuffer *recvBuf);
	 int processCommand();
	 bool checkCommand(rObj*  robjs);

public:
	int32_t 			reqtype;
	int32_t 			multibulklen;
	int64_t 			bulklen;
	int32_t 		        argc;
	xBuffer 			sendBuf;
	xBuffer 			sendSlaveBuf;
	xBuffer 			sendPubSub;
	xTcpconnectionPtr 	conn;
	std::vector<xTcpconnectionPtr> pubSubTcpconn;
	std::deque<rObj*>  robjs;
	std::string command;
	xRedis *redis;
	bool authEnabled;
	bool retrieveBuffer;
	bool fromMaster;
};

