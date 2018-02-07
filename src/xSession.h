#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xSds.h"
#include "xUtil.h"

class xSentinel;
class xRedis;
class xSession : noncopyable, public std::enable_shared_from_this<xSession>
{
public:
	xSession(xRedis *redis,const xTcpconnectionPtr & conn);
	~xSession();

	 void resetVlaue();
	 void clearObj();
	 void reset();
	 void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf);
	 int32_t processMultibulkBuffer(xBuffer *recvBuf);
	 int32_t processInlineBuffer(xBuffer *recvBuf);
	 int32_t processCommand();
	 bool checkCommand(rObj*  robjs);

public:
	int32_t reqtype;
	int32_t multibulklen;
	int64_t bulklen;
	int32_t argc;
	xBuffer sendBuf;
	xBuffer sendSlaveBuf;
	xBuffer sendPubSub;
	xTcpconnectionPtr 	conn;
	std::vector<xTcpconnectionPtr> pubSubTcpconn;
	std::deque<rObj*>  robjs;
	xRedis *redis;
	rObj * command;
	bool authEnabled;
	bool replyBuffer;
	bool fromMaster;
	bool fromSlave;
};

