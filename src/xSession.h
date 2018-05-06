#pragma once
#include "xAll.h"
#include "xTcpConnection.h"
#include "xObject.h"
#include "xSds.h"
#include "xUtil.h"

class xSentinel;
class xRedis;

class xSession : public std::enable_shared_from_this<xSession>
{
public:
	xSession(xRedis *redis,const TcpConnectionPtr &conn);
	~xSession();

	void resetVlaue();
	void clearObj();
	void reset();
	void readCallBack(const TcpConnectionPtr &conn,xBuffer *buffer);
	int32_t processMultibulkBuffer(xBuffer *buffer);
	int32_t processInlineBuffer(xBuffer *buffer);
	int32_t processCommand();
	bool checkCommand(rObj *robjs);
	void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	xBuffer &getClientBuffer() { return clientBuffer; }
	TcpConnectionPtr getClientConn() { return clientConn; }
	void setAuth(bool enbaled) { authEnabled = enbaled; }

private:
	xSession(const xSession&);
	void operator=(const xSession&);

	xRedis *redis;
	rObj *command;
	std::deque<rObj*> commands;

	int32_t reqtype;
	int32_t multibulklen;
	int64_t bulklen;
	int32_t argc;
	xBuffer clientBuffer;
	xBuffer slaveBuffer;
	xBuffer pubsubBuffer;
	TcpConnectionPtr clientConn;

	bool authEnabled;
	bool replyBuffer;
	bool fromMaster;
	bool fromSlave;
};

