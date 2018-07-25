//
// Created by zhanghao on 2018/6/17.
//

#pragma once
#include "all.h"
#include "tcpconnection.h"
#include "object.h"
#include "sds.h"
#include "util.h"

class Redis;
class Session : public std::enable_shared_from_this<Session>
{
public:
	Session(Redis *redis,const TcpConnectionPtr &conn);
	~Session();

	void resetVlaue();
	void clearObj();
	void reset();
	void readCallBack(const TcpConnectionPtr &conn,Buffer *buffer);
	int32_t processMultibulkBuffer(Buffer *buffer);
	int32_t processInlineBuffer(Buffer *buffer);
	int32_t processCommand();
	void onMessage(const TcpConnectionPtr &conn,Buffer *buffer);
	Buffer &getClientBuffer() { return clientBuffer; }
	TcpConnectionPtr getClientConn() { return clientConn; }
	void setAuth(bool enbaled) { authEnabled = enbaled; }

private:
	Session(const Session&);
	void operator=(const Session&);

	Redis *redis;
	RedisObject *cmd;
	std::deque<RedisObject*> commands;

	int32_t reqtype;
	int32_t multibulklen;
	int64_t bulklen;
	int32_t argc;
	Buffer clientBuffer;
	Buffer slaveBuffer;
	Buffer pubsubBuffer;
	TcpConnectionPtr clientConn;

	bool authEnabled;
	bool replyBuffer;
	bool fromMaster;
	bool fromSlave;
};

