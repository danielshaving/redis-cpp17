#pragma once
#include "all.h"
#include "tcpconnection.h"
#include "sds.h"
#include "timerqueue.h"
#include "leveldb/db.h"

class RedisDb;
class RedisSession : public std::enable_shared_from_this<RedisSession>
{
public:
	RedisSession(RedisDb *redis, const TcpConnectionPtr &conn);
	~RedisSession();

	void proxyReadCallback(const TcpConnectionPtr &conn, Buffer *buffer);
	int32_t processMultibulkBuffer(const TcpConnectionPtr &conn, Buffer *buffer);
	int32_t processInlineBuffer(const TcpConnectionPtr &conn, Buffer *buffer);
	void reset();
	
private:
	RedisDb *redis;
	RedisObjectPtr command;
	std::vector<RedisObjectPtr> redisCommands;
	const char *buf;
	size_t len;
	int32_t pos;
	int32_t reqtype;
	int32_t multibulklen;
	int32_t bulklen;
	int32_t argc;
};
