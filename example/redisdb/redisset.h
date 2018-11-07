#pragma once
#include "all.h"
#include "object.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

class RedisDb;
class RedisSet
{
public:
	RedisSet(RedisDb *db);
	~RedisSet();
	
	bool setCommand(const std::vector<RedisObjectPtr> &commands,
		const TcpConnectionPtr &conn);
	bool getCommand(const std::vector<RedisObjectPtr> &commands,
		const TcpConnectionPtr &conn);
	bool delCommand(const RedisObjectPtr &command,
		const TcpConnectionPtr &conn);
	
private:
	leveldb::DB *db;
	RedisDb *redisdb;
	std::string value;
	std::string dbsize;
};