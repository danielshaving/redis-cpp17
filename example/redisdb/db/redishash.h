#pragma once
#include <memory>
#include <string>
#include <ratio>
#include <chrono>
#include <string_view>

#include "option.h"
#include "coding.h"
#include "db.h"
#include "redis.h"
#include "lockmgr.h"

class RedisDB;

class RedisHash {
public:
	RedisHash(RedisDB* redis, 
		const Options& options, const std::string& path);
	~RedisHash();

	Status Open();

	Status Hset(const std::string_view& key, 
		const std::string_view& field, const std::string_view& value, int32_t* res);

	Status Hget(const std::string_view& key,
		const std::string_view& field, std::string* value);

	Status Hmset(const std::string_view& key, 
		const std::vector<FieldValue>& fvs);

	Status Hmget(const std::string_view& key,
		const std::vector<std::string>& fields, std::vector<ValueStatus>* vss);

	Status Hgetall(const std::string_view& key,
		std::vector<FieldValue>* fvs);


private:
	RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};
