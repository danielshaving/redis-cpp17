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

class RedisList {
public:
	RedisList(RedisDB* redis,
		const Options& options, const std::string& path);
	~RedisList();

	Status Open();

	Status LPop(const std::string_view& key, std::string* element);
	
	Status LPush(const std::string_view& key, const std::vector<std::string>& values,
		uint64_t* ret);

	Status LRange(const std::string_view& key, int64_t start, int64_t stop,
	        std::vector<std::string>* ret);
	
	Status LLen(const std::string_view& key, uint64_t* len);

	Status LInsert(const std::string_view& key, const BeforeOrAfter& beforeorafter,
	         const std::string& pivot, const std::string& value,
	         int64_t* ret);

	Status LRem(const std::string_view& key, int64_t count,
              const std::string_view& value, uint64_t* ret);

private:
	RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};