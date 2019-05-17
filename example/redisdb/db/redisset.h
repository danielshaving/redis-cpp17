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

class RedisSet {
public:
	RedisSet(RedisDB* redis, 
		const Options& options, const std::string& path);
	~RedisSet();

	Status Open();

	// Setes Commands
	Status SAdd(const std::string_view& key,
			const std::vector<std::string>& members, int32_t* ret);

	Status SCard(const std::string_view& key, int32_t* ret);

	Status SMembers(const std::string_view& key,
                  std::vector<std::string>* members);

	Status SIsmember(const std::string_view& key, 
			const std::string_view& member, int32_t* ret);

private:
    RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};