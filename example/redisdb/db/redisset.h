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

	Status DestroyDB(const std::string path, const Options& options);

	// Setes Commands
	Status SAdd(const std::string_view& key,
			const std::vector<std::string>& members, int32_t* ret);

	Status SCard(const std::string_view& key, int32_t* ret);

	Status SMembers(const std::string_view& key,
                  std::vector<std::string>* members);

	Status SIsmember(const std::string_view& key, 
			const std::string_view& member, int32_t* ret);

	Status ScanKeyNum(KeyInfo* keyinfo);

	Status ScanKeys(const std::string& pattern,
				std::vector<std::string>* keys);

	Status Del(const std::string_view& key);
				
	Status Expire(const std::string_view& key, int32_t ttl);
private:
    RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};