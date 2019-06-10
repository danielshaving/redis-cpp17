#pragma once
#include <memory>
#include <string>
#include <ratio>
#include <chrono>
#include <string_view>

#include "option.h"
#include "coding.h"
#include "db.h"
#include "serialize.h"
#include "redis.h"
#include "lockmgr.h"

class RedisDB;
class RedisZset {
public:
	RedisZset(RedisDB* redis, 
		const Options& options, const std::string& path);
	~RedisZset();

	Status Open();

	Status DestroyDB(const std::string path, const Options& options);

	Status ZAdd(const std::string_view& key,
		const std::vector<ScoreMember>& scoremembers, int32_t* ret);

	Status ZRange(const std::string_view& key,
		int32_t start, int32_t stop, std::vector<ScoreMember>* scoremembers);

	Status ZRank(const std::string_view& key,
		const std::string_view& member, int32_t* rank);

	Status ZCount(const std::string_view& key, double min, double max,
        bool leftclose, bool rightclose, int32_t* ret);

	Status ZCard(const std::string_view& key, int32_t* card);

	Status ZIncrby(const std::string_view& key, const std::string_view& member,
		double increment, double* ret);
	
	Status Expire(const std::string_view& key, int32_t ttl);

	Status ScanKeyNum(KeyInfo* keyinfo);

	Status Del(const std::string_view& key);

	Status ScanKeys(const std::string& pattern,
				std::vector<std::string>* keys);
private:
	RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};
