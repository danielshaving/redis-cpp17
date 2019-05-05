#pragma once
#include<memory>
#include<string>
#include<ratio>
#include<chrono>
#include<string_view>

#include "option.h"
#include "coding.h"
#include "db.h"
#include "redis.h"

class RedisDB;
class RedisZset {
public:
	RedisZset(RedisDB* redis, const Options& options, const std::string& path);
	~RedisZset();

	Status open();

	Status zadd(const std::string_view& key,
		const std::vector<ScoreMember>& scoremembers, int32_t* ret);

	Status zrange(const std::string_view& key,
		int32_t start, int32_t stop, std::vector<ScoreMember>* scoremembers);

	Status zrank(const std::string_view& key,
		const std::string_view& member, int32_t* rank);

	Status zcard(const std::string_view& key, int32_t* card);

	Status zincrby(const std::string_view& key,
	                 const std::string_view& member,
	                 double increment,
	                 double* ret);

private:
	RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};
