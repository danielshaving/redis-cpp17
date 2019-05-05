#pragma once
#include<memory>
#include<string>
#include<ratio>
#include<chrono>
#include<string_view>

#include "coding.h"
#include "db.h"
#include "option.h"
#include "redis.h"

class RedisDB;

class RedisString {
public:
	RedisString(RedisDB* redis, const Options& options, const std::string& path);
	~RedisString();

	Status open();

	Status set(const std::string_view& key, const std::string_view& value);

	Status setnx(const std::string_view& key, const std::string& value, int32_t* ret, const int32_t ttl = 0);

	Status setvx(const std::string_view& key, const std::string_view& value, const std::string_view& newValue, int32_t* ret, const int32_t ttl = 0);

	Status setxx(const std::string_view& key, const std::string_view& value, int32_t* ret, const int32_t ttl = 0);

	Status get(const std::string_view& key, std::string* value);

	Status getset(const std::string_view& key, const std::string_view& value, std::string* oldValue);

	Status setbit(const std::string_view& key, int64_t offset, int32_t value, int32_t* ret);

	Status getbit(const std::string_view& key, int64_t offset, int32_t* ret);

	Status mset(const std::vector<KeyValue>& kvs);

	Status msetnx(const std::vector<KeyValue>& kvs, int32_t* ret);

	Status mget(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss);

	Status del(const std::string_view& key);

	Status delvx(const std::string_view& key, const std::string_view& value, int32_t* ret);

	Status setrange(const std::string_view& key, int64_t startOffset,
		const std::string_view& value, int32_t* ret);

	Status getrange(const std::string_view& key, int64_t startOffset, int64_t endOffset,
		std::string* ret);

private:
	RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};
