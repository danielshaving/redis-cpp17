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

class RedisHash {
public:
	RedisHash(RedisDB* redis,
		const Options& options, const std::string& path);
	~RedisHash();

	Status Open();
	
	Status CompactRange(const std::string_view* begin,
                      const std::string_view* end, const ColumnFamilyType& type = kMetaAndData);
					  
	Status DestroyDB(const std::string path, const Options& options);

	Status HExists(const std::string_view& key, const std::string_view& field);

	Status HSet(const std::string_view& key,
		const std::string_view& field, const std::string_view& value, int32_t* res);

	Status HGet(const std::string_view& key,
		const std::string_view& field, std::string* value);

	Status HMSet(const std::string_view& key,
		const std::vector<FieldValue>& fvs);

	Status HMGet(const std::string_view& key,
		const std::vector<std::string>& fields, std::vector<ValueStatus>* vss);

	Status HGetall(const std::string_view& key,
		std::vector<FieldValue>* fvs);

	Status HDel(const std::string_view& key,
		const std::vector<std::string>& fields, int32_t* ret);

	Status Expire(const std::string_view& key, int32_t ttl);

	Status Del(const std::string_view& key);

	Status ScanKeyNum(KeyInfo* keyinfo);

	Status ScanKeys(const std::string& pattern,
		std::vector<std::string>* keys);

	Status HKeys(const std::string_view& key,
		std::vector<std::string>* fields);

private:
	RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};
