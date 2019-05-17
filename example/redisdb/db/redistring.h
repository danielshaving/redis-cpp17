#pragma once
#include <memory>
#include <string>
#include <ratio>
#include <chrono>
#include <string_view>
#include "lockmgr.h"
#include "coding.h"
#include "db.h"
#include "option.h"
#include "redis.h"

class RedisDB;

class RedisString {
public:
	RedisString(RedisDB* redis,
		const Options& options, const std::string& path);
	~RedisString();

	Status Open();

	Status Set(const std::string_view& key,
		const std::string_view& value);

	Status Setnx(const std::string_view& key,
		const std::string& value, int32_t* ret, 
		const int32_t ttl = 0);

	Status Setvx(const std::string_view& key,
		const std::string_view& value,
		const std::string_view& newValue,
		int32_t* ret, const int32_t ttl = 0);

	Status Setxx(const std::string_view& key,
		const std::string_view& value, int32_t* ret, 
		const int32_t ttl = 0);

	Status Get(const std::string_view& key,
		std::string* value);

	Status GetSet(const std::string_view& key,
		const std::string_view& value, std::string* oldValue);

	Status SetBit(const std::string_view& key,
		int64_t offset, int32_t value, int32_t* ret);

	Status GetBit(const std::string_view& key,
		int64_t offset, int32_t* ret);

	Status Mset(const std::vector<KeyValue>& kvs);

	Status Msetnx(const std::vector<KeyValue>& kvs,
		int32_t* ret);

	Status Mget(const std::vector<std::string>& keys,
		std::vector<ValueStatus>* vss);

	Status Delete(const std::string_view& key);

	Status Delvx(const std::string_view& key,
		const std::string_view& value, int32_t* ret);

	Status Setrange(const std::string_view& key,
		int64_t startOffset,
		const std::string_view& value, int32_t* ret);

	Status Getrange(const std::string_view& key,
		int64_t startOffset, int64_t endOffset,
		std::string* ret);

	Status Strlen(const std::string_view& key,
		int32_t* len);

	Status Expire(const std::string_view& key,
		int32_t ttl);

	bool Scan(const std::string& startkey,
		const std::string& pattern,
		std::vector<std::string>* keys,
		int64_t* Count, std::string* nextkey);

	Status Expireat(const std::string_view& key,
		int32_t timestamp);

	Status Persist(const std::string_view& key);

	Status TTL(const std::string_view& key,
		int64_t* timestamp);

	Status Incrby(const std::string_view& key,
		int64_t value, int64_t* ret);

	Status Incrbyfloat(const std::string_view& key,
		const std::string_view& value, std::string* ret);

	Status GetProperty(const std::string& property, 
		uint64_t* out);

	Status ScanKeyNum(KeyInfo* keyinfo);

	Status Decrby(const std::string_view& key,
		int64_t value, int64_t* ret);

	Status Append(const std::string_view& key,
		const std::string_view& value, int32_t* ret);

	Status BitCount(const std::string_view& key, 
		int64_t startoffset, int64_t endoffset,
		int32_t* ret, bool haverange);

	Status BitOp(BitOpType op, const std::string& destkey,
		const std::vector<std::string>& srckeys, int64_t* ret);

	Status CompactRange(const std::string_view* begin,
		const std::string_view* end);
private:
	RedisDB* redis;
	std::shared_ptr<DB> db;
	LockMgr lockmgr;
};
