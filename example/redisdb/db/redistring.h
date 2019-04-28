#pragma once
#include <memory>
#include <string>

#include "dbimpl.h"
#include "option.h"
#include "stringformat.h"

class Redis;

class RedisString {
public:
	RedisString(Redis *redis, const Options &options, const std::string &path);
	~RedisString();
	
	Status open();
	
	Status set(const std::string_view &key, const std::string_view &value);
	
	Status get(const std::string_view &key, std::string *value);
	  
	Status setxx(const std::string_view &key, const std::string_view &value, int32_t *ret, const int32_t ttl = 0);

	Status getSet(const std::string_view &key, const std::string_view &value, std::string *oldValue);
	
	Status setBit(const std::string_view &key, int64_t offset, int32_t value, int32_t *ret);
	
	Status getBit(const std::string_view &key, int64_t offset, int32_t *ret);
	
	Status mset(const std::vector<KeyValue> &kvs);
	
	Status mget(const std::vector<std::string> &keys, std::vector<ValueStatus> *vss);
	
private:
	Redis *redis;
	std::shared_ptr <DBImpl> db;
};