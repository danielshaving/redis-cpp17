#pragma once
#include <memory>
#include <string>

#include "dbimpl.h"
#include "option.h"
#include "redisdb.h"

#include <string>
#include <ratio>
#include <chrono>
#include <string_view>
#include "coding.h"


class Redis;

class RedisHash {
public:
	RedisHash(Redis *redis, const Options &options, const std::string &path);
	~RedisHash();
	
	Status open();
	
	Status hset(const std::string_view &key, const std::string_view &field, const std::string_view &value, int32_t *res);

	Status hget(const std::string_view &key, const std::string_view &field, std::string *value);

	Status hmset(const std::string_view &key, const std::vector <FieldValue> &fvs);
		   
	Status hmget(const std::string_view &key, const std::vector <std::string> &fields, std::vector <ValueStatus> *vss);

	Status hgetall(const std::string_view &key, std::vector <FieldValue> *fvs);

		   
private:
	Redis *redis;
	std::shared_ptr <DBImpl> db;
};