#pragma once
#include <string>
#include <map>
#include <list>
#include <queue>
#include <vector>
#include <unistd.h>

#include "dbimpl.h"
#include "option.h"
#include "cache.h"
#include "coding.h"
#include <vector>
#include <functional>
#include <cassert>
#include <iostream>
#include "redistring.h"


class Redis {
public:
	Redis(const Options &options, const std::string &path);
	~Redis();

	Status open();

	Status set(const std::string_view &key, const std::string_view &value);

	Status setxx(const std::string_view &key, const std::string_view &value, int32_t *ret, const int32_t ttl = 0);

	Status get(const std::string_view &key, std::string *value);

	Status getSet(const std::string_view &key, const std::string_view &value, std::string *oldValue);

	Status setBit(const std::string_view &key, int64_t offset, int32_t value, int32_t *ret);

	Status getBit(const std::string_view &key, int64_t offset, int32_t *ret);

	Status mset(const std::vector<KeyValue> &kvs);

	Status mget(const std::vector<std::string> &keys, std::vector<ValueStatus> *vss);
private:
	std::shared_ptr <RedisString> redisString;
	std::shared_ptr <Env> env;
	const Options options;
	std::string path;
};

