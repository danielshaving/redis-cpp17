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

	// Strings Commands

	// Set key to hold the string value. if key
	// already holds a value, it is overwritten
	Status set(const std::string_view &key, const std::string_view &value);

	// Set key to hold the string value. if key exist
	Status setxx(const std::string_view &key, const std::string_view &value, int32_t *ret, const int32_t ttl = 0);

	 // Get the value of key. If the key does not exist
	 // the special value nil is returned
	Status get(const std::string_view &key, std::string *value);

	// Atomically sets key to value and returns the old value stored at key
	// Returns an error when key exists but does not hold a string value.
	Status getSet(const std::string_view &key, const std::string_view &value, std::string *oldValue);

	// Sets or clears the bit at offset in the string value stored at key
	Status setBit(const std::string_view &key, int64_t offset, int32_t value, int32_t *ret);

	// Returns the bit value at offset in the string value stored at key
	Status getBit(const std::string_view &key, int64_t offset, int32_t *ret);

	 // Sets the given keys to their respective values
	 // MSET replaces existing values with new values
	Status mset(const std::vector <KeyValue> &kvs);

	// Returns the values of all specified keys. For every key
	// that does not hold a string value or does not exist, the
	// special value nil is returned
	Status mget(const std::vector <std::string> &keys, std::vector <ValueStatus> *vss);

	// Set key to hold string value if key does not exist
	// return 1 if the key was set
	// return 0 if the key was not set
	Status setnx(const std::string_view &key, const std::string &value, int32_t *ret, const int32_t ttl = 0);

	// Sets the given keys to their respective values.
	// MSETNX will not perform any operation at all even
	// if just a single key already exists.
	Status msetnx(const std::vector <KeyValue>& kvs, int32_t* ret);

	// Set key to hold string new_value if key currently hold the give value
	// return 1 if the key currently hold the give value And override success
	// return 0 if the key doesn't exist And override fail
	// return -1 if the key currently does not hold the given value And override fail
	Status setvx(const std::string_view &key, const std::string_view &value, const std::string_view &newValue, int32_t *ret, const int32_t ttl = 0);

	// Removes the specified keys
	// return -1 operation exception errors happen in database
	// return >=0 the number of keys that were removed
	int64_t del(const std::vector <std::string> &keys,
			  std::map<DataType, Status> *typeStatus);

	// delete the key that holds a given value
	// return 1 if the key currently hold the give value And delete success
	// return 0 if the key doesn't exist And del fail
	// return -1 if the key currently does not hold the given value And del fail
	Status delvx(const std::string_view &key, const std::string_view &value, int32_t *ret);

	// Set key to hold string value if key does not exist
	// return the length of the string after it was modified by the command
	Status setrange(const std::string_view &key, int64_t startOffset,
				  const std::string_view &value, int32_t *ret);

	// Returns the substring of the string value stored at key,
	// determined by the offsets start and end (both are inclusive)
	Status getrange(const std::string_view &key, int64_t startOffset, int64_t endOffset,
				  std::string *ret);
private:
	std::shared_ptr <RedisString> redisString;
	std::shared_ptr <Env> env;
	const Options options;
	std::string path;
};

