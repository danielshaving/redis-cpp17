#pragma once
#include <string>
#include <map>
#include <list>
#include <queue>
#include <vector>
#include <unistd.h>
#include <vector>
#include <functional>
#include <cassert>
#include <iostream>

#include "option.h"
#include "cache.h"
#include "coding.h"
#include "db.h"
#include "redistring.h"
#include "redishash.h"
#include "rediszset.h"
#include "redislist.h"
#include "redisset.h"


class RedisDB {
public:
	RedisDB(const Options& options, const std::string& path);
	~RedisDB();

	Status Open();
	
	Status DestoryDB(const std::string path, const Options& options);

	const std::shared_ptr<Env> getEnv() {
		return options.env;
	}

	// Strings Commands

	// Set key to hold the string value. if key
	// already holds a value, it is overwritten
	Status Set(const std::string_view& key, const std::string_view& value);

	// Set key to hold the string value. if key exist
	Status Setxx(const std::string_view& key, const std::string_view& value, int32_t* ret, const int32_t ttl = 0);

	// Get the value of key. If the key does not exist
	// the special value nil is returned
	Status Get(const std::string_view& key, std::string* value);

	// Atomically sets key to value and returns the old value stored at key
	// Returns an error when key exists but does not hold a string value.
	Status GetSet(const std::string_view& key, const std::string_view& value, std::string* old_value);

	// Sets or clears the bit at offset in the string value stored at key
	Status SetBit(const std::string_view& key, int64_t offset, int32_t value, int32_t* ret);

	// Returns the bit value at offset in the string value stored at key
	Status GetBit(const std::string_view& key, int64_t offset, int32_t* ret);

	// Sets the given keys to their respective values
	// MSET replaces existing values with new values
	Status MSet(const std::vector<KeyValue>& kvs);

	// Returns the values of all specified keys. For every key
	// that does not hold a string value or does not exist, the
	// special value nil is returned
	Status MGet(const std::vector<std::string>& keys,
			std::vector<ValueStatus>* vss);

	// Set key to hold string value if key does not exist
	// return 1 if the key was set
	// return 0 if the key was not set
	Status Setnx(const std::string_view& key, 
		const std::string_view& value, int32_t* ret, const int32_t ttl = 0);

	// Sets the given keys to their respective values.
	// MSETNX will not perform any operation at all even
	// if just a single key already exists.
	Status MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret);

	// Set key to hold string new_value if key currently hold the give value
	// return 1 if the key currently hold the give value And override success
	// return 0 if the key doesn't exist And override fail
	// return -1 if the key currently does not hold the given value And override fail
	Status Setvx(const std::string_view& key, const std::string_view& value, 
		const std::string_view& new_value, int32_t* ret, const int32_t ttl = 0);

	// delete the key that holds a given value
	// return 1 if the key currently hold the give value And delete success
	// return 0 if the key doesn't exist And del fail
	// return -1 if the key currently does not hold the given value And del fail
	Status Delvx(const std::string_view& key, const std::string_view& value, int32_t* ret);

	// Set key to hold string value if key does not exist
	// return the length of the string after it was modified by the command
	Status Setrange(const std::string_view& key, int64_t startoffset,
				const std::string_view& value, int32_t* ret);

	// Returns the substring of the string value stored at key,
	// determined by the offsets start and end (both are inclusive)
	Status Getrange(const std::string_view& key, int64_t startoffset, int64_t endoffset,
				std::string* ret);

	// If key already exists and is a string, this command appends the value at
	// the end of the string
	// return the length of the string after the append operation
	Status Append(const std::string_view& key, const std::string_view& value, int32_t* ret);

	// Count the number of set bits (population counting) in a string.
	// return the number of bits set to 1
	// note: if need to specified offset, set have_range to true
	Status BitCount(const std::string_view& key, int64_t startoffset, int64_t endoffset,
				int32_t* ret, bool haveoffset);

	// Perform a bitwise operation between multiple keys
	// and store the result in the destination key
	Status BitOp(BitOpType op, const std::string& destkey,
			const std::vector<std::string>& srckeys, int64_t* ret);

	// Return the position of the first bit set to 1 or 0 in a string
	// BitPos key 0
	Status BitPos(const std::string_view& key, int32_t bit, int64_t* ret);
	// BitPos key 0 [start]
	Status BitPos(const std::string_view& key, int32_t bit,
			int64_t startoffset, int64_t* ret);
	// BitPos key 0 [start] [end]
	Status BitPos(const std::string_view& key, int32_t bit,
			int64_t startoffset, int64_t endoffset,
			int64_t* ret);

	// Decrements the number stored at key by decrement
	// return the value of key after the decrement
	Status Decrby(const std::string_view& key, int64_t value, int64_t* ret);

	// Increments the number stored at key by increment.
	// If the key does not exist, it is set to 0 before performing the operation
	Status Incrby(const std::string_view& key, int64_t value, int64_t* ret);

	// Increment the string representing a floating point number
	// stored at key by the specified increment.
	Status Incrbyfloat(const std::string_view& key, const std::string_view& value, std::string* ret);

	// Set key to hold the string value and set key to timeout after a given
	// number of seconds
	Status Setex(const std::string_view& key, const std::string_view& value, int32_t ttl);

	// Returns the length of the string value stored at key. An error
	// is returned when key holds a non-string value.
	Status Strlen(const std::string_view& key, int32_t* len);

	// PKSETEXAT has the same effect and semantic as SETEX, but instead of
	// specifying the number of seconds representing the TTL (time to live), it
	// takes an absolute Unix timestamp (seconds since January 1, 1970). A
	// timestamp in the past will delete the key immediately.
	Status PKSetexAt(const std::string_view& key, const std::string_view& value, int32_t timestamp);

	// Hashes Commands

	// Sets field in the hash stored at key to value. If key does not exist, a new
	// key holding a hash is created. If field already exists in the hash, it is
	// overwritten.
	Status HSet(const std::string_view& key, const std::string_view& field, const std::string_view& value,
				int32_t* res);

	// Returns the value associated with field in the hash stored at key.
	// the value associated with field, or nil when field is not present in the
	// hash or key does not exist.
	Status HGet(const std::string_view& key, const std::string_view& field, std::string* value);

	// Sets the specified fields to their respective values in the hash stored at
	// key. This command overwrites any specified fields already existing in the
	// hash. If key does not exist, a new key holding a hash is created.
	Status HMSet(const std::string_view& key,const std::vector<FieldValue>& fvs);

	// Hashes Commands
	// Returns the values associated with the specified fields in the hash stored
	// at key.
	// For every field that does not exist in the hash, a nil value is returned.
	// Because a non-existing keys are treated as empty hashes, running HMGET
	// against a non-existing key will return a list of nil values.
	Status HMGet(const std::string_view& key, const std::vector<std::string>& fields,
			std::vector<ValueStatus>* vss);

	// Returns all fields and values of the hash stored at key. In the returned
	// value, every field name is followed by its value, so the length of the
	// reply is twice the size of the hash.
	Status HGetall(const std::string_view& key, std::vector<FieldValue>* fvs);

	// Returns all field names in the hash stored at key.
	Status HKeys(const std::string_view& key, std::vector<std::string>* fields);

	// Returns all values in the hash stored at key.
	Status HVals(const std::string_view& key, std::vector<std::string>* values);

	// Sets field in the hash stored at key to value, only if field does not yet
	// exist. If key does not exist, a new key holding a hash is created. If field
	// already exists, this operation has no effect.
	Status HSetnx(const std::string_view& key, const std::string_view& field, 
		const std::string_view& value, int32_t* ret);

	// Returns the number of fields contained in the hash stored at key.
	// Return 0 when key does not exist.
	Status HLen(const std::string_view& key, int32_t* ret);

	// Returns the string length of the value associated with field in the hash
	// stored at key. If the key or the field do not exist, 0 is returned.
	Status HStrlen(const std::string_view& key, const std::string_view& field, int32_t* len);

	// Returns if field is an existing field in the hash stored at key.
	// Return Status::Ok() if the hash contains field.
	// Return Status::NotFound() if the hash does not contain field,
	// or key does not exist.
	Status HExists(const std::string_view& key, const std::string_view& field);

	// Increments the number stored at field in the hash stored at key by
	// increment. If key does not exist, a new key holding a hash is created. If
	// field does not exist the value is set to 0 before the operation is
	// performed.
	Status HIncrby(const std::string_view& key, const std::string_view& field, int64_t value,
					int64_t* ret);

	// Zsets Commands

	// Adds all the specified members with the specified scores to the sorted set
	// stored at key. It is possible to specify multiple score / member pairs. If
	// a specified member is already a member of the sorted set, the score is
	// updated and the element reinserted at the right position to ensure the
	// correct ordering.
	//
	// If key does not exist, a new sorted set with the specified members as sole
	// members is created, like if the sorted set was empty. If the key exists but
	// does not hold a sorted set, an error is returned.
	//
	// The score values should be the string representation of a double precision
	// floating point number. +inf and -inf values are valid values as well.
	Status ZAdd(const std::string_view& key,
			const std::vector<ScoreMember>& scoremembers,
			int32_t* ret);

	// Returns the sorted set cardinality (number of elements) of the sorted set
	// stored at key.
	Status ZCard(const std::string_view& key, int32_t* ret);


	// Returns the specified range of elements in the sorted set stored at key.
	// The elements are considered to be ordered from the lowest to the highest
	// score. Lexicographical order is used for elements with equal score.
	//
	// See ZREVRANGE when you need the elements ordered from highest to lowest
	// score (and descending lexicographical order for elements with equal score).
	//
	// Both start and stop are zero-based indexes, where 0 is the first element, 1
	// is the next element and so on. They can also be negative numbers indicating
	// offsets from the end of the sorted set, with -1 being the last element of
	// the sorted set, -2 the penultimate element and so on.
	//
	// start and stop are inclusive ranges, so for example ZRANGE myzset 0 1 will
	// return both the first and the second element of the sorted set.
	//
	// Out of range indexes will not produce an error. If start is larger than the
	// largest index in the sorted set, or start > stop, an empty list is
	// returned. If stop is larger than the end of the sorted set Redis will treat
	// it like it is the last element of the sorted set.
	//
	// It is possible to pass the WITHSCORES option in order to return the scores
	// of the elements together with the elements. The returned list will contain
	// value1,score1,...,valueN,scoreN instead of value1,...,valueN. Client
	// libraries are free to return a more appropriate data type (suggestion: an
	// array with (value, score) arrays/tuples).
	Status ZRange(const std::string_view& key,
				int32_t start,
				int32_t stop,
				std::vector<ScoreMember>* scoremembers);


	// Returns the number of elements in the sorted set at key with a score
	// between min and max.
	//
	// The min and max arguments have the same semantic as described for
	// ZRANGEBYSCORE.
	//
	// Note: the command has a complexity of just O(log(N)) because it uses
	// elements ranks (see ZRANK) to get an idea of the range. Because of this
	// there is no need to do a work proportional to the size of the range.
	Status ZCount(const std::string_view& key,
			double min,
			double max,
			bool leftclose,
			bool rightclose,
			int32_t* ret);
	
	// Sets Commands

	// Add the specified members to the set stored at key. Specified members that
	// are already a member of this set are ignored. If key does not exist, a new
	// set is created before adding the specified members.
	Status SAdd(const std::string_view& key, const std::vector<std::string>& members,
		int32_t* ret);

	// Returns the set cardinality (number of elements) of the set stored at key.
	Status SCard(const std::string_view& key, int32_t* ret);

	// Keys Commands

	// Note:
	// While any error happens, you need to check type_status for
	// the error message

	// Set a timeout on key
	// return -1 operation exception errors happen in database
	// return >=0 success
	int32_t Expire(const std::string_view& key, int32_t ttl,
					std::map<DataType, Status>* typestatus);
	
	// Removes the specified keys
	// return -1 operation exception errors happen in database
	// return >=0 the number of keys that were removed
	int64_t Del(const std::vector<std::string>& keys, std::map<DataType, Status>* typestatus);

	// Returns the remaining time to live of a key that has a timeout.
	// return -3 operation exception errors happen in database
	// return -2 if the key does not exist
	// return -1 if the key exists but has not associated expire
	// return > 0 TTL in seconds
	std::map<DataType, int64_t> TTL(const std::string_view& key,
								std::map<DataType, Status>* typestatus);
	
	// Reutrns the data type of the key
	Status Type(const std::string& key, std::string* type);

	Status Keys(const std::string& type,
			  const std::string& pattern,
			  std::vector<std::string>* keys);
			  
	// Admin Commands
	Status StartBGThread();
	
	Status RunBGTask();
	
	Status AddBGTask(const BGTask& bgtask);
  
	Status DoCompact(const DataType& type);
		
	Status Compact(const DataType& type, bool sync = false);
	
	Status CompactKey(const DataType& type, const std::string& key);
	
private:
	std::shared_ptr<RedisString> redisstring;
	std::shared_ptr<RedisHash> redishash;
	std::shared_ptr<RedisZset> rediszset;
	std::shared_ptr<RedisList> redislist;
	std::shared_ptr<RedisSet> redisset;
	const Options options;
	std::string path;
		
	std::unique_ptr<std::thread> bgthread;
	std::mutex bgtasksmutex;
	std::condition_variable bgtaskscondvar;
	std::queue<BGTask> bgtasksqueue;
	std::atomic<int> currenttasktype;
	std::atomic<bool> bgtasksshouldexit;
	std::atomic<bool> scankeynumexit;
};

