#pragma once

#include <stdint.h>
#include <string_view>
#include <any>
#include <list>
#include <memory>
#include <assert.h>
#include <functional>
#include <unordered_set>

#include "filename.h"
#include "table.h"
#include "coding.h"

class LRUCache;

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
class LRUHandle {
public:
	LRUHandle();

	~LRUHandle();

	std::any value;
	std::function<void(const std::string_view& k,
		const std::any& value)> deleter;

	size_t charge;      // TODO(opt): Only allow uint32_t?
	size_t keyLength;
	bool inCache;      // Whether entry is in the cache.
	uint32_t refs;      // References, including cache reference, if present.
	uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
	char* keyData;   // Beginning of key

	std::string_view key() const {
		// next_ is only Equal to this if the LRU handle is the list head of an
		// empty list. List heads never have meaningful keys.
		return std::string_view(keyData, keyLength);
	}
};

// A single Shard of sharded cache.
class LRUCache {
public:
	LRUCache();

	~LRUCache();

	// Separate from constructor so caller can easily make an array of LRUCache
	void SetCapacity(size_t capacity) { this->capacity = capacity; }

	// Like Cache methods, but with an extra "hash" parameter.
	std::shared_ptr<LRUHandle> Insert(const std::string_view& key, uint32_t hash,
		const std::any& value, size_t charge,
		std::function<void(const std::string_view& k, const std::any& value)>& deleter);

	std::shared_ptr<LRUHandle> Lookup(const std::string_view& key, uint32_t hash);

	void Release(const std::shared_ptr<LRUHandle>& handle);

	void Erase(const std::string_view& key, uint32_t hash);

	size_t TotalCharge() const {
		std::unique_lock<std::mutex> lk(mutex);
		return usage;
	}

	struct Hash {
		size_t operator()(const std::shared_ptr<LRUHandle>& x) const {
			return x->hash;
		}
	};

	struct Equal {
		bool operator()(const std::shared_ptr<LRUHandle>& x,
			const std::shared_ptr<LRUHandle>& y) const {
			return memcmp(x->key().data(),
				y->key().data(), y->key().size()) == 0;
		}
	};

private:
	// Initialized before use.
	size_t capacity;
	size_t usage;
	mutable std::mutex mutex;
	std::list<std::shared_ptr<LRUHandle>> lru;
	std::list<std::shared_ptr<LRUHandle>> uselru;
	std::unordered_set<std::shared_ptr<LRUHandle>, Hash, Equal> tables;
};

static const int kNumShardBits = 4;
static const int kNumShards = 1<< kNumShardBits;

class ShardedLRUCache {
private:
	LRUCache shards[kNumShards];
	uint64_t lastId;

	static inline uint32_t HashView(const std::string_view& s) {
		return std::hash<std::string>{}(std::string(s.data(),s.size()));
	}

	static uint32_t Shard(uint32_t hash) {
		return hash >> (32 - kNumShardBits);
	}

public:
	explicit ShardedLRUCache(size_t capacity)
		: lastId(0) {
		const size_t perShard = (capacity + (kNumShards - 1)) / kNumShards;
		for (int s = 0; s< kNumShards; s++) {
			shards[s].SetCapacity(perShard);
		}
	}

	~ShardedLRUCache() {

	}

	std::shared_ptr<LRUHandle> Insert(const std::string_view& key, const std::any& value, size_t charge,
		std::function<void(const std::string_view& k,
			const std::any& value)>&& deleter) {
		const uint32_t hash = HashView(key);
		return shards[Shard(hash)].Insert(key, hash, value, charge, deleter);
	}

	std::shared_ptr<LRUHandle> Lookup(const std::string_view& key) {
		const uint32_t hash = HashView(key);
		return shards[Shard(hash)].Lookup(key, hash);
	}

	void Erase(const std::string_view& key) {
		const uint32_t hash = HashView(key);
		shards[Shard(hash)].Erase(key, hash);
	}

	const std::any& Value(const std::shared_ptr<LRUHandle>& handle) {
		return handle->value;
	}

	void Release(const std::shared_ptr<LRUHandle>& handle) {
		shards[Shard(handle->hash)].Release(handle);
	}

	uint64_t NewId() {
		return ++(lastId);
	}

	size_t TotalCharge() const {
		size_t total = 0;
		for (int s = 0; s< kNumShards; s++) {
			total += shards[s].TotalCharge();
		}
		return total;
	}
};


std::shared_ptr<ShardedLRUCache> NewLRUCache(size_t capacity);