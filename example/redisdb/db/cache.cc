#include "cache.h"

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  Contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  Contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

LRUHandle::LRUHandle()
	: keyData(nullptr) {

}

LRUHandle::~LRUHandle() {
	if (keyData != nullptr) {
		free(keyData);
	}
}

LRUCache::LRUCache()
	: usage(0) {

}

LRUCache::~LRUCache() {

}

std::shared_ptr<LRUHandle> LRUCache::Lookup(const std::string_view& key, uint32_t hash) {
	std::unique_lock<std::mutex> lk(mutex);
	std::shared_ptr<LRUHandle> e(new LRUHandle);
	e->keyData = (char*)malloc(key.size());
	memcpy(e->keyData, key.data(), key.size());
	e->hash = hash;
	auto it = tables.find(e);
	if (it == tables.end()) {
		return nullptr;
	}
	else {
		return *it;
	}
}

std::shared_ptr<LRUHandle> LRUCache::Insert(const std::string_view& key, uint32_t hash,
	const std::any& value, size_t charge, 
	std::function<void(const std::string_view& k, const std::any& value)>& deleter) {

	std::unique_lock<std::mutex> lk(mutex);
	assert(tables.size() == lru.size());

	assert(value.has_value());
	std::shared_ptr<LRUHandle> e(new LRUHandle);
	e->value = value;
	e->deleter = deleter;
	e->charge = charge;
	e->keyLength = key.size();
	e->inCache = false;
	e->keyData = (char*)malloc(key.size());
	memcpy(e->keyData, key.data(), key.size());

	if (capacity > 0) {
		e->inCache = true;
		usage += charge;
		tables.insert(e);
		lru.push_back(e);
	}
	else {

	}

	while (usage > capacity&& !lru.empty()) {
		auto front = lru.front();
		lru.pop_front();
		usage -= front->charge;
		size_t n = tables.erase(front);
		assert(n == 1);
	}

	assert(tables.size() == lru.size());
	return e;
}

void LRUCache::Release(const std::shared_ptr<LRUHandle>& handle) {
	std::unique_lock<std::mutex> lk(mutex);
	for (auto it = lru.begin(); it != lru.end(); ) {
		if ((*it)->key() == handle->key()) {
			lru.erase(it);
			break;
		}
		else {
			++it;
		}
	}

	size_t n = tables.erase(handle);
	assert(n == 1);
}

void LRUCache::Erase(const std::string_view& key, uint32_t hash) {
	std::unique_lock<std::mutex> lk(mutex);
	assert(tables.size() == lru.size());
	for (auto it = lru.begin(); it != lru.end(); ) {
		if ((*it)->key() == key) {
			usage -= (*it)->charge;
			auto iter = tables.find((*it));
			assert(iter != tables.end());
			if ((*iter)->deleter) {
				(*iter)->deleter(std::string_view((*iter)->keyData,
					(*iter)->keyLength), (*iter)->value);
			}

			lru.erase(it);
			tables.erase(iter);
			break;
		}
		else {
			++it;
		}
	}
	assert(tables.size() == lru.size());
}

std::shared_ptr<ShardedLRUCache> NewLRUCache(size_t capacity) {
	std::shared_ptr<ShardedLRUCache> cache(new ShardedLRUCache(capacity));
	return cache;
}
