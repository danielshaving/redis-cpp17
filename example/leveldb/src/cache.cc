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
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
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

std::shared_ptr <LRUHandle> LRUCache::lookup(const std::string_view &key, uint32_t hash) {
    std::shared_ptr <LRUHandle> e(new LRUHandle);
    e->keyData = (char *) malloc(key.size());
    memcpy(e->keyData, key.data(), key.size());
    e->hash = hash;
    auto it = tables.find(e);
    if (it == tables.end()) {
        return nullptr;
    } else {
        return *it;
    }
}

std::shared_ptr <LRUHandle> LRUCache::insert(const std::string_view &key, uint32_t hash,
                                             const std::any &value, size_t charge,
                                             std::function<void(const std::string_view &k,
                                                                const std::any &value)> &deleter) {
    assert(tables.size() == lru.size());
    assert(value.has_value());

    std::shared_ptr <LRUHandle> e(new LRUHandle);
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->keyLength = key.size();
    e->inCache = false;
    e->keyData = (char *) malloc(key.size());
    memcpy(e->keyData, key.data(), key.size());

    e->inCache = true;
    usage += charge;
    lru.push_back(e);
    tables.insert(e);

    while (usage > capacity && !lru.empty()) {
        std::shared_ptr <LRUHandle> efront = lru.front();
        lru.pop_front();
        size_t n = tables.erase(efront);
        assert(n == 1);
    }
    return e;
}

void LRUCache::erase(const std::string_view &key, uint32_t hash) {
    std::shared_ptr <LRUHandle> e(new LRUHandle);
    e->keyData = (char *) malloc(key.size());
    memcpy(e->keyData, key.data(), key.size());
    e->hash = hash;
    auto it = tables.find(e);
    assert(it != tables.end());
    (*it)->deleter(std::string_view((*it)->keyData, (*it)->keyLength), (*it)->value);
    size_t n = tables.erase(e);
    assert(n == 1);
}
