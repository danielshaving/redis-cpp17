#include "block.h"
#include <vector>
#include <algorithm>

inline uint32_t Block::numRestarts() const {
    assert(size >= sizeof(uint32_t));
    return decodeFixed32(data + size - sizeof(uint32_t));
}

// Helper routine: decode the next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not dereference past "limit".
// 
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static inline const char *decodeEntry(const char *p, const char *limit,
                                      uint32_t *shared,
                                      uint32_t *nonShared,
                                      uint32_t *valueLength) {
    if (limit - p < 3) return nullptr;
    *shared = reinterpret_cast<const unsigned char *>(p)[0];
    *nonShared = reinterpret_cast<const unsigned char *>(p)[1];
    *valueLength = reinterpret_cast<const unsigned char *>(p)[2];
    if ((*shared | *nonShared | *valueLength) < 128) {
        // Fast path: all three values are encoded in one byte each
        p += 3;
    } else {
        if ((p = getVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
        if ((p = getVarint32Ptr(p, limit, nonShared)) == nullptr) return nullptr;
        if ((p = getVarint32Ptr(p, limit, valueLength)) == nullptr) return nullptr;
    }

    if (static_cast<uint32_t>(limit - p) < (*nonShared + *valueLength)) {
        return nullptr;
    }
    return p;
}

class BlockIterator : public Iterator {
private:
    const Comparator *const comparator;
    const char *const data;      // underlying block contents
    uint32_t const restarts;     // Offset of restart array (list of fixed32)
    uint32_t const numRestarts; // Number of uint32_t entries in restart array

    // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
    uint32_t current;
    uint32_t restartIndex;  // Index of restart block in which current_ falls
    std::string k;
    std::string_view v;
    Status s;
    std::list <std::any> clearnups;

    inline int compare(const std::string_view &a, const std::string_view &b) const {
        return comparator->compare(a, b);

        int r = comparator->compare(extractUserKey(a), extractUserKey(b));
        if (r == 0) {
            const uint64_t anum = decodeFixed64(a.data() + a.size() - 8);
            const uint64_t bnum = decodeFixed64(b.data() + b.size() - 8);

            if (anum > bnum) {
                r = -1;
            } else if (anum < bnum) {
                r = +1;
            }
        }
        return r;
    }

    // Return the offset in data_ just past the end of the current entry.
    inline uint32_t nextEntryOffset() const {
        return (v.data() + v.size()) - data;
    }

    uint32_t getRestartPoint(uint32_t index) {
        assert(index < numRestarts);
        return decodeFixed32(data + restarts + index * sizeof(uint32_t));
    }

    void seekToRestartPoint(uint32_t index) {
        k.clear();
        restartIndex = index;
        // current_ will be fixed by ParseNextKey();

        // ParseNextKey() starts at the end of value_, so set value_ accordingly
        uint32_t offset = getRestartPoint(index);
        v = std::string_view(data + offset, 0);
    }

public:
    BlockIterator(const Comparator *comparator,
                  const char *data,
                  uint32_t restarts,
                  uint32_t numRestarts)
            : comparator(comparator),
              data(data),
              restarts(restarts),
              numRestarts(numRestarts),
              current(restarts),
              restartIndex(numRestarts) {

        assert(numRestarts > 0);
    }

    virtual ~BlockIterator() {

    }

    virtual void registerCleanup(const std::any &arg) {
        clearnups.push_back(arg);
    }

    virtual bool valid() const { return current < restarts; }

    virtual Status status() const { return s; }

    virtual std::string_view key() const {
        assert(valid());
        return k;
    }

    virtual std::string_view value() const {
        assert(valid());
        return v;
    }

    virtual void next() {
        assert(valid());
        parseNextKey();
    }

    virtual void prev() {
        assert(valid());

        // Scan backwards to a restart point before current_
        const uint32_t original = current;
        while (getRestartPoint(restartIndex) >= original) {
            if (restartIndex == 0) {
                // No more entries
                current = restarts;
                restartIndex = numRestarts;
                return;
            }
            restartIndex--;
        }

        seekToRestartPoint(restartIndex);
        do {
            // Loop until end of current entry hits the start of original entry
        } while (parseNextKey() && nextEntryOffset() < original);
    }

    virtual void seek(const std::string_view &target) {
        // Binary search in restart array to find the last restart point
        // with a key < target
        uint32_t left = 0;
        uint32_t right = numRestarts - 1;
        while (left < right) {
            uint32_t mid = (left + right + 1) / 2;
            uint32_t regionOffset = getRestartPoint(mid);
            uint32_t shared, nonShared, valueLength;
            const char *keyPtr = decodeEntry(data + regionOffset,
                                             data + restarts,
                                             &shared, &nonShared, &valueLength);
            if (keyPtr == nullptr || (shared != 0)) {
                corruptionError();
                return;
            }

            std::string_view midKey(keyPtr, nonShared);
            if (compare(midKey, target) < 0) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            }
        }

        // Linear search (within restart block) for first key >= target
        seekToRestartPoint(left);
        while (true) {
            if (!parseNextKey()) {
                return;
            }

            if (compare(k, target) >= 0) {
                return;
            }
        }
    }

    virtual void seekToFirst() {
        seekToRestartPoint(0);
        parseNextKey();
    }

    virtual void seekToLast() {
        seekToRestartPoint(numRestarts - 1);
        while (parseNextKey() && nextEntryOffset() < restarts) {
            // Keep skipping
        }
    }

    virtual void corruptionError() {
        current = restarts;
        restartIndex = numRestarts;
        s = Status::corruption("bad entry in block");
        k.clear();
        v = std::string_view();
    }

    virtual bool parseNextKey() {
        current = nextEntryOffset();
        const char *p = data + current;
        const char *limit = data + restarts;  // Restarts come right after data
        if (p >= limit) {
            // No more entries to return.  Mark as invalid.
            current = restarts;
            restartIndex = numRestarts;
            return false;
        }

        // Decode next entry
        uint32_t shared, nonShared, valueLength;
        p = decodeEntry(p, limit, &shared, &nonShared, &valueLength);
        if (p == nullptr || k.size() < shared) {
            corruptionError();
            return false;
        } else {
            k.resize(shared);
            k.append(p, nonShared);
            v = std::string_view(p + nonShared, valueLength);
            while (restartIndex + 1 < numRestarts &&
                   getRestartPoint(restartIndex + 1) < current) {
                ++restartIndex;
            }
            return true;
        }
    }
};

Block::Block(const BlockContents &contents)
        : data(contents.data.data()),
          size(contents.data.size()),
          owned(contents.heapAllocated) {
    if (size < sizeof(uint32_t)) {
        size = 0;  // Error marker
    } else {
        size_t maxRestartsAllowed = (size - sizeof(uint32_t)) / sizeof(uint32_t);
        if (numRestarts() > maxRestartsAllowed) {
            // The size is too small for NumRestarts()
            size = 0;
        } else {
            restartOffset = size - (1 + numRestarts()) * sizeof(uint32_t);
        }
    }
}

Block::~Block() {
    if (owned) {
        free((void *) data);
    }
}

std::shared_ptr <Iterator> Block::newIterator(const Comparator *cmp) {
    if (size < sizeof(uint32_t)) {
        return newErrorIterator(Status::corruption("bad block contents"));
    }

    const uint32_t restarts = numRestarts();
    if (restarts == 0) {
        return newEmptyIterator();
    } else {
        std::shared_ptr <Iterator> iter(new BlockIterator(cmp, data, restartOffset, restarts));
        return iter;
    }
}


