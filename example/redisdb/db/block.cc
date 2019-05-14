#include "block.h"
#include <vector>
#include <algorithm>

inline uint32_t Block::NumRestarts() const {
	assert(size >= sizeof(uint32_t));
	return DecodeFixed32(data + size - sizeof(uint32_t));
}

// Helper routine: decode the Next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not dereference past "limit".
// 
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static inline const char* decodeEntry(const char* p, const char* limit,
	uint32_t * shared,
	uint32_t * nonShared,
	uint32_t * valueLength) {
	if (limit - p< 3) return nullptr;
	*shared = reinterpret_cast<const unsigned char*>(p)[0];
	*nonShared = reinterpret_cast<const unsigned char*>(p)[1];
	*valueLength = reinterpret_cast<const unsigned char*>(p)[2];
	if ((*shared | *nonShared | *valueLength)< 128) {
		// Fast path: all three values are encoded in one byte each
		p += 3;
	}
	else {
		if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
		if ((p = GetVarint32Ptr(p, limit, nonShared)) == nullptr) return nullptr;
		if ((p = GetVarint32Ptr(p, limit, valueLength)) == nullptr) return nullptr;
	}

	if (static_cast<uint32_t>(limit - p)< (*nonShared + *valueLength)) {
		return nullptr;
	}
	return p;
}

class BlockIterator : public Iterator {
private:
	const Comparator* const comparator;
	const char* const data;      // underlying block Contents
	uint32_t const restarts;     // Offset of restart array (list of fixed32)
	uint32_t const NumRestarts; // Number of uint32_t entries in restart array

	// current_ is offset in data_ of current entry.  >= restarts_ if !Valid
	uint32_t current;
	uint32_t restartIndex;  // Index of restart block in which current_ falls
	std::string k;
	std::string_view v;
	Status s;
	std::list<std::any> clearnups;

	inline int compare(const std::string_view& a, const std::string_view& b) const {
		return comparator->Compare(a, b);
	}

	// Return the offset in data_ just past the end of the current entry.
	inline uint32_t nextEntryOffset() const {
		return (v.data() + v.size()) - data;
	}

	uint32_t getRestartPoint(uint32_t index) {
		assert(index< NumRestarts);
		return DecodeFixed32(data + restarts + index * sizeof(uint32_t));
	}

	void seekToRestartPoint(uint32_t index) {
		k.clear();
		restartIndex = index;
		// current_ will be fixed by ParseNextKey();

		// ParseNextKey() starts at the end of value_, so Set value_ accordingly
		uint32_t offset = getRestartPoint(index);
		v = std::string_view(data + offset, 0);
	}

public:
	BlockIterator(const Comparator* comparator,
		const char* data,
		uint32_t restarts,
		uint32_t NumRestarts)
		: comparator(comparator),
		data(data),
		restarts(restarts),
		NumRestarts(NumRestarts),
		current(restarts),
		restartIndex(NumRestarts) {

		assert(NumRestarts > 0);
	}

	virtual ~BlockIterator() {

	}

	virtual void RegisterCleanup(const std::any& arg) {
		clearnups.push_back(arg);
	}

	virtual bool Valid() const { return current< restarts; }

	virtual Status status() const { return s; }

	virtual std::string_view key() const {
		assert(Valid());
		return k;
	}

	virtual std::string_view value() const {
		assert(Valid());
		return v;
	}

	virtual void Next() {
		assert(Valid());
		parseNextKey();
	}

	virtual void Prev() {
		assert(Valid());

		// Scan backwards to a restart point before current_
		const uint32_t original = current;
		while (getRestartPoint(restartIndex) >= original) {
			if (restartIndex == 0) {
				// No more entries
				current = restarts;
				restartIndex = NumRestarts;
				return;
			}
			restartIndex--;
		}

		seekToRestartPoint(restartIndex);
		do {
			// Loop until end of current entry hits the start of original entry
		} while (parseNextKey() && nextEntryOffset()< original);
	}

	virtual void Seek(const std::string_view& target) {
		// Binary search in restart array to find the last restart point
		// with a key< target
		uint32_t left = 0;
		uint32_t right = NumRestarts - 1;
		while (left< right) {
			uint32_t mid = (left + right + 1) / 2;
			uint32_t regionOffset = getRestartPoint(mid);
			uint32_t shared, nonShared, valueLength;
			const char* keyPtr = decodeEntry(data + regionOffset,
				data + restarts,
				&shared, &nonShared, &valueLength);
			if (keyPtr == nullptr || (shared != 0)) {
				corruptionError();
				return;
			}

			std::string_view midKey(keyPtr, nonShared);
			if (compare(midKey, target)< 0) {
				// Key at "mid" is smaller than "target".  Therefore all
				// blocks before "mid" are uninteresting.
				left = mid;
			}
			else {
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

	virtual void SeekToFirst() {
		seekToRestartPoint(0);
		parseNextKey();
	}

	virtual void SeekToLast() {
		seekToRestartPoint(NumRestarts - 1);
		while (parseNextKey() && nextEntryOffset()< restarts) {
			// Keep skipping
		}
	}

	virtual void corruptionError() {
		current = restarts;
		restartIndex = NumRestarts;
		s = Status::Corruption("bad entry in block");
		k.clear();
		v = std::string_view();
	}

	virtual bool parseNextKey() {
		current = nextEntryOffset();
		const char* p = data + current;
		const char* limit = data + restarts;  // Restarts come right after data
		if (p >= limit) {
			// No more entries to return.  Mark as invalid.
			current = restarts;
			restartIndex = NumRestarts;
			return false;
		}

		// Decode Next entry
		uint32_t shared, nonShared, valueLength;
		p = decodeEntry(p, limit, &shared, &nonShared, &valueLength);
		if (p == nullptr || k.size()< shared) {
			corruptionError();
			return false;
		}
		else {
			k.resize(shared);
			k.append(p, nonShared);
			v = std::string_view(p + nonShared, valueLength);
			while (restartIndex + 1< NumRestarts &&
				getRestartPoint(restartIndex + 1)< current) {
				++restartIndex;
			}
			return true;
		}
	}
};

Block::Block(const BlockContents& Contents)
	: data(Contents.data.data()),
	size(Contents.data.size()),
	owned(Contents.heapAllocated) {
	if (size< sizeof(uint32_t)) {
		size = 0;  // Error marker
	}
	else {
		size_t maxRestartsAllowed = (size - sizeof(uint32_t)) / sizeof(uint32_t);
		if (NumRestarts() > maxRestartsAllowed) {
			// The size is too small for NumRestarts()
			size = 0;
		}
		else {
			restartOffset = size - (1 + NumRestarts()) * sizeof(uint32_t);
		}
	}
}

Block::~Block() {
	if (owned) {
		free((void*)data);
	}
}

std::shared_ptr<Iterator> Block::NewIterator(const Comparator* cmp) {
	if (size< sizeof(uint32_t)) {
		return NewErrorIterator(Status::Corruption("bad block Contents"));
	}

	const uint32_t restarts = NumRestarts();
	if (restarts == 0) {
		return NewEmptyIterator();
	}
	else {
		std::shared_ptr<Iterator> iter(new BlockIterator(cmp, data, restartOffset, restarts));
		return iter;
	}
}


