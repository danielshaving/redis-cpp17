#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string_view>
#include <any>
#include <list>
#include <memory>
#include <assert.h>
#include "coding.h"
#include "zmalloc.h"
#include "format.h"

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
                                      uint32_t *valueLength)
{
	if (limit - p < 3) return nullptr;
	*shared = reinterpret_cast<const unsigned char*>(p)[0];
	*nonShared = reinterpret_cast<const unsigned char*>(p)[1];
	*valueLength = reinterpret_cast<const unsigned char*>(p)[2];
	if ((*shared | *nonShared | *valueLength) < 128)
	{
		// Fast path: all three values are encoded in one byte each
		p += 3;
	}
	else
	{
		if ((p = getVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
		if ((p = getVarint32Ptr(p, limit, nonShared)) == nullptr) return nullptr;
		if ((p = getVarint32Ptr(p, limit, valueLength)) == nullptr) return nullptr;
	}

	if (static_cast<uint32_t>(limit - p) < (*nonShared + *valueLength))
	{
		return nullptr;
	}
	return p;
}

struct BlockContents;
class BlockIterator
{
private:
	const InternalKeyComparator *const comparator;
	const char *const data;      // underlying block contents
	uint32_t const restarts;     // Offset of restart array (list of fixed32)
	uint32_t const numRestarts; // Number of uint32_t entries in restart array
	
	// current_ is offset in data_ of current entry.  >= restarts_ if !Valid
	uint32_t current;
	uint32_t restartIndex;  // Index of restart block in which current_ falls
	std::string key;
	std::string_view value;
	Status status;
	
	inline int compare(const std::string_view &a, const std::string_view &b) const 
	{
		return comparator->compare(a, b);
	}

	// Return the offset in data_ just past the end of the current entry.
	inline uint32_t nextEntryOffset() const 
	{
		return (value.data() + value.size()) - data;
	}

	uint32_t getRestartPoint(uint32_t index) 
	{
		assert(index < numRestarts);
		return decodeFixed32(data + restarts + index * sizeof(uint32_t));
	}

	void seekToRestartPoint(uint32_t index) 
	{
		key.clear();
		restartIndex = index;
		// current_ will be fixed by ParseNextKey();

		// ParseNextKey() starts at the end of value_, so set value_ accordingly
		uint32_t offset = getRestartPoint(index);
		value = std::string_view(data + offset, 0);
	}

public:
	BlockIterator(const Status &s)
	:status(s),
	 comparator(nullptr),
	 data(nullptr),
	 restarts(0),
	 numRestarts(0)
	{

	}

	BlockIterator(const InternalKeyComparator *comparator,
	       const char *data,
	       uint32_t restarts,
	       uint32_t numRestarts)
	      : comparator(comparator),
	        data(data),
	        restarts(restarts),
	        numRestarts(numRestarts),
	        current(restarts),
	        restartIndex(numRestarts)
	{
	    assert(numRestarts > 0);
	}

	bool valid() const { return current < restarts; }
	Status getStatus() const { return status; }
	std::string_view getKey() const
	{
		assert(valid());
		return key;
	}

	std::string_view getValue() const
	{
		assert(valid());
		return value;
	}

	void next()
	{
		assert(valid());
		parseNextKey();
	}

	void prev()
	{
		assert(valid());

		// Scan backwards to a restart point before current_
		const uint32_t original = current;
		while (getRestartPoint(restartIndex) >= original)
		{
			if (restartIndex == 0)
			{
				// No more entries
				current = restarts;
				restartIndex = numRestarts;
				return;
			}
			restartIndex--;
		}

		seekToRestartPoint(restartIndex);
		do
		{
		// Loop until end of current entry hits the start of original entry
		} while (parseNextKey() && nextEntryOffset() < original);
	}

	void seek(const std::string_view &target)
	{
		// Binary search in restart array to find the last restart point
		// with a key < target
		uint32_t left = 0;
		uint32_t right = numRestarts - 1;
		while (left < right)
		{
			uint32_t mid = (left + right + 1) / 2;
			uint32_t regionOffset = getRestartPoint(mid);
			uint32_t shared, nonShared, valueLength;
			const char *keyPtr = decodeEntry(data + regionOffset,
											data + restarts,
											&shared, &nonShared, &valueLength);
			if (keyPtr == nullptr || (shared != 0))
			{
				corruptionError();
				return;
			}

			std::string_view midKey(keyPtr, nonShared);
			if (compare(midKey, target) < 0)
			{
				// Key at "mid" is smaller than "target".  Therefore all
				// blocks before "mid" are uninteresting.
				left = mid;
			}
			else
			{
				// Key at "mid" is >= "target".  Therefore all blocks at or
				// after "mid" are uninteresting.
				right = mid - 1;
			}
		}

		// Linear search (within restart block) for first key >= target
		seekToRestartPoint(left);
		while (true)
		{
			if (!parseNextKey())
			{
				return;
			}

			if (compare(key, target) >= 0)
			{
				return;
			}
		}
	}

	void seekToFirst()
	{
		seekToRestartPoint(0);
		parseNextKey();
	}

	void seekToLast()
	{
		seekToRestartPoint(numRestarts - 1);
		while (parseNextKey() && nextEntryOffset() < restarts)
		{
		  // Keep skipping
		}
	}

	void corruptionError()
	{
		current = restarts;
		restartIndex = numRestarts;
		status = Status::corruption("bad entry in block");
		key.clear();
		value = std::string_view();
	}

	bool parseNextKey()
	{
		current = nextEntryOffset();
		const char *p = data + current;
		const char *limit = data + restarts;  // Restarts come right after data
		if (p >= limit)
		{
			// No more entries to return.  Mark as invalid.
			current = restarts;
			restartIndex = numRestarts;
			return false;
		}

		// Decode next entry
		uint32_t shared, nonShared, valueLength;
		p = decodeEntry(p, limit, &shared, &nonShared, &valueLength);
		if (p == nullptr || key.size() < shared)
		{
			corruptionError();
			return false;
		}
		else
		{
			key.resize(shared);
			key.append(p, nonShared);
			value = std::string_view(p + nonShared, valueLength);
			while (restartIndex + 1 < numRestarts &&
				 getRestartPoint(restartIndex + 1) < current)
			{
				++restartIndex;
			}
			return true;
		}
	 }
};

class Block 
{
public:
	 // Initialize the block with the specified contents.
	Block(const BlockContents &contents);

	~Block();

	size_t getSize() const { return size; }
	std::shared_ptr<BlockIterator> newIterator(const InternalKeyComparator *comparator);
  
private:
	 uint32_t numRestarts() const;

	const char *data;
	size_t size;
	uint32_t restartOffset;     // Offset in data_ of restart array
	bool owned;                  // Block owns data_[]

	// No copying allowed
	Block(const Block&);
	void operator=(const Block&);
};
