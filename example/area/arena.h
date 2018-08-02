#pragma once
#include "all.h"
#include "util.h"
class Arena
{
public:
	Arena();
	~Arena();

	// Return a pointer to a newly allocated memory block of "bytes" bytes.
	char *allocate(size_t bytes);

	// Allocate memory with the normal alignment guarantees provided by malloc
	char *allocateAligned(size_t bytes);

	size_t getMemoryUsage() { return memoryUsage; }

private:
	char *allocateFallback(size_t bytes);
	char *allocateNewBlock(size_t block_bytes);

	// Allocation state
	char *allocPtr;
	size_t allocBytesRemaining;

	// Array of new[] allocated memory blocks
	std::vector<char*> blocks;

	// Total memory usage of the arena.
	std::atomic<size_t> memoryUsage;

	// No copying allowed
	Arena(const Arena&);
	void operator=(const Arena&);
};

inline char *Arena::allocate(size_t bytes)
{
	// The semantics of what to return are a bit messy if we allow
	// 0-byte allocations, so we disallow them here (we don't need
	// them for our internal use).
	assert(bytes > 0);
	if (bytes <= allocBytesRemaining)
	{
		char *result = allocPtr;
		allocPtr += bytes;
		allocBytesRemaining -= bytes;
		return result;
	}
	return allocateFallback(bytes);
}
