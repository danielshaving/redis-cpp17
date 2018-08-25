#include "arena.h"

static const int kBlockSize = 4096;

Arena::Arena()
	:memoryUsage(0),
	allocPtr(nullptr),
	allocBytesRemaining(0)
{

}

Arena::~Arena()
{
	for (size_t i = 0; i < blocks.size(); i++)
	{
		delete[] blocks[i];
	}
}

char *Arena::allocateFallback(size_t bytes)
{
	if (bytes > kBlockSize / 4)
	{
		// Object is more than a quarter of our block size.  Allocate it separately
		// to avoid wasting too much space in leftover bytes.
		char *result = allocateNewBlock(bytes);
		return result;
	}

	// We waste the remaining space in the current block.
	allocPtr = allocateNewBlock(kBlockSize);
	allocBytesRemaining = kBlockSize;

	char *result = allocPtr;
	allocPtr += bytes;
	allocBytesRemaining -= bytes;
	return result;
}

char *Arena::allocateAligned(size_t bytes)
{
	const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
	assert((align & (align - 1)) == 0);   // Pointer size should be a power of 2
	size_t currentmod = reinterpret_cast<uintptr_t>(allocPtr) & (align - 1);
	size_t slop = (currentmod == 0 ? 0 : align - currentmod);
	size_t needed = bytes + slop;
	char *result;
	if (needed <= allocBytesRemaining)
	{
		result = allocPtr + slop;
		allocPtr += needed;
		allocBytesRemaining -= needed;
	}
	else
	{
		// AllocateFallback always returned aligned memory
		result = allocateFallback(bytes);
	}
	assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
	return result;
}

char *Arena::allocateNewBlock(size_t blockBytes)
{
	char *result = new char[blockBytes];
	blocks.push_back(result);
	memoryUsage += blockBytes + sizeof(char*);
	return result;
}
