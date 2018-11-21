#include "block.h"
#include <vector>
#include <algorithm>

inline uint32_t Block::numRestarts() const 
{
	assert(size >= sizeof(uint32_t));
	return decodeFixed32(data + size - sizeof(uint32_t));
}

Block::Block(const BlockContents &contents)
    : data(contents.data.data()),
      size(contents.data.size()),
      owned(contents.heapAllocated) 
{ 
	if (size < sizeof(uint32_t)) 
	{
		size = 0;  // Error marker
	} 
	else 
	{
		size_t maxRestartsAllowed = (size - sizeof(uint32_t)) / sizeof(uint32_t);
		if (numRestarts() > maxRestartsAllowed) 
		{
			// The size is too small for NumRestarts()
			size = 0;
		} 
		else 
		{
			restartOffset = size - (1 + numRestarts()) * sizeof(uint32_t);
		}
	}
}

Block::~Block() 
{
	if (owned) 
	{
		zfree((void*)data);
	}
}

std::shared_ptr<BlockIterator> Block::newIterator(const Comparator *comparator)
{
	if (size < sizeof(uint32_t))
	{
		std::shared_ptr<BlockIterator> iter(new BlockIterator(Status::corruption("bad block contents")));
		return iter;
	}

	const uint32_t restarts = numRestarts();
	if (restarts == 0)
	{
		std::shared_ptr<BlockIterator> iter(new BlockIterator(Status::OK()));
		return iter;
	}
	else
	{
		std::shared_ptr<BlockIterator> iter(new BlockIterator(comparator, data, restartOffset, restarts));
		return iter;
	}
}
