#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string_view>
#include <any>
#include <list>
#include <memory>
#include <assert.h>
#include "coding.h"
#include "format.h"
#include "dbformat.h"
#include "iterator.h"

class Block {
public:
	// Initialize the block with the specified contents.
	Block(const BlockContents& contents);

	~Block();

	size_t getSize() const { return size; }

	std::shared_ptr<Iterator> newIterator(const Comparator* comparator);

private:
	uint32_t numRestarts() const;

	const char* data;
	size_t size;
	uint32_t restartOffset;     // Offset in data_ of restart array
	bool owned;                  // Block owns data_[]

	// No copying allowed
	Block(const Block&);

	void operator=(const Block&);
};
