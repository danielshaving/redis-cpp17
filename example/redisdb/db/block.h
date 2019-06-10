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
	// Initialize the block with the specified Contents.
	Block(const BlockContents& contents);

	~Block();

	size_t GetSize() const { return size; }

	std::shared_ptr<Iterator> NewIterator(const Comparator* comparator);

private:
	uint32_t numrestarts() const;

	const char* data;
	size_t size;
	uint32_t restartoffset;     // Offset in data_ of restart array
	bool owned;                  // Block owns data_[]

	// No copying allowed
	Block(const Block&);

	void operator=(const Block&);
};
