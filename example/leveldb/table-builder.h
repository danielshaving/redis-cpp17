//
// Created by zhanghao on 2018/7/26.
//

#pragma once

#include <stdint.h>
#include "status.h"
#include "option.h"
#include "posix.h"

class TableBuilder
{
public:
	// Create a builder that will store the contents of the table it is
	// building in *file.  Does not close the file.  It is up to the
	// caller to close the file after calling Finish().
	TableBuilder(const Options &options, PosixWritableFile *file);

	TableBuilder(const TableBuilder&) = delete;
	void operator=(const TableBuilder&) = delete;

	~TableBuilder();

	// Add key,value to the table being constructed.
	// REQUIRES: key is after any previously added key according to comparator.
	// REQUIRES: Finish(), Abandon() have not been called
	void add(const std::string_view &key, const std::string_view &value);

	// Finish building the table.  Stops using the file passed to the
	// constructor after this function returns.
	// REQUIRES: Finish(), Abandon() have not been called
	Status finish();

	// Advanced operation: flush any buffered key/value pairs to file.
	// Can be used to ensure that two adjacent entries never live in
	// the same data block.  Most clients should not need to use this method.
	// REQUIRES: Finish(), Abandon() have not been called
	void flush();

private:
	void writeBlock(BlockBuilder *block, BlockHandle *handle)
		struct Rep;
	std::shared_ptr<Rep> rep;
};