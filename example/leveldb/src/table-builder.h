#pragma once

#include <stdint.h>
#include "status.h"
#include "option.h"
#include "posix.h"

class BlockBuilder;
class BlockHandle;

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

	// Indicate that the contents of this builder should be abandoned.  Stops
   // using the file passed to the constructor after this function returns.
   // If the caller is not going to call Finish(), it must call Abandon()
   // before destroying this builder.
   // REQUIRES: Finish(), Abandon() have not been called

	void abandon();

	// Number of calls to Add() so far.
	uint64_t numEntries() const;

	// Size of the file generated so far.  If invoked after a successful
	// Finish() call, returns the size of the final generated file.

	uint64_t fileSize() const;

private:
	void writeBlock(BlockBuilder *block, BlockHandle *handle);
	void writeRawBlock(const std::string_view &blockContents,
		CompressionType type, BlockHandle *handle);
	struct Rep;
	std::shared_ptr<Rep> rep;
};