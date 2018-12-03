#pragma once
#include <vector>
#include <stdint.h>
#include <string_view>
#include "option.h"

class BlockBuilder
{
public:
	BlockBuilder(const Options *options);
	
	// Reset the contents as if the BlockBuilder was just constructed.
	void reset();

	// REQUIRES: Finish() has not been called since the last call to Reset().
	// REQUIRES: key is larger than any previously added key
	void add(const std::string_view &key, const std::string_view &value);

	// Finish building the block and return a slice that refers to the
	// block contents.  The returned slice will remain valid for the
	// lifetime of this builder or until Reset() is called.
	std::string_view finish();

	// Returns an estimate of the current (uncompressed) size of the block
	// we are building.
	size_t currentSizeEstimate() const;

	// Return true iff no entries have been added since the last Reset()
	bool empty() const { return buffer.empty(); }

private:
	const Options *options;
	std::string buffer;      // Destination buffer
	std::vector<uint32_t> restarts;    // Restart points
	int counter;     // Number of entries emitted since restart
	bool finished;    // Has Finish() been called?
	std::string lastKey;

	// No copying allowed
	BlockBuilder(const BlockBuilder&);
	void operator=(const BlockBuilder&);
};
