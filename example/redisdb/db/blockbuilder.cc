#include "coding.h"
#include <algorithm>
#include <assert.h>
#include "blockbuilder.h"

BlockBuilder::BlockBuilder(const Options* options)
	: options(options),
	restarts(),
	counter(0),
	finished(false) {
	assert(options->blockrestartinterval >= 1);
	restarts.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::reset() {
	buffer.clear();
	restarts.clear();
	restarts.push_back(0);       // First restart point is at offset 0
	counter = 0;
	finished = false;
	lastkey.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
	return (buffer.size() +                        // Raw data buffer
		restarts.size() * sizeof(uint32_t) +   // Restart array
		sizeof(uint32_t));                      // Restart array length
}

std::string_view BlockBuilder::Finish() {
	// Append restart array
	for (size_t i = 0; i< restarts.size(); i++) {
		PutFixed32(&buffer, restarts[i]);
	}

	PutFixed32(&buffer, restarts.size());
	finished = true;
	return std::string_view(buffer);
}

void BlockBuilder::Add(const std::string_view& key, const std::string_view& value) {
	std::string_view lastKeyPiece(lastkey);
	assert(!finished);
	assert(counter<= options->blockrestartinterval);
	assert(buffer.empty() // No values yet?
		|| options->comparator->Compare(key, lastKeyPiece) > 0);
	size_t shared = 0;
	if (counter< options->blockrestartinterval) {
		// See how much sharing to do with previous string
		size_t minLength;
		if (lastKeyPiece.size()< key.size()) {
			minLength = lastKeyPiece.size();
		}
		else {
			minLength = key.size();
		}

		while ((shared< minLength) && (lastKeyPiece[shared] == key[shared])) {
			shared++;
		}
	}
	else {
		// Restart compression
		restarts.push_back(buffer.size());
		counter = 0;
	}
	const size_t nonShared = key.size() - shared;

	// Add "<shared><non_shared><value_size>" to buffer_
	PutVarint32(&buffer, shared);
	PutVarint32(&buffer, nonShared);
	PutVarint32(&buffer, value.size());

	// Add string delta to buffer_ followed by value
	buffer.append(key.data() + shared, nonShared);
	buffer.append(value.data(), value.size());

	// Update state
	lastkey.resize(shared);
	lastkey.append(key.data() + shared, nonShared);
	assert(std::string_view(lastkey) == key);
	counter++;
}
