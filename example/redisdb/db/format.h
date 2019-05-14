#pragma once

#include <string>
#include <stdint.h>
#include <string_view>
#include <memory>
#include "status.h"
#include "option.h"

class Block;

class RandomAccessFile;

struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
public:
	BlockHandle();

	// The offset of the block in the file.
	uint64_t GetOffset() const { return offset; }

	void SetOffset(uint64_t offset) { this->offset = offset; }

	// The size of the stored block
	uint64_t GetSize() const { return size; }

	void SetSize(uint64_t size) { this->size = size; }

	void EncodeTo(std::string* dst) const;

	Status DecodeFrom(std::string_view* input);

	// Maximum encoding length of a BlockHandle
	enum {
		kMaxEncodedLength = 10 + 10
	};

private:
	uint64_t offset;
	uint64_t size;
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
class Footer {
public:
	Footer() {}

	// The block handle for the metaindex block of the table
	const BlockHandle& GetMetaindexHandle() const { return metaindexHandle; }

	void SetMetaindexHandle(const BlockHandle& h) { metaindexHandle = h; }

	// The block handle for the index block of the tabdle
	const BlockHandle& GetIndexHandle() const { return indexHandle; }

	void SetIndexHandle(const BlockHandle& h) { indexHandle = h; }

	void EncodeTo(std::string* dst) const;

	Status DecodeFrom(std::string_view* input);

	// Encoded length of a Footer.  Note that the serialization of a
	// Footer will always occupy exactly this many bytes.  It consists
	// of two block handles and a magic number.
	enum {
		kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8
	};

private:
	BlockHandle metaindexHandle;
	BlockHandle indexHandle;
};

// kTableMagicNumber was picked by running
// and taking the leading 64 bits.
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

struct BlockContents {
	std::string_view data; // Actual Contents of data
	bool cachable;        // True iff data can be cached
	bool heapAllocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
Status ReadBlock(const std::shared_ptr<RandomAccessFile>& file,
	const ReadOptions& options,
	const BlockHandle& handle,
	BlockContents* result);

// Implementation details follow.  Clients should ignore,

inline BlockHandle::BlockHandle()
	: offset(~static_cast<uint64_t>(0)),
	size(~static_cast<uint64_t>(0)) {

}
