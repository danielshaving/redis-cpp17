#include "format.h"
#include "block.h"
#include "coding.h"
#include "crc32c.h"
#include "env.h"

void BlockHandle::EncodeTo(std::string* dst) const {
	// Sanity check that all fields have been Set
	assert(offset != ~static_cast<uint64_t>(0));
	assert(size != ~static_cast<uint64_t>(0));
	PutVarint64(dst, offset);
	PutVarint64(dst, size);
}

Status BlockHandle::DecodeFrom(std::string_view* input) {
	if (GetVarint64(input, &offset) &&
		GetVarint64(input, &size)) {
		return Status::OK();
	}
	else {
		return Status::Corruption("bad block handle");
	}
}

void Footer::EncodeTo(std::string* dst) const {
	const size_t originalSize = dst->size();
	metaindexHandle.EncodeTo(dst);
	indexHandle.EncodeTo(dst);
	dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
	PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
	PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
	assert(dst->size() == originalSize + kEncodedLength);
	(void)originalSize;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(std::string_view* input) {
	const char* magicPtr = input->data() + kEncodedLength - 8;
	const uint32_t magicLo = DecodeFixed32(magicPtr);
	const uint32_t magicHi = DecodeFixed32(magicPtr + 4);
	const uint64_t magic = ((static_cast<uint64_t>(magicHi)<< 32) |
		(static_cast<uint64_t>(magicLo)));
	if (magic != kTableMagicNumber) {
		return Status::Corruption("not an sstable (bad magic number)");
	}

	Status result = metaindexHandle.DecodeFrom(input);
	if (result.ok()) {
		result = indexHandle.DecodeFrom(input);
	}

	if (result.ok()) {
		// We Skip over any leftover data (just padding for now) in "input"
		const char* end = magicPtr + 8;
		*input = std::string_view(end, input->data() + input->size() - end);
	}
	return result;
}

Status ReadBlock(const std::shared_ptr<RandomAccessFile>& file,
	const ReadOptions& options,
	const BlockHandle& handle,
	BlockContents* result) {
	result->data = std::string_view();
	result->cachable = false;
	result->heapAllocated = false;

	// Read the block Contents as well as the type/crc footer.
	// See table_builder.cc for the code that built this structure.
	size_t n = static_cast<size_t>(handle.GetSize());
	char* buf = (char*)malloc(n + kBlockTrailerSize);
	std::string_view Contents;
	Status s = file->read(handle.GetOffset(), n + kBlockTrailerSize, &Contents, buf);
	if (!s.ok()) {
		free(buf);
		return s;
	}

	if (Contents.size() != n + kBlockTrailerSize) {
		free(buf);
		return Status::Corruption("truncated block read");
	}

	// Check the crc of the type and the block Contents
	const char* data = Contents.data();    // Pointer to where Read Put the data
	if (options.verifyChecksums) {
		const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
		const uint32_t actual = crc32c::Value(data, n + 1);
		if (actual != crc) {
			free(buf);
			s = Status::Corruption("block checksum mismatch");
			return s;
		}
	}

	switch (data[n]) {
	case kNoCompression:
		if (data != buf) {
			// File implementation gave us pointer to some other data.
			// Use it directly under the assumption that it will be live
			// while the file is Open.
			free(buf);
			result->data = std::string_view(data, n);
			result->heapAllocated = false;
			result->cachable = false;  // Do not double-cache
		}
		else {
			result->data = std::string_view(buf, n);
			result->heapAllocated = true;
			result->cachable = true;
		}

		// Ok
		break;
	case kSnappyCompression: {

	}
	default:
		free(buf);
		return Status::Corruption("bad block type");
	}
	return s;
}

