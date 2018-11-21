#include "format.h"
#include "block.h"
#include "coding.h"
#include "crc32c.h"
#include "zmalloc.h"

void BlockHandle::encodeTo(std::string *dst) const
{
	// Sanity check that all fields have been set
	assert(offset != ~static_cast<uint64_t>(0));
	assert(size != ~static_cast<uint64_t>(0));
	putVarint64(dst, offset);
	putVarint64(dst, size);
}

Status BlockHandle::decodeFrom(std::string_view *input)
{
	if (getVarint64(input, &offset) &&
		getVarint64(input, &size))
	{
		Status s;
		return s;
	}
	else
	{
		return Status::corruption("bad block handle");
	}
}

void Footer::encodeTo(std::string *dst) const
{
	const size_t originalSize = dst->size();
	metaindexHandle.encodeTo(dst);
	indexHandle.encodeTo(dst);
	dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
	putFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
	putFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
	assert(dst->size() == originalSize + kEncodedLength);
	(void)originalSize;  // Disable unused variable warning.
}

Status Footer::decodeFrom(std::string_view *input)
{
	const char *magicPtr = input->data() + kEncodedLength - 8;
	const uint32_t magicLo = decodeFixed32(magicPtr);
	const uint32_t magicHi = decodeFixed32(magicPtr + 4);
	const uint64_t magic = ((static_cast<uint64_t>(magicHi) << 32) |
		(static_cast<uint64_t>(magicLo)));
	if (magic != kTableMagicNumber)
	{
		return Status::corruption("not an sstable (bad magic number)");
	}

	Status result = metaindexHandle.decodeFrom(input);
	if (result.ok())
	{
		result = indexHandle.decodeFrom(input);
	}

	if (result.ok())
	{
		// We skip over any leftover data (just padding for now) in "input"
		const char *end = magicPtr + 8;
		*input = std::string_view(end, input->data() + input->size() - end);
	}
	return result;
}

Status readBlock(const std::shared_ptr<RandomAccessFile> &file,
	const ReadOptions &options,
	const BlockHandle &handle,
	BlockContents *result)
{
	result->data = std::string_view();
	result->cachable = false;
	result->heapAllocated = false;

	// Read the block contents as well as the type/crc footer.
	// See table_builder.cc for the code that built this structure.
	size_t n = static_cast<size_t>(handle.getSize());
	char *buf = (char*)zmalloc(n + kBlockTrailerSize);
	std::string_view contents;
	Status s = file->read(handle.getOffset(), n + kBlockTrailerSize, &contents, buf);
	if (!s.ok())
	{
		zfree(buf);
		return s;
	}

	if (contents.size() != n + kBlockTrailerSize)
	{
		zfree(buf);
		return Status::corruption("truncated block read");
	}

	// Check the crc of the type and the block contents
	const char *data = contents.data();    // Pointer to where Read put the data
	if (options.verifyChecksums)
	{
		const uint32_t crc = crc32c::unmask(decodeFixed32(data + n + 1));
		const uint32_t actual = crc32c::value(data, n + 1);
		if (actual != crc)
		{
			zfree(buf);
			s = Status::corruption("block checksum mismatch");
			return s;
		}
	}

	switch (data[n])
	{
	case kNoCompression:
		if (data != buf)
		{
			// File implementation gave us pointer to some other data.
			// Use it directly under the assumption that it will be live
			// while the file is open.
			zfree(buf);
			result->data = std::string_view(data, n);
			result->heapAllocated = false;
			result->cachable = false;  // Do not double-cache
		}
		else
		{
			result->data = std::string_view(buf, n);
			result->heapAllocated = true;
			result->cachable = true;
		}

		// Ok
		break;
	case kSnappyCompression:
	{

	}
	default:
		zfree(buf);
		return Status::corruption("bad block type");
	}
	return s;
}

