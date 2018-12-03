#include "logwriter.h"
#include "crc32c.h"
#include "coding.h"

static void initTypeCrc(uint32_t *typeCrc)
{
	for (int i = 0; i <= kMaxRecordType; i++)
	{
		char t = static_cast<char>(i);
		typeCrc[i] = crc32c::value(&t, 1);
	}
}

LogWriter::LogWriter(PosixWritableFile *dest)
	:dest(dest),
	blockOffset(0)
{
	initTypeCrc(typeCrc);
}

LogWriter::LogWriter(PosixWritableFile *dest, uint64_t destLength)
	:dest(dest),
	blockOffset(destLength % kBlockSize)
{
	initTypeCrc(typeCrc);
}

Status LogWriter::addRecord(const std::string_view &slice)
{
	const char *ptr = slice.data();
	size_t left = slice.size();
	// Fragment the record if necessary and emit it.  Note that if slice
	// is empty, we still want to iterate once to emit a single
	// zero-length record
	Status s;
	bool begin = true;
	do
	{
		const int leftover = kBlockSize - blockOffset;
		assert(leftover >= 0);
		if (leftover < kHeaderSize)
		{
			// Switch to a new block
			if (leftover > 0)
			{
				// Fill the trailer (literal below relies on kHeaderSize being 7)
				assert(kHeaderSize == 7);
				dest->append(std::string_view("\x00\x00\x00\x00\x00\x00", leftover));
			}
			blockOffset = 0;
		}

		// Invariant: we never leave < kHeaderSize bytes in a block.
		assert(kBlockSize - blockOffset - kHeaderSize >= 0);

		const size_t avail = kBlockSize - blockOffset - kHeaderSize;
		const size_t fragmentLength = (left < avail) ? left : avail;

		RecordType type;
		const bool end = (left == fragmentLength);
		if (begin && end)
		{
			type = kFullType;
		}
		else if (begin)
		{
			type = kFirstType;
		}
		else if (end)
		{
			type = kLastType;
		}
		else
		{
			type = kMiddleType;
		}

		s = emitPhysicalRecord(type, ptr, fragmentLength);
		ptr += fragmentLength;
		left -= fragmentLength;
		begin = false;
	} while (s.ok() && left > 0);
	return s;
}

Status LogWriter::emitPhysicalRecord(RecordType t, const char *ptr, size_t n)
{
	assert(n <= 0xffff);  // Must fit in two bytes
	assert(blockOffset + kHeaderSize + n <= kBlockSize);

	// Format the header
	char buf[kHeaderSize];
	buf[4] = static_cast<char>(n & 0xff);
	buf[5] = static_cast<char>(n >> 8);
	buf[6] = static_cast<char>(t);

	// Compute the crc of the record type and the payload.
	uint32_t crc = crc32c::extend(typeCrc[t], ptr, n);
	crc = crc32c::mask(crc); // Adjust for storage
	encodeFixed32(buf, crc);

	// Write the header and the payload
	Status s = dest->append(std::string_view(buf, kHeaderSize));
	if (s.ok())
	{
		s = dest->append(std::string_view(ptr, n));
		if (s.ok())
		{
			s = dest->flush();
		}
	}
	blockOffset += kHeaderSize + n;
	return s;
}
