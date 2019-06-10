#include "logwriter.h"
#include "crc32c.h"
#include "coding.h"

static void initTypeCrc(uint32_t* typecrc) {
	for (int i = 0; i<= kMaxRecordType; i++) {
		char t = static_cast<char>(i);
		typecrc[i] = crc32c::Value(&t, 1);
	}
}

LogWriter::LogWriter(WritableFile* dest)
	: dest(dest),
	blockoffset(0) {
	initTypeCrc(typecrc);
}

LogWriter::LogWriter(WritableFile* dest, uint64_t destlength)
	: dest(dest),
	blockoffset(destlength % kBlockSize) {
	initTypeCrc(typecrc);
}

Status LogWriter::AddRecord(const std::string_view& slice) {
	const char* ptr = slice.data();
	size_t left = slice.size();
	// Fragment the record if necessary and emit it.  Note that if slice
	// is empty, we still want to Iterate once to emit a single
	// zero-length record
	Status s;
	bool begin = true;
	do {
		const int leftover = kBlockSize - blockoffset;
		assert(leftover >= 0);
		if (leftover< kHeaderSize) {
			// Switch to a new block
			if (leftover > 0) {
				// Fill the trailer (literal below relies on kHeaderSize being 7)
				assert(kHeaderSize == 7);
				dest->append(std::string_view("\x00\x00\x00\x00\x00\x00", leftover));
			}
			blockoffset = 0;
		}

		// Invariant: we never leave< kHeaderSize bytes in a block.
		assert(kBlockSize - blockoffset - kHeaderSize >= 0);

		const size_t avail = kBlockSize - blockoffset - kHeaderSize;
		const size_t fragmentlength = (left< avail) ? left : avail;

		RecordType type;
		const bool end = (left == fragmentlength);
		if (begin && end) {
			type = kFullType;
		}
		else if (begin) {
			type = kFirstType;
		}
		else if (end) {
			type = kLastType;
		}
		else {
			type = kMiddleType;
		}

		s = EmitPhysicalRecord(type, ptr, fragmentlength);
		ptr += fragmentlength;
		left -= fragmentlength;
		begin = false;
	} while (s.ok() && left > 0);
	return s;
}

Status LogWriter::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
	assert(n<= 0xffff);  // Must fit in two bytes
	assert(blockoffset + kHeaderSize + n <= kBlockSize);

	// Format the header
	char buf[kHeaderSize];
	buf[4] = static_cast<char>(n & 0xff);
	buf[5] = static_cast<char>(n >> 8);
	buf[6] = static_cast<char>(t);

	// Compute the crc of the record type and the payload.
	uint32_t crc = crc32c::Extend(typecrc[t], ptr, n);
	crc = crc32c::Mask(crc); // Adjust for storage
	EncodeFixed32(buf, crc);

	// Write the header and the payload
	Status s = dest->append(std::string_view(buf, kHeaderSize));
	if (s.ok()) {
		s = dest->append(std::string_view(ptr, n));
		if (s.ok()) {
			s = dest->flush();
		}
	}
	blockoffset += kHeaderSize + n;
	return s;
}
