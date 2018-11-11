#pragma once
#include "option.h"
#include "status.h"
#include "posix.h"

class LogWriter
{
public:
	// Create a writer that will append data to "*dest".
	// "*dest" must be initially empty.
	// "*dest" must remain live while this Writer is in use.
	explicit LogWriter(PosixWritableFile *dest);

	// Create a writer that will append data to "*dest".
	// "*dest" must have initial length "dest_length".
	// "*dest" must remain live while this Writer is in use.
	LogWriter(PosixWritableFile *dest, uint64_t destLength);

	Status addRecord(const std::string_view &slice);

private:
	PosixWritableFile * dest;
	int blockOffset;       // Current offset in block
	// crc32c values for all supported record types.  These are
	// pre-computed to reduce the overhead of computing the crc of the
	// record type stored in the header.
	uint32_t typeCrc[kMaxRecordType + 1];

	Status emitPhysicalRecord(RecordType type, const char *ptr, size_t length);

	// No copying allowed
	LogWriter(const LogWriter&);
	void operator=(const LogWriter&);
};



