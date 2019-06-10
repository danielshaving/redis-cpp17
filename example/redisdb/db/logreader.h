#pragma once

#include <stdint.h>
#include <string_view>
#include <stdio.h>
#include "status.h"
#include "option.h"
#include "env.h"

class SequentialFile;

class LogReporter {
public:
	~LogReporter();

	// Some Corruption was detected.  "size" is the approximate number
	// of bytes dropped due to the Corruption.
	void Corruption(size_t bytes, const Status& status);

	//Logger *infoLog;
	const char* fname;
	Status* status;
};

class LogReader {
public:
	// Create a reader that will return log records from "*file".
	// "*file" must remain live while this Reader is in use.
	//
	// If "reporter" is non-null, it is notified whenever some data is
	// dropped due to a detected Corruption.  "*reporter" must remain
	// live while this Reader is in use.
	//
	// If "checksum" is true, verify checksums if available.
	//
	// The Reader will start reading at the first record located at physical
	// position >= initial_offset within the file.
	LogReader(const std::shared_ptr<SequentialFile>& file, LogReporter* reporter, bool checksum,
		uint64_t initialoffset);

	~LogReader();

	// Read the Next record into *record.  Returns true if read
	// successfully, false if we hit end of the input.  May use
	// "*scratch" as temporary storage.  The Contents filled in *record
	// will only be Valid until the Next mutating operation on this
	// reader or the Next mutation to *scratch.
	bool ReadRecord(std::string_view* record, std::string* scratch);

	// Returns the physical offset of the last record returned by ReadRecord.
	//
	// Undefined before the first call to ReadRecord.
	uint64_t GetLastRecordOffset();

private:
	std::shared_ptr<SequentialFile> file;
	LogReporter* const reporter;
	bool const checksum;
	char* const backingstore;
	std::string_view buffer;
	bool eof;   // Last Read() indicated EOF by returning< kBlockSize

	// Offset of the last record returned by ReadRecord.
	uint64_t lastrecordoffset;
	// Offset of the first location past the end of buffer_.
	uint64_t endofbufferoffset;

	// Offset at which to start looking for the first record to return
	uint64_t const initialoffset;

	// True if we are resynchronizing after a Seek (initial_offset_ > 0). In
	// particular, a run of kMiddleType and kLastType records can be silently
	// skipped in this mode
	bool resyncing;

	// Extend record types with the following special values
	enum {
		kEof = kMaxRecordType + 1,
		// Returned whenever we find an invalid physical record.
		// Currently there are three situations in which this happens:
		// * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
		// * The record is a 0-length record (No drop is reported)
		// * The record is below constructor's initial_offset (No drop is reported)
		kBadRecord = kMaxRecordType + 2
	};

	// Skips all blocks that are completely before "initial_offset_".
	//
	// Returns true on success. Handles reporting.
	bool SkipToInitialBlock();

	// Return type, or one of the preceding special values
	unsigned int ReadPhysicalRecord(std::string_view* result);

	// Reports dropped bytes to the reporter.
	// buffer_ must be updated to remove the dropped bytes prior to invocation.
	void ReportCorruption(uint64_t bytes, const char* reason);

	void ReportDrop(uint64_t bytes, const Status& reason);

	// No copying allowed
	LogReader(const LogReader&);

	void operator=(const LogReader&);
};
