#pragma once

#include <stdint.h>
#include <string_view>
#include "status.h"
#include "option.h"

class PosixSequentialFile;

class LogReporter {
public:
    ~LogReporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    void corruption(size_t bytes, const Status &status);

    PosixEnv *env;
    //Logger *infoLog;
    const char *fname;
    Status *status;
};

class LogReader {
public:
    // Create a reader that will return log records from "*file".
    // "*file" must remain live while this Reader is in use.
    //
    // If "reporter" is non-null, it is notified whenever some data is
    // dropped due to a detected corruption.  "*reporter" must remain
    // live while this Reader is in use.
    //
    // If "checksum" is true, verify checksums if available.
    //
    // The Reader will start reading at the first record located at physical
    // position >= initial_offset within the file.
    LogReader(PosixSequentialFile *file, LogReporter *reporter, bool checksum,
              uint64_t initialOffset);

    ~LogReader();

    // Read the next record into *record.  Returns true if read
    // successfully, false if we hit end of the input.  May use
    // "*scratch" as temporary storage.  The contents filled in *record
    // will only be valid until the next mutating operation on this
    // reader or the next mutation to *scratch.
    bool readRecord(std::string_view *record, std::string *scratch);

    // Returns the physical offset of the last record returned by ReadRecord.
    //
    // Undefined before the first call to ReadRecord.
    uint64_t getLastRecordOffset();

private:
    PosixSequentialFile *const file;
    LogReporter *const reporter;
    bool const checksum;
    char *const backingStore;
    std::string_view buffer;
    bool eof;   // Last Read() indicated EOF by returning < kBlockSize

    // Offset of the last record returned by ReadRecord.
    uint64_t lastRecordOffset;
    // Offset of the first location past the end of buffer_.
    uint64_t endofBufferOffset;

    // Offset at which to start looking for the first record to return
    uint64_t const initialOffset;

    // True if we are resynchronizing after a seek (initial_offset_ > 0). In
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
    bool skipToInitialBlock();

    // Return type, or one of the preceding special values
    unsigned int readPhysicalRecord(std::string_view *result);

    // Reports dropped bytes to the reporter.
    // buffer_ must be updated to remove the dropped bytes prior to invocation.
    void reportCorruption(uint64_t bytes, const char *reason);

    void reportDrop(uint64_t bytes, const Status &reason);

    // No copying allowed
    LogReader(const LogReader &);

    void operator=(const LogReader &);
};
