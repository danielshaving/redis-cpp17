#include "logreader.h"
#include "coding.h"
#include "crc32c.h"

LogReporter::~LogReporter() {

}

void LogReporter::Corruption(size_t bytes, const Status& status) {

}

LogReader::LogReader(const std::shared_ptr<SequentialFile>& file, LogReporter* reporter, bool checksum,
	uint64_t initialOffset)
	: file(file),
	reporter(reporter),
	checksum(checksum),
	backingStore((char*)malloc(kBlockSize)),
	buffer(),
	eof(false),
	lastRecordOffset(0),
	endofBufferOffset(0),
	initialOffset(initialOffset),
	resyncing(initialOffset > 0) {

}

LogReader::~LogReader() {
	free(backingStore);
}

bool LogReader::SkipToInitialBlock() {
	const size_t offsetInBlock = initialOffset % kBlockSize;
	uint64_t blockStartLocation = initialOffset - offsetInBlock;

	// Don't search a block if we'd be in the trailer
	if (offsetInBlock > kBlockSize - 6) {
		blockStartLocation += kBlockSize;
	}

	endofBufferOffset = blockStartLocation;

	// Skip to start of first block that can contain the initial record
	if (blockStartLocation > 0) {
		Status skipStatus = file->Skip(blockStartLocation);
		if (!skipStatus.ok()) {
			ReportDrop(blockStartLocation, skipStatus);
			return false;
		}
	}
	return true;
}

bool LogReader::ReadRecord(std::string_view* record, std::string* scratch) {
	if (lastRecordOffset< initialOffset) {
		if (!SkipToInitialBlock()) {
			return false;
		}
	}

	scratch->clear();
	*record = "";
	bool inFragmentedRecord = false;
	// Record offset of the logical record that we're reading
	// 0 is a dummy value to make compilers happy
	uint64_t prospectiveRecordOffset = 0;

	std::string_view fragment;
	while (true) {
		const unsigned int recordType = ReadPhysicalRecord(&fragment);

		// ReadPhysicalRecord may have only had an empty trailer remaining in its
		// internal buffer. Calculate the offset of the Next physical record now
		// that it has returned, properly accounting for its header size.
		uint64_t physicalRecordOffset = endofBufferOffset - buffer.size() - kHeaderSize - fragment.size();

		if (resyncing) {
			if (recordType == kMiddleType) {
				continue;
			}
			else if (recordType == kLastType) {
				resyncing = false;
				continue;
			}
			else {
				resyncing = false;
			}
		}

		switch (recordType) {
		case kFullType:
			if (inFragmentedRecord) {
				// Handle bug in earlier versions of log::Writer where
				// it could emit an empty kFirstType record at the tail end
				// of a block followed by a kFullType or kFirstType record
				// at the beginning of the Next block.
				if (!scratch->empty()) {
					ReportCorruption(scratch->size(), "partial record without end(1)");
				}
			}
			prospectiveRecordOffset = physicalRecordOffset;
			scratch->clear();
			*record = fragment;
			lastRecordOffset = prospectiveRecordOffset;
			return true;

		case kFirstType:
			if (inFragmentedRecord) {
				// Handle bug in earlier versions of log::Writer where
				// it could emit an empty kFirstType record at the tail end
				// of a block followed by a kFullType or kFirstType record
				// at the beginning of the Next block.
				if (!scratch->empty()) {
					ReportCorruption(scratch->size(), "partial record without end(2)");
				}
			}
			prospectiveRecordOffset = physicalRecordOffset;
			scratch->assign(fragment.data(), fragment.size());
			inFragmentedRecord = true;
			break;

		case kMiddleType:
			if (!inFragmentedRecord) {
				ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
			}
			else {
				scratch->append(fragment.data(), fragment.size());
			}
			break;

		case kLastType:
			if (!inFragmentedRecord) {
				ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
			}
			else {
				scratch->append(fragment.data(), fragment.size());
				*record = std::string_view(*scratch);
				lastRecordOffset = prospectiveRecordOffset;
				return true;
			}
			break;

		case kEof:
			if (inFragmentedRecord) {
				// This can be caused by the writer dying immediately after
				// writing a physical record but before completing the Next; don't
				// treat it as a Corruption, just ignore the entire logical record.
				scratch->clear();
			}
			return false;

		case kBadRecord:
			if (inFragmentedRecord) {
				ReportCorruption(scratch->size(), "error in middle of record");
				inFragmentedRecord = false;
				scratch->clear();
			}
			break;

		default: {
			char buf[40];
			snprintf(buf, sizeof(buf), "unknown record type %u", recordType);
			ReportCorruption((fragment.size() + (inFragmentedRecord ? scratch->size() : 0)), buf);
			inFragmentedRecord = false;
			scratch->clear();
			break;
		}
		}
	}
	return false;
}

uint64_t LogReader::GetLastRecordOffset() {
	return lastRecordOffset;
}

void LogReader::ReportCorruption(uint64_t bytes, const char* reason) {
	ReportDrop(bytes, Status::Corruption(reason));
}

void LogReader::ReportDrop(uint64_t bytes, const Status& reason) {
	if (reporter != nullptr &&
		endofBufferOffset - buffer.size() - bytes >= initialOffset) {
		reporter->Corruption(static_cast<size_t>(bytes), reason);
	}
}

unsigned int LogReader::ReadPhysicalRecord(std::string_view* result) {
	while (true) {
		if (buffer.size()< kHeaderSize) {
			if (!eof) {
				// Last read was a full read, so this is a trailer to Skip
				buffer = "";
				Status status = file->read(kBlockSize, &buffer, backingStore);
				endofBufferOffset += buffer.size();
				if (!status.ok()) {
					buffer = "";
					ReportDrop(kBlockSize, status);
					eof = true;
					return kEof;
				}
				else if (buffer.size()< kBlockSize) {
					eof = true;
				}
				continue;
			}
			else {
				// Note that if buffer_ is non-empty, we have a truncated header at the
				// end of the file, which can be caused by the writer crashing in the
				// middle of writing the header. Instead of considering this an error,
				// just report EOF.
				buffer = "";
				return kEof;
			}
		}

		// Parse the header
		const char* header = buffer.data();
		const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
		const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
		const unsigned int type = header[6];
		const uint32_t length = a | (b<< 8);
		if (kHeaderSize + length > buffer.size()) {
			size_t dropSize = buffer.size();
			buffer = "";
			if (!eof) {
				ReportCorruption(dropSize, "bad record length");
				return kBadRecord;
			}
			// If the end of the file has been reached without reading |length| bytes
			// of payload, assume the writer died in the middle of writing the record.
			// Don't report a Corruption.
			return kEof;
		}

		if (type == kZeroType && length == 0) {
			// Skip zero length record without reporting any drops since
			// such records are produced by the mmap based writing code in
			// env_posix.cc that preallocates file regions.
			buffer = "";
			return kBadRecord;
		}

		// Check crc
		if (checksum) {
			uint32_t expectedCrc = crc32c::Unmask(DecodeFixed32(header));
			uint32_t actualCrc = crc32c::Value(header + 6, 1 + length);
			if (actualCrc != expectedCrc) {
				// Drop the rest of the buffer since "length" itself may have
				// been corrupted and if we trust it, we could find some
				// fragment of a real log record that just happens to look
				// like a Valid log record.
				size_t dropSize = buffer.size();
				buffer = "";
				ReportCorruption(dropSize, "checksum mismatch");
				return kBadRecord;
			}
		}

		buffer.remove_prefix(kHeaderSize + length);

		// Skip physical record that started before initial_offset_
		if (endofBufferOffset - buffer.size() - kHeaderSize - length<
			initialOffset) {
			*result = "";
			return kBadRecord;
		}

		*result = std::string_view(header + kHeaderSize, length);
		return type;
	}
}

