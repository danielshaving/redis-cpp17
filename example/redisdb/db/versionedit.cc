#include "versionedit.h"
#include "versionset.h"
#include "coding.h"
#include "logging.h"

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
	kComparator = 1,
	kLogNumber = 2,
	kNextFileNumber = 3,
	kLastSequence = 4,
	kCompactPointer = 5,
	kDeletedFile = 6,
	kNewFile = 7,
	// 8 was used for large value refs
	kPrevLogNumber = 9
};

void VersionEdit::clear() {
	comparator.clear();
	logNumber = 0;
	prevLogNumber = 0;
	lastSequence = 0;
	nextFileNumber = 0;
	hasComparator = false;
	hasLogNumber = false;
	hasPrevLogNumber = false;
	hasNextFileNumber = false;
	hasLastSequence = false;
	deletedFiles.clear();
	newFiles.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
	if (hasComparator) {
		PutVarint32(dst, kComparator);
		PutLengthPrefixedSlice(dst, comparator);
	}

	if (hasLogNumber) {
		PutVarint32(dst, kLogNumber);
		PutVarint64(dst, logNumber);
	}

	if (hasPrevLogNumber) {
		PutVarint32(dst, kPrevLogNumber);
		PutVarint64(dst, prevLogNumber);
	}

	if (hasNextFileNumber) {
		PutVarint32(dst, kNextFileNumber);
		PutVarint64(dst, nextFileNumber);
	}

	if (hasLastSequence) {
		PutVarint32(dst, kLastSequence);
		PutVarint64(dst, lastSequence);
	}

	for (size_t i = 0; i< compactPointers.size(); i++) {
		PutVarint32(dst, kCompactPointer);
		PutVarint32(dst, compactPointers[i].first);  // level
		PutLengthPrefixedSlice(dst, compactPointers[i].second.Encode());
	}

	for (auto iter = deletedFiles.begin(); iter != deletedFiles.end(); ++iter) {
		PutVarint32(dst, kDeletedFile);
		PutVarint32(dst, iter->first);   // level
		PutVarint64(dst, iter->second);  // file number
	}

	for (size_t i = 0; i< newFiles.size(); i++) {
		const FileMetaData& f = newFiles[i].second;
		PutVarint32(dst, kNewFile);
		PutVarint32(dst, newFiles[i].first);  // level
		PutVarint64(dst, f.number);
		PutVarint64(dst, f.FileSize);
		PutLengthPrefixedSlice(dst, f.smallest.Encode());
		PutLengthPrefixedSlice(dst, f.largest.Encode());
	}
}

static bool getInternalKey(std::string_view* input, InternalKey* dst) {
	std::string_view str;
	if (GetLengthPrefixedSlice(input, &str)) {
		dst->DecodeFrom(str);
		return true;
	}
	else {
		return false;
	}
}

static bool getLevel(std::string_view* input, int* level) {
	uint32_t v;
	if (GetVarint32(input, &v) && v< kNumLevels) {
		*level = v;
		return true;
	}
	else {
		return false;
	}
}

Status VersionEdit::DecodeFrom(const std::string_view& src) {
	clear();
	std::string_view input = src;
	const char* msg = nullptr;
	uint32_t tag;

	// Temporary storage for parsing
	int level;
	uint64_t number;
	FileMetaData f;
	std::string_view str;
	InternalKey key;

	while (msg == nullptr && GetVarint32(&input, &tag)) {
		switch (tag) {
		case kComparator:
			if (GetLengthPrefixedSlice(&input, &str)) {
				comparator = std::string(str.data(), str.size());
				hasComparator = true;
			}
			else {
				msg = "comparator name";
			}
			break;

		case kLogNumber:
			if (GetVarint64(&input, &logNumber)) {
				hasLogNumber = true;
			}
			else {
				msg = "log number";
			}
			break;

		case kPrevLogNumber:
			if (GetVarint64(&input, &prevLogNumber)) {
				hasPrevLogNumber = true;
			}
			else {
				msg = "previous log number";
			}
			break;

		case kNextFileNumber:
			if (GetVarint64(&input, &nextFileNumber)) {
				hasNextFileNumber = true;
			}
			else {
				msg = "Next file number";
			}
			break;

		case kLastSequence:
			if (GetVarint64(&input, &lastSequence)) {
				hasLastSequence = true;
			}
			else {
				msg = "last sequence number";
			}
			break;

		case kCompactPointer:
			if (getLevel(&input, &level) && getInternalKey(&input, &key)) {
				compactPointers.push_back(std::make_pair(level, key));
			}
			else {
				msg = "compaction pointer";
			}
			break;

		case kDeletedFile:
			if (getLevel(&input, &level) && GetVarint64(&input, &number)) {
				deletedFiles.insert(std::make_pair(level, number));
			}
			else {
				msg = "deleted file";
			}
			break;

		case kNewFile:
			if (getLevel(&input, &level) &&
				GetVarint64(&input, &f.number) &&
				GetVarint64(&input, &f.FileSize) &&
				getInternalKey(&input, &f.smallest) &&
				getInternalKey(&input, &f.largest)) {
				newFiles.push_back(std::make_pair(level, f));
			}
			else {
				msg = "new-file entry";
			}
			break;

		default:
			msg = "unknown tag";
			break;
		}
	}

	if (msg == nullptr && !input.empty()) {
		msg = "invalid tag";
	}

	Status result;
	if (msg != nullptr) {
		result = Status::Corruption("VersionEdit", msg);
	}
	return result;
}

std::string VersionEdit::DebugString() const {
	std::string r;
	r.append("VersionEdit {");
	if (hasComparator) {
		r.append("\n  Comparator: ");
		r.append(comparator);
	}

	if (hasLogNumber) {
		r.append("\n  LogNumber: ");
		AppendNumberTo(&r, logNumber);
	}

	if (hasPrevLogNumber) {
		r.append("\n  PrevLogNumber: ");
		AppendNumberTo(&r, prevLogNumber);
	}

	if (hasNextFileNumber) {
		r.append("\n  NextFile: ");
		AppendNumberTo(&r, nextFileNumber);
	}

	if (hasLastSequence) {
		r.append("\n  LastSeq: ");
		AppendNumberTo(&r, lastSequence);
	}

	for (size_t i = 0; i< compactPointers.size(); i++) {
		r.append("\n  CompactPointer: ");
		AppendNumberTo(&r, compactPointers[i].first);
		r.append(" ");
		r.append(compactPointers[i].second.DebugString());
	}

	for (auto iter = deletedFiles.begin(); iter != deletedFiles.end(); ++iter) {
		r.append("\n  DeleteFile: ");
		AppendNumberTo(&r, iter->first);
		r.append(" ");
		AppendNumberTo(&r, iter->second);
	}

	for (size_t i = 0; i< newFiles.size(); i++) {
		const FileMetaData& f = newFiles[i].second;
		r.append("\n  AddFile: ");
		AppendNumberTo(&r, newFiles[i].first);
		r.append(" ");
		AppendNumberTo(&r, f.number);
		r.append(" ");
		AppendNumberTo(&r, f.FileSize);
		r.append(" ");
		r.append(f.smallest.DebugString());
		r.append(" .. ");
		r.append(f.largest.DebugString());
	}
	r.append("\n}\n");
	return r;
}




