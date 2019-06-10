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
	lognumber = 0;
	prevlognumber = 0;
	lastsequence = 0;
	nextfilenumber = 0;
	hascomparator = false;
	haslognumber = false;
	hasprevlognumber = false;
	hasnextfilenumber = false;
	haslastsequence = false;
	deletedfiles.clear();
	newfiles.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
	if (hascomparator) {
		PutVarint32(dst, kComparator);
		PutLengthPrefixedSlice(dst, comparator);
	}

	if (haslognumber) {
		PutVarint32(dst, kLogNumber);
		PutVarint64(dst, lognumber);
	}

	if (hasprevlognumber) {
		PutVarint32(dst, kPrevLogNumber);
		PutVarint64(dst, prevlognumber);
	}

	if (hasnextfilenumber) {
		PutVarint32(dst, kNextFileNumber);
		PutVarint64(dst, nextfilenumber);
	}

	if (haslastsequence) {
		PutVarint32(dst, kLastSequence);
		PutVarint64(dst, lastsequence);
	}

	for (size_t i = 0; i< compactpointers.size(); i++) {
		PutVarint32(dst, kCompactPointer);
		PutVarint32(dst, compactpointers[i].first);  // level
		PutLengthPrefixedSlice(dst, compactpointers[i].second.Encode());
	}

	for (auto iter = deletedfiles.begin(); iter != deletedfiles.end(); ++iter) {
		PutVarint32(dst, kDeletedFile);
		PutVarint32(dst, iter->first);   // level
		PutVarint64(dst, iter->second);  // file number
	}

	for (size_t i = 0; i< newfiles.size(); i++) {
		const FileMetaData& f = newfiles[i].second;
		PutVarint32(dst, kNewFile);
		PutVarint32(dst, newfiles[i].first);  // level
		PutVarint64(dst, f.number);
		PutVarint64(dst, f.filesize);
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
				hascomparator = true;
			}
			else {
				msg = "comparator name";
			}
			break;

		case kLogNumber:
			if (GetVarint64(&input, &lognumber)) {
				haslognumber = true;
			}
			else {
				msg = "log number";
			}
			break;

		case kPrevLogNumber:
			if (GetVarint64(&input, &prevlognumber)) {
				hasprevlognumber = true;
			}
			else {
				msg = "previous log number";
			}
			break;

		case kNextFileNumber:
			if (GetVarint64(&input, &nextfilenumber)) {
				hasnextfilenumber = true;
			}
			else {
				msg = "Next file number";
			}
			break;

		case kLastSequence:
			if (GetVarint64(&input, &lastsequence)) {
				haslastsequence = true;
			}
			else {
				msg = "last sequence number";
			}
			break;

		case kCompactPointer:
			if (getLevel(&input, &level) && getInternalKey(&input, &key)) {
				compactpointers.push_back(std::make_pair(level, key));
			}
			else {
				msg = "compaction pointer";
			}
			break;

		case kDeletedFile:
			if (getLevel(&input, &level) && GetVarint64(&input, &number)) {
				deletedfiles.insert(std::make_pair(level, number));
			}
			else {
				msg = "deleted file";
			}
			break;

		case kNewFile:
			if (getLevel(&input, &level) &&
				GetVarint64(&input, &f.number) &&
				GetVarint64(&input, &f.filesize) &&
				getInternalKey(&input, &f.smallest) &&
				getInternalKey(&input, &f.largest)) {
				newfiles.push_back(std::make_pair(level, f));
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
	if (hascomparator) {
		r.append("\n  Comparator: ");
		r.append(comparator);
	}

	if (haslognumber) {
		r.append("\n  LogNumber: ");
		AppendNumberTo(&r, lognumber);
	}

	if (hasprevlognumber) {
		r.append("\n  PrevLogNumber: ");
		AppendNumberTo(&r, prevlognumber);
	}

	if (hasnextfilenumber) {
		r.append("\n  NextFile: ");
		AppendNumberTo(&r, nextfilenumber);
	}

	if (haslastsequence) {
		r.append("\n  LastSeq: ");
		AppendNumberTo(&r, lastsequence);
	}

	for (size_t i = 0; i< compactpointers.size(); i++) {
		r.append("\n  CompactPointer: ");
		AppendNumberTo(&r, compactpointers[i].first);
		r.append(" ");
		r.append(compactpointers[i].second.DebugString());
	}

	for (auto iter = deletedfiles.begin(); iter != deletedfiles.end(); ++iter) {
		r.append("\n  DeleteFile: ");
		AppendNumberTo(&r, iter->first);
		r.append(" ");
		AppendNumberTo(&r, iter->second);
	}

	for (size_t i = 0; i< newfiles.size(); i++) {
		const FileMetaData& f = newfiles[i].second;
		r.append("\n  AddFile: ");
		AppendNumberTo(&r, newfiles[i].first);
		r.append(" ");
		AppendNumberTo(&r, f.number);
		r.append(" ");
		AppendNumberTo(&r, f.filesize);
		r.append(" ");
		r.append(f.smallest.DebugString());
		r.append(" .. ");
		r.append(f.largest.DebugString());
	}
	r.append("\n}\n");
	return r;
}




