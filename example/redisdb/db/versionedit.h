#pragma once

#include <set>
#include <utility>
#include <string_view>
#include <vector>
#include "dbformat.h"
#include "status.h"

class VersionSet;

struct FileMetaData {
	int allowedseeks;          // Seeks allowed until compaction
	uint64_t number;
	uint64_t filesize;         // File size in bytes
	InternalKey smallest;       // Smallest internal key served by table
	InternalKey largest;        // Largest internal key served by table

	FileMetaData() : allowedseeks(1 << 30), filesize(0) {}
};

class VersionEdit {
public:
	VersionEdit() { clear(); }

	~VersionEdit() {}

	void clear();

	void SetComparatorName(const std::string_view& name) {
		hascomparator = true;
		comparator = std::string(name.data(), name.size());
	}

	void SetLogNumber(uint64_t num) {
		haslognumber = true;
		lognumber = num;
	}

	void SetPrevLogNumber(uint64_t num) {
		hasprevlognumber = true;
		prevlognumber = num;
	}

	void SetNextFile(uint64_t num) {
		hasnextfilenumber = true;
		nextfilenumber = num;
	}

	void SetLastSequence(uint64_t seq) {
		haslastsequence = true;
		lastsequence = seq;
	}

	void SetCompactPointer(int level, const InternalKey& key) {
		compactpointers.push_back(std::make_pair(level, key));
	}

	// Add the specified file at the specified number.
	// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
	// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
	void AddFile(int level, uint64_t file,
		uint64_t filesize,
		const InternalKey& smallest,
		const InternalKey& largest) {
		FileMetaData f;
		f.number = file;
		f.filesize = filesize;
		f.smallest = smallest;
		f.largest = largest;
		newfiles.push_back(std::make_pair(level, f));
	}

	// Delete the specified "file" from the specified "level".
	void DeleteFile(int level, uint64_t file) {
		deletedfiles.insert(std::make_pair(level, file));
	}

	void EncodeTo(std::string* dst) const;

	Status DecodeFrom(const std::string_view& src);

	std::string DebugString() const;

	typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;
	std::string comparator;
	uint64_t lognumber;
	uint64_t prevlognumber;
	uint64_t nextfilenumber;
	uint64_t lastsequence;

	bool hascomparator;
	bool haslognumber;
	bool hasprevlognumber;
	bool hasnextfilenumber;
	bool haslastsequence;

	std::vector<std::pair<int, InternalKey>> compactpointers;
	DeletedFileSet deletedfiles;
	std::vector<std::pair<int, FileMetaData>> newfiles;
};
