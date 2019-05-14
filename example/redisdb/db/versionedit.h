#pragma once

#include <set>
#include <utility>
#include <string_view>
#include <vector>
#include "dbformat.h"
#include "status.h"

class VersionSet;

struct FileMetaData {
	int allowedSeeks;          // Seeks allowed until compaction
	uint64_t number;
	uint64_t FileSize;         // File size in bytes
	InternalKey smallest;       // Smallest internal key served by table
	InternalKey largest;        // Largest internal key served by table

	FileMetaData() : allowedSeeks(1<< 30), FileSize(0) {}
};

class VersionEdit {
public:
	VersionEdit() { clear(); }

	~VersionEdit() {}

	void clear();

	void SetComparatorName(const std::string_view& name) {
		hasComparator = true;
		comparator = std::string(name.data(), name.size());
	}

	void SetLogNumber(uint64_t num) {
		hasLogNumber = true;
		logNumber = num;
	}

	void SetPrevLogNumber(uint64_t num) {
		hasPrevLogNumber = true;
		prevLogNumber = num;
	}

	void SetNextFile(uint64_t num) {
		hasNextFileNumber = true;
		nextFileNumber = num;
	}

	void SetLastSequence(uint64_t seq) {
		hasLastSequence = true;
		lastSequence = seq;
	}

	void SetCompactPointer(int level, const InternalKey& key) {
		compactPointers.push_back(std::make_pair(level, key));
	}

	// Add the specified file at the specified number.
	// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
	// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
	void AddFile(int level, uint64_t file,
		uint64_t FileSize,
		const InternalKey& smallest,
		const InternalKey& largest) {
		FileMetaData f;
		f.number = file;
		f.FileSize = FileSize;
		f.smallest = smallest;
		f.largest = largest;
		newFiles.push_back(std::make_pair(level, f));
	}

	// Delete the specified "file" from the specified "level".
	void DeleteFile(int level, uint64_t file) {
		deletedFiles.insert(std::make_pair(level, file));
	}

	void EncodeTo(std::string* dst) const;

	Status DecodeFrom(const std::string_view& src);

	std::string DebugString() const;

	typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;
	std::string comparator;
	uint64_t logNumber;
	uint64_t prevLogNumber;
	uint64_t nextFileNumber;
	uint64_t lastSequence;

	bool hasComparator;
	bool hasLogNumber;
	bool hasPrevLogNumber;
	bool hasNextFileNumber;
	bool hasLastSequence;

	std::vector<std::pair<int, InternalKey>> compactPointers;
	DeletedFileSet deletedFiles;
	std::vector<std::pair<int, FileMetaData>> newFiles;
};
