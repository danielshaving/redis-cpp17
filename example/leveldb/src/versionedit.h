#pragma once

#include <set>
#include <utility>
#include <string_view>
#include <vector>
#include "dbformat.h"
#include "status.h"

class VersionSet;

struct FileMetaData {
    int refs;
    int allowedSeeks;          // Seeks allowed until compaction
    uint64_t number;
    uint64_t fileSize;         // File size in bytes
    InternalKey smallest;       // Smallest internal key served by table
    InternalKey largest;        // Largest internal key served by table

    FileMetaData() : refs(0), allowedSeeks(1 << 30), fileSize(0) {}
};

class VersionEdit {
public:
    VersionEdit() { clear(); }

    ~VersionEdit() {}

    void clear();

    void setComparatorName(const std::string_view &name) {
        hasComparator = true;
        comparator = std::string(name.data(), name.size());
    }

    void setLogNumber(uint64_t num) {
        hasLogNumber = true;
        logNumber = num;
    }

    void setPrevLogNumber(uint64_t num) {
        hasPrevLogNumber = true;
        prevLogNumber = num;
    }

    void setNextFile(uint64_t num) {
        hasNextFileNumber = true;
        nextFileNumber = num;
    }

    void setLastSequence(uint64_t seq) {
        hasLastSequence = true;
        lastSequence = seq;
    }

    void setCompactPointer(int level, const InternalKey &key) {
        compactPointers.push_back(std::make_pair(level, key));
    }

    // Add the specified file at the specified number.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    void addFile(int level, uint64_t file,
                 uint64_t fileSize,
                 const InternalKey &smallest,
                 const InternalKey &largest) {
        FileMetaData f;
        f.number = file;
        f.fileSize = fileSize;
        f.smallest = smallest;
        f.largest = largest;
        newFiles.push_back(std::make_pair(level, f));
    }

    // Delete the specified "file" from the specified "level".
    void deleteFile(int level, uint64_t file) {
        deletedFiles.insert(std::make_pair(level, file));
    }

    void encodeTo(std::string *dst) const;

    Status decodeFrom(const std::string_view &src);

    std::string debugString() const;

    typedef std::set <std::pair<int, uint64_t>> DeletedFileSet;
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

    std::vector <std::pair<int, InternalKey>> compactPointers;
    DeletedFileSet deletedFiles;
    std::vector <std::pair<int, FileMetaData>> newFiles;
};
