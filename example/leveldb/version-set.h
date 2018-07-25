#pragma once
#include <map>
#include <set>
#include <vector>
#include <list>
#include <deque>
#include <assert.h>
#include "option.h"
#include "dbformat.h"
#include "version-edit.h"
#include "log-writer.h"

class VersionSet;
class Version
{
public:
	Version(VersionSet *vset)
	:vset(vset),
	 refs(0)
	{

	}

	VersionSet *vset;     // VersionSet to which this Version belongs

	// Reference count management (so Versions do not disappear out from
	// under live iterators)
	void ref()
	{
		++refs;
	}

	bool unref()
	{
		assert(refs >= 1);
		--refs;
		if (refs == 0)
		{
			return true;
		}
		return false;
	}

	// List of files per level
	std::vector<std::shared_ptr<FileMetaData>> files[kNumLevels];

	// Next file to compact based on seek stats.
	std::shared_ptr<FileMetaData> fileToCompact;
	int fileToCompactLevel;

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed.  These fields
	// are initialized by Finalize().
	double compactionScore;
	int compactionLevel;
	int refs;

private:
	// No copying allowed
	Version(const Version&);
	void operator=(const Version&);

};

class Builder
{
private:
	// Helper to sort by v->files_[file_number].smallest
	struct BySmallestKey 
	{
		const InternalKeyComparator *internalComparator;
		bool operator()(const std::shared_ptr<FileMetaData> &f1,
			const std::shared_ptr<FileMetaData> &f2) const 
		{
			int r = internalComparator->compare(f1->smallest,f2->smallest);
			if (r != 0) 
			{
				return (r < 0);
			} 
			else 
			{
			// Break ties by file number
				return (f1->number < f2->number);
			}
		}
	};

	typedef std::set<std::shared_ptr<FileMetaData>,BySmallestKey> FileSet;
	struct LevelState 
	{
    	std::set<uint64_t> deletedFiles;
    	std::shared_ptr<FileSet> addedFiles;
 	};

	VersionSet *vset;
	Version *base;
	LevelState levels[kNumLevels];
public:
	Builder(VersionSet *vset,Version *base)
	:vset(vset),
	 base(base)
	{
		BySmallestKey cmp;
		for (int level = 0; level < kNumLevels; level++) 
		{
			levels[level].addedFiles.reset(new FileSet(cmp));
		}
	}

	~Builder() 
	{
		
	}
	// Apply all of the edits in *edit to the current state.
	void apply(VersionEdit *edit);
	
	// Save the current state in *v.
	void saveTo(Version *v); 
	
	void maybeAddFile(Version *v,int level,const std::shared_ptr<FileMetaData> &f);

};

class VersionSet
{
public:
	VersionSet(const std::string &dbname,const Options &options);
	~VersionSet();

	uint64_t getLastSequence() const { return lastSequence; }
	void setLastSequence(uint64_t s)
	{
		assert(s >= lastSequence);
		lastSequence = s;
	}

	// Apply *edit to the current version to form a new descriptor that
	// is both saved to persistent state and installed as the new
	// current version.  Will release *mu while actually writing to the file.
	// REQUIRES: *mu is held on entry.
	// REQUIRES: no other thread concurrently calls LogAndApply()
	Status logAndApply(VersionEdit *edit);

	void finalize(Version* v);
	// Add all files listed in any live version to *live.
	// May also mutate some internal state.
	void addLiveFiles(std::set<uint64_t> *live);

	uint64_t getLogNumber() { return logNumber; }
	uint64_t getPrevLogNumber() { return prevLogNumber; }

	const InternalKeyComparator icmp;
	
	uint64_t newFileNumber() { return nextFileNumber++; }
	Status recover(bool *manifest);
	void markFileNumberUsed(uint64_t number);
	uint64_t getManifestFileNumber() { return manifestFileNumber; }
	void appendVersion(const std::shared_ptr<Version> &v);
	bool reuseManifest(const std::string &dscname,const std::string &dscbase);

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    std::string compactPointer[kNumLevels];

private:
	const std::string dbname;
	const Options options;
	uint64_t nextFileNumber;
	uint64_t manifestFileNumber;
	uint64_t lastSequence;
	uint64_t logNumber;
	uint64_t prevLogNumber;  // 0 or backing store for memtable being compacted
    std::list<std::shared_ptr<Version>> versions;
	std::shared_ptr<LogWriter> descriptorLog;
	std::shared_ptr<PosixWritableFile> descriptorFile;
};
