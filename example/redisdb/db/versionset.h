#pragma once

#include <map>
#include <set>
#include <vector>
#include <list>
#include <deque>
#include <assert.h>
#include "logwriter.h"
#include "tablecache.h"
#include "versionedit.h"
#include "option.h"
#include "dbformat.h"
#include "env.h"

class VersionSet;

class Compaction;

class Version {
public:
	Version(VersionSet* vset)
		: vset(vset) {

	}

	// Append to *iters a sequence of iterators that will
	// yield the Contents of this Version when merged together.
	// REQUIRES: This version has been saved (see VersionSet::SaveTo)
	void AddIterators(const ReadOptions& ops, std::vector<std::shared_ptr<Iterator>>* iters);


	VersionSet* vset;     // VersionSet to which this Version belongs

	// Lookup the value for key.  If found, store it in *val and
	// return OK.  Else return a non-OK status.  Fills *stats.
	// REQUIRES: lock is not held
	struct GetStats {
		std::shared_ptr<FileMetaData> seekFile;
		int seekFileLevel;
	};

	Status Get(const ReadOptions& options, const LookupKey& key, std::string* val,
		GetStats* stats);

	// Return the level at which we should place a new memtable compaction
	// result that covers the range [smallest_user_key,largest_user_key].
	int PickLevelForMemTableOutput(const std::string_view& smallestuserkey,
		const std::string_view& largestuserkey);


	void GetOverlappingInputs(
		int level,
		const InternalKey* begin,         // nullptr means before all keys
		const InternalKey* end,           // nullptr means after all keys
		std::vector<std::shared_ptr<FileMetaData>>* inputs);

	void SaveValue(const std::any& arg, const std::string_view& ikey, const std::string_view& v);
	// Returns true iff some file in the specified level overlaps
	// some part of [*smallest_user_key,*largest_user_key].
	// smallest_user_key==nullptr represents a key smaller than all the DB's keys.
	// largest_user_key==nullptr represents a key largest than all the DB's keys.

	bool OverlapInLevel(int level, const std::string_view* smallestuserkey,
		const std::string_view* largestuserkey);

	// Adds "stats" into the version state.  Returns true if a new
	// compaction may need to be triggered, false otherwise.
	// REQUIRES: lock is held
	bool UpdateStats(const GetStats& stats);

	// Return a human readable string that describes this version's Contents.
	std::string DebugString() const;

	// No copying allowed
	Version(const Version&);

	void operator=(const Version&);

	friend class Compaction;

	friend class VersionSet;

	class LevelFileNumIterator;

	std::shared_ptr<Iterator> NewConcatenatingIterator(const ReadOptions& ops, int level) const;

	// List of files per level
	std::vector<std::shared_ptr<FileMetaData>> files[kNumLevels];

	// Next file to compact based on Seek stats.
	std::shared_ptr<FileMetaData> filetocompact;
	int filetocompactlevel;

	// Level that should be compacted Next and its compaction score.
	// Score< 1 means compaction is not strictly needed.  These fields
	// are initialized by Finalize().
	double compactionscore;
	int compactionlevel;
};

class Builder {
public:
	Builder(VersionSet* vset, const std::shared_ptr<Version>& base);

	~Builder();

	// Apply all of the edits in *edit to the version state.
	void Apply(VersionEdit* edit);

	// Save the version state in *v.
	void SaveTo(Version* v);

	void MaybeAddFile(Version* v, int level, const std::shared_ptr<FileMetaData>& f);

private:
	// Helper to sort by v->files_[file_number].smallest
	struct BySmallestKey {
		const InternalKeyComparator* internalComparator;

		bool operator()(const std::shared_ptr<FileMetaData>& f1,
			const std::shared_ptr<FileMetaData>& f2) const {
			int r = internalComparator->Compare(f1->smallest, f2->smallest);
			if (r != 0) {
				return (r< 0);
			}
			else {
				// Break ties by file number
				return (f1->number< f2->number);
			}
		}
	};

	typedef std::set<std::shared_ptr<FileMetaData>, BySmallestKey> FileSet;
	struct LevelState {
		std::set<uint64_t> deletedfiles;
		std::shared_ptr<FileSet> addedFiles;
	};

	VersionSet* vset;
	std::shared_ptr<Version> base;
	LevelState levels[kNumLevels];
};

class VersionSet {
public:
	VersionSet(const std::string& dbname, const Options& options,
		const std::shared_ptr<TableCache>& tablecache, const InternalKeyComparator* cmp);

	~VersionSet();

	std::shared_ptr<Version> current() const {
		assert(!dummyversions.empty());
		return dummyversions.back();
	}

	uint64_t GetLastSequence() const { return lastsequence; }

	void SetLastSequence(uint64_t s) {
		assert(s >= lastsequence);
		lastsequence = s;
	}

	// Returns true iff some level needs a compaction.
	bool NeedsCompaction() const {
		return (current()->compactionscore >= 1) || (current()->filetocompact != nullptr);
	}

	// Arrange to reuse "file_number" unless a newer file number has
	// already been allocated.
	// REQUIRES: "file_number" was returned by a call to NewFileNumber().
	void ReuseFileNumber(uint64_t fileNumber) {
		if (nextfilenumber == fileNumber + 1) {
			nextfilenumber = fileNumber;
		}
	}

	// Apply *edit to the version version to form a new descriptor that
	// is both saved to persistent state and installed as the new
	// version version.  Will Release *mu while actually writing to the file.
	// REQUIRES: *mu is held on entry.
	// REQUIRES: no other thread concurrently calls LogAndApply()
	Status LogAndApply(VersionEdit* edit, std::mutex* mutex);

	void Finalize(Version* v);

	// Save version Contents to *log
	Status WriteSnapshot();

	// Add all files listed in any live version to *live.
	// May also mutate some internal state.
	void AddLiveFiles(std::set<uint64_t>* live);

	// Return the approximate offset in the database of the data for
	// "key" as of version "v".
	uint64_t ApproximateOffsetOf(const InternalKey& key);

	uint64_t GetLogNumber() { return lognumber; }

	uint64_t GetPrevLogNumber() { return prevlognumber; }

	const InternalKeyComparator icmp;

	uint64_t NewFileNumber() { return nextfilenumber++; }

	Status Recover(bool* manifest);

	void MarkFileNumberUsed(uint64_t number);

	uint64_t GetManifestFileNumber() { return manifestfilenumber; }

	void AppendVersion(const std::shared_ptr<Version>& v);

	bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

	// Return the number of Table files at the specified level.
	int NumLevelFiles(int level) const;

	// Return the combined file size of all files at the specified level.
	int64_t NumLevelBytes(int level) const;

	// Return the maximum overlapping data (in bytes) at Next level for any
	// file at a level >= 1.
	int64_t MaxNextLevelOverlappingBytes();

	std::shared_ptr<Iterator> MakeInputIterator(Compaction* c);

	// Return a human-readable short (single-line) summary of the number
	// of files per level.  Uses *scratch as backing store.
	struct LevelSummaryStorage {
		char buffer[100];
	};

	const char* LevelSummary(LevelSummaryStorage* scratch) const;

	// Pick level and inputs for a new compaction.
	// Returns nullptr if there is no compaction to be done.
	// Otherwise returns a pointer to a heap-allocated object that
	// describes the compaction.  Caller should delete the result.
	std::shared_ptr<Compaction> PickCompaction();

	// Return a compaction object for compacting the range [begin,end] in
	// the specified level.  Returns nullptr if there is nothing in that
	// level that overlaps the specified range.  Caller should delete
	// the result.
	std::shared_ptr<Compaction> CompactRange(
		int level,
		const InternalKey* begin,
		const InternalKey* end);

	void GetRange(const std::vector<std::shared_ptr<FileMetaData>>& inputs,
		InternalKey* smallest,
		InternalKey* largest);

	void GetRange2(const std::vector<std::shared_ptr<FileMetaData>>& inputs1,
		const std::vector<std::shared_ptr<FileMetaData>>& inputs2,
		InternalKey* smallest,
		InternalKey* largest);

	void SetupOtherInputs(const std::shared_ptr<Compaction>& c);

	std::shared_ptr<TableCache> GetTableCache() { return tablecache; }
	// Per-level key at which the Next compaction at that level should start.
	// Either an empty string, or a Valid InternalKey.

	std::string compactPointer[kNumLevels];
	const std::string dbname;
	const Options options;
	uint64_t nextfilenumber;
	uint64_t manifestfilenumber;
	uint64_t lastsequence;
	uint64_t lognumber;
	uint64_t prevlognumber;  // 0 or backing store for memtable being compacted

	std::list<std::shared_ptr<Version>> dummyversions;
	std::shared_ptr<LogWriter> descriptorlog;
	std::shared_ptr<WritableFile> descriptorfile;
	std::shared_ptr<TableCache> tablecache;
};

int FindFile(const InternalKeyComparator& icmp,
	const std::vector<std::shared_ptr<FileMetaData>>& files,
	const std::string_view & key);

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjointSortedFiles,
	const std::vector<std::shared_ptr<FileMetaData>>& files,
	const std::string_view* smallestuserkey,
	const std::string_view* largestuserkey);

// A Compaction encapsulates information about a compaction.
class Compaction {
public:
	Compaction(const Options* options, int level);

	~Compaction();

	// Return the level that is being compacted.  Inputs from "level"
	// and "level+1" will be merged to produce a Set of "level+1" files.
	int getLevel() const { return level; }

	// Return the object that holds the edits to the descriptor done
	// by this compaction.
	VersionEdit* getEdit() { return &edit; }

	// "which" must be either 0 or 1
	int numInputFiles(int which) const { return inputs[which].size(); }

	// Return the ith input file at "level()+which" ("which" must be 0 or 1).
	std::shared_ptr<FileMetaData> input(int which, int i) const { return inputs[which][i]; }

	// Maximum size of files to build during this compaction.
	uint64_t getMaxOutputFileSize() const { return maxoutputfilesize; }

	// Is this a trivial compaction that can be implemented by just
	// moving a single input file to the Next level (no merging or splitting)
	bool isTrivialMove() const;

	// Add all inputs to this compaction as delete operations to *edit.
	void addInputDeletions(VersionEdit* edit);

	// Returns true if the information we have available guarantees that
	// the compaction is producing data in "level+1" for which no data exists
	// in levels greater than "level+1".
	bool isBaseLevelForKey(const std::string_view& userkey);

	// Returns true iff we should stop building the version output
	// before processing "internal_key".
	bool shouldStopBefore(const std::string_view& internalKey);

	// Release the input version for the compaction, once the compaction
	// is successful.
	void releaseInputs();

private:
	friend class Version;

	friend class VersionSet;

	int level;
	uint64_t maxoutputfilesize;
	size_t grandparentindex; // Index in grandparent_starts_
	bool seenkey; // Some output key has been seen
	int64_t overlappedbytes; // Bytes of overlap between version output
	// and grandparent files
	// State for implementing IsBaseLevelForKey

	// level_ptrs_ holds indices into input_version_->levels_: our state
	// is that we are positioned at one of the file ranges for each
	// higher level than the ones involved in this compaction (i.e. for
	// all L >= level_ + 2).
	size_t levelptrs[kNumLevels];

	VersionEdit edit;
	std::shared_ptr<Version> inputversion;
	// Each compaction reads inputs from "level_" and "level_+1"
	std::vector<std::shared_ptr<FileMetaData>> inputs[2];
	// State used to check for number of of overlapping grandparent files
	// (parent == level_ + 1, grandparent == level_ + 2)
	std::vector<std::shared_ptr<FileMetaData>> grandparents;
};

