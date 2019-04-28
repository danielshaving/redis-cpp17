#pragma once

#include "logwriter.h"
#include "versionedit.h"
#include "versionset.h"
#include <deque>
#include <set>
#include <memory>
#include <mutex>
#include <condition_variable>
#include "option.h"
#include "status.h"
#include "writebatch.h"
#include "env.h"
#include "tablecache.h"

// A range of keys
struct Range {
    std::string_view start;          // Included in the range
    std::string_view limit;          // Not included in the range

    Range() {}

    Range(const std::string_view &s, const std::string_view &l) : start(s), limit(l) {}
};

class DBImpl {
public:
    DBImpl(const Options &options, const std::string &dbname);

    ~DBImpl();

    Status open();

    Status newDB();

    // Set the database entry for "key" to "value".  Returns OK on success,
    // and a non-OK status on error.
    // Note: consider setting options.sync = true.
    Status put(const WriteOptions &, const std::string_view &key, const std::string_view &value);

    // Remove the database entry (if any) for "key".  Returns OK on
    // success, and a non-OK status on error.  It is not an error if "key"
    // did not exist in the database.
    // Note: consider setting options.sync = true.
    Status del(const WriteOptions &, const std::string_view &key);

    // Apply the specified updates to the database.
    // Returns OK on success, non-OK on failure.
    // Note: consider setting options.sync = true.
    Status write(const WriteOptions &options, WriteBatch *updates);

    // If the database contains an entry for "key" store the
    // corresponding value in *value and return OK.
    //
    // If there is no entry for "key" leave *value unchanged and return
    // a status for which Status::IsNotFound() returns true.
    //
    // May return some other Status on an error.
    Status get(const ReadOptions &options, const std::string_view &key, std::string *value);

    Status destroyDB(const std::string &dbname, const Options &options);

    void deleteObsoleteFiles();

    // Recover the descriptor from persistent storage.  May do a significant
    // amount of work to recover recently logged updates.  Any changes to
    // be made to the descriptor are added to *edit.
    Status recover(VersionEdit *edit, bool *saveManifest);

    Status recoverLogFile(uint64_t logNumber, bool lastLog,
                          bool *saveManifest, VersionEdit *edit, uint64_t *maxSequence);

    Status writeLevel0Table(const std::shared_ptr <MemTable> &mem, VersionEdit *edit, Version *base);

    Status buildTable(FileMetaData *meta, const std::shared_ptr <Iterator> &iter);

    void maybeScheduleCompaction();

    void backgroundCompaction();

    void compactMemTable();

    std::shared_ptr <Iterator> newInternalIterator(const ReadOptions &options,
                                                   uint64_t *latestSnapshot, uint32_t *seed);

    // Return a heap-allocated iterator over the contents of the database.
    // The result of NewIterator() is initially invalid (caller must
    // call one of the Seek methods on the iterator before using it).
    //
    // Caller should delete the iterator when it is no longer needed.
    // The returned iterator should be deleted before this db is deleted.
    std::shared_ptr <Iterator> newIterator(const ReadOptions &options);

    Options sanitizeOptions(const std::string &dbname,
                            const InternalKeyComparator *icmp,
                            const Options &src);

    // DB implementations can export properties about their state
    // via this method.  If "property" is a valid property understood by this
    // DB implementation, fills "*value" with its current value and returns
    // true.  Otherwise returns false.
    //
    //
    // Valid property names include:
    //
    //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
    //     where <N> is an ASCII representation of a level number (e.g. "0").
    //  "leveldb.stats" - returns a multi-line string that describes statistics
    //     about the internal operation of the DB.
    //  "leveldb.sstables" - returns a multi-line string that describes all
    //     of the sstables that make up the db contents.
    //  "leveldb.approximate-memory-usage" - returns the approximate number of
    //     bytes of memory in use by the DB.
    bool getProperty(const std::string_view &property, std::string *value);

    // For each i in [0,n-1], store in "sizes[i]", the approximate
    // file system space used by keys in "[range[i].start .. range[i].limit)".
    //
    // Note that the returned sizes measure file system space usage, so
    // if the user data compresses by a factor of ten, the returned
    // sizes will be one-tenth the size of the corresponding user data size.
    //
    // The results may not include the sizes of recently written data.
    void getApproximateSizes(const Range *range, int n, uint64_t *sizes);

	void compactRange(const std::string_view *begin, const std::string_view *end);
	  
    // Force current memtable contents to be compacted.
    Status testCompactMemTable();

    // Compact any files in the named level that overlap [*begin,*end]
    void testCompactRange(int level, const std::string_view *begin, const std::string_view *end);

    // Return an internal iterator over the current state of the database.
    // The keys of this iterator are internal keys (see format.h).
    // The returned iterator should be deleted when no longer needed.
    std::shared_ptr <Iterator> testNewInternalIterator();

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    int64_t testMaxNextLevelOverlappingBytes();

    void backgroundCallback();

	void maybeIgnoreError(Status *s) const;
	
	void recordBackgroundError(const Status &s);
	  
private:
    struct Writer;
    struct CompactionState;

    // No copying allowed
    DBImpl(const DBImpl &);

    void operator=(const DBImpl &);

    Status makeRoomForWrite(std::unique_lock <std::mutex> &lk, bool force /* compact even if there is room? */);

    WriteBatch *buildBatchGroup(Writer **lastWriter);

    Status doCompactionWork(CompactionState *compact);

    Status finishCompactionOutputFile(CompactionState *compact,
                                      const std::shared_ptr <Iterator> &input);

    Status openCompactionOutputFile(CompactionState *compact);

    Status installCompactionResults(CompactionState *compact);

    void cleanupCompaction(CompactionState *compact);

    std::atomic<bool> shuttingdown;
    std::atomic<bool> hasimm;         // So bg thread can detect non-null imm_
    // Has a background compaction been scheduled or is running?
    bool backgroundCompactionScheduled;

    // Queue of writers.
    std::deque<Writer *> writers;
    std::shared_ptr <WriteBatch> tmpBatch;
    std::shared_ptr <MemTable> mem;
    std::shared_ptr <MemTable> imm;
    std::shared_ptr <VersionSet> versions;
    std::shared_ptr <LogWriter> log;
    std::shared_ptr <WritableFile> logfile;
    std::shared_ptr <TableCache> tableCache;
	std::shared_ptr <Env> env;

	  // Lock over the persistent DB state.  Non-null iff successfully acquired.
	std::shared_ptr <FileLock> dbLock;

    const Options options;
    uint64_t logfileNumber;
    const std::string dbname;
    uint32_t seed;

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set <uint64_t> pendingOutPuts;
    const InternalKeyComparator comparator;

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        int64_t micros;
        int64_t bytesRead;
        int64_t bytesWritten;

        CompactionStats() : micros(0), bytesRead(0), bytesWritten(0) {}

        void add(const CompactionStats &c) {
            this->micros += c.micros;
            this->bytesRead += c.bytesRead;
            this->bytesWritten += c.bytesWritten;
        }
    };

    CompactionStats stats[kNumLevels];

    // Information for a manual compaction
    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey *begin;   // null means beginning of key range
        const InternalKey *end;     // null means end of key range
        InternalKey tmpStorage;    // Used to keep track of compaction progress
    };

    ManualCompaction *manualCompaction;
    std::mutex mutex;
    std::condition_variable backgroundWorkFinishedSignal;
    Status bgerror;
};
