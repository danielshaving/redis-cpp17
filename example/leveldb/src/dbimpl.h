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
#include "posix.h"
#include "tablecache.h"

class DBImpl
{
public:
	DBImpl(const Options &options, const std::string &dbname);
	~DBImpl();

	Status open();
	Status put(const WriteOptions&, const std::string_view &key, const std::string_view &value);
	Status del(const WriteOptions&, const std::string_view &key);
	Status write(const WriteOptions &options, WriteBatch *updates);
	Status get(const ReadOptions &options, const std::string_view &key, std::string *value);

	Status destroyDB(const std::string &dbname, const Options &options);
	void deleteObsoleteFiles();
	Status recover(VersionEdit *edit, bool *saveManifest);
	Status recoverLogFile(uint64_t logNumber, bool lastLog,
		bool *saveManifest, VersionEdit *edit, uint64_t *maxSequence);
	Status newDB();
	Status writeLevel0Table(VersionEdit *edit, Version *base);
	Status buildTable(FileMetaData *meta);
	void maybeScheduleCompaction();
	void backgroundCompaction();
	void compactMemTable();

private:
	struct Writer;
	// No copying allowed
	DBImpl(const DBImpl&);
	void operator=(const DBImpl&);

	Status makeRoomForWrite(bool force /* compact even if there is room? */);
	WriteBatch *buildBatchGroup(Writer **lastWriter);
	// Queue of writers.
	std::deque<Writer*> writers;
	std::shared_ptr<WriteBatch>	tmpBatch;
	std::shared_ptr<MemTable> mem;
	std::shared_ptr<MemTable> imm;
	std::shared_ptr<VersionSet> versions;
	std::shared_ptr<LogWriter> log;
	std::shared_ptr<PosixWritableFile> logfile;
	std::shared_ptr<TableCache> tableCache;
	const Options options;  // options_.comparator == &internal_comparator_
	uint64_t logfileNumber;
	const std::string dbname;

	// Set of table files to protect from deletion because they are
	// part of ongoing compactions.
	std::set<uint64_t> pendingOutPuts;
	const InternalKeyComparator comparator;
	// Per level compaction stats.  stats_[level] stores the stats for
	// compactions that produced data for the specified "level".
	struct CompactionStats 
	{
		int64_t micros;
		int64_t bytesRead;
		int64_t bytesWritten;

		CompactionStats() : micros(0), bytesRead(0), bytesWritten(0) { }

		void add(const CompactionStats &c) 
		{
			this->micros += c.micros;
			this->bytesRead += c.bytesRead;
			this->bytesWritten += c.bytesWritten;
		}
	};
	CompactionStats stats[kNumLevels];
	
	// Information for a manual compaction
	struct ManualCompaction 
	{
		int level;
		bool done;
		const InternalKey *begin;   // null means beginning of key range
		const InternalKey *end;     // null means end of key range
		InternalKey tmpStorage;    // Used to keep track of compaction progress
	};
	
	std::shared_ptr<ManualCompaction> manualCompaction;
	std::mutex mutex;
	std::condition_variable condition;
	Status bgerror;
};
