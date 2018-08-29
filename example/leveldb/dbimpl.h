#pragma once
#include <deque>
#include <set>
#include <memory>
#include "option.h"
#include "status.h"
#include "writebatch.h"
#include "log-writer.h"
#include "posix.h"
#include "version-edit.h"
#include "version-set.h"

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
	// Implementations of the DB interface

	void deleteObsoleteFiles();
	Status recover(VersionEdit *edit, bool *saveManifest);
	Status recoverLogFile(uint64_t logNumber, bool lastLog,
		bool *saveManifest, VersionEdit *edit, uint64_t *maxSequence);
	Status newDB();
	Status writeLevel0Table(VersionEdit *edit, Version *base);
	Status buildTable(FileMetaData *meta);

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
	const Options options;  // options_.comparator == &internal_comparator_
	uint64_t logfileNumber;
	const std::string dbname;

	// Set of table files to protect from deletion because they are
	// part of ongoing compactions.
	std::set<uint64_t> pendingOutPuts;
};
