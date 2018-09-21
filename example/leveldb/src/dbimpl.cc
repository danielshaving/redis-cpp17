#include "dbimpl.h"
#include "filename.h"
#include "log-reader.h"
#include "table-builder.h"
#include "coding.h"

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer
{
	Status status;
	WriteBatch *batch;
	bool sync;
	bool done;
};

DBImpl::DBImpl(const Options &options, const std::string &dbname)
	:options(options),
	dbname(dbname)
{
	versions.reset(new VersionSet(dbname, options));
	mem.reset(new MemTable);
	imm.reset(new MemTable);
}

DBImpl::~DBImpl()
{

}

Status DBImpl::open()
{
	Status s = options.env->createDir(dbname);
	VersionEdit edit;
	bool saveManifest = false;
	s = recover(&edit, &saveManifest);
	if (s.ok() && imm->getMemoryUsage() == 0)
	{
		uint64_t newLogNumber = versions->newFileNumber();
		s = options.env->newWritableFile(logFileName(dbname, newLogNumber), logfile);
		if (s.ok())
		{
			log.reset(new LogWriter(logfile.get()));
			logfileNumber = newLogNumber;
		}
	}

	if (s.ok() && saveManifest)
	{
		edit.setPrevLogNumber(0);  // No older logs needed after recovery.
		edit.setLogNumber(logfileNumber);
		s = versions->logAndApply(&edit);
	}

	if (s.ok())
	{
		deleteObsoleteFiles();
	}
	else
	{
		//RecordBackgroundError(s);
	}
	return s;
}

void DBImpl::deleteObsoleteFiles()
{
	// Make a set of all of the live files
	std::set<uint64_t> live = pendingOutPuts;
	versions->addLiveFiles(&live);

	std::vector<std::string> filenames;
	options.env->getChildren(dbname, &filenames);  // Ignoring errors on purpose
	uint64_t number;
	FileType type;
	for (size_t i = 0; i < filenames.size(); i++)
	{
		if (parseFileName(filenames[i], &number, &type))
		{
			bool keep = true;
			switch (type)
			{
			case kLogFile:
				keep = ((number >= versions->getLogNumber()) ||
					(number == versions->getPrevLogNumber()));
				break;
			case kDescriptorFile:
				// Keep my manifest file, and any newer incarnations'
				// (in case there is a race that allows other incarnations)
				keep = (number >= versions->getManifestFileNumber());
				break;
			case kTableFile:
				keep = (live.find(number) != live.end());
				break;
			case kTempFile:
				// Any temp files that are currently being written to must
				// be recorded in pending_outputs_, which is inserted into "live"
				keep = (live.find(number) != live.end());
				break;
			case kCurrentFile:
			case kDBLockFile:
			case kInfoLogFile:
				keep = true;
				break;
			}

			if (!keep)
			{
				printf("Delete type=%d #%lld\n",
					static_cast<int>(type),
					static_cast<unsigned long long>(number));
				options.env->deleteFile(dbname + "/" + filenames[i]);
			}
		}
	}
}

Status DBImpl::newDB()
{
	VersionEdit newdb;
	newdb.setLogNumber(0);
	newdb.setNextFile(2);
	newdb.setLastSequence(0);
	const std::string manifest = descriptorFileName(dbname, 1);
	std::shared_ptr<PosixWritableFile> file;
	Status s = options.env->newWritableFile(manifest, file);
	if (!s.ok())
	{
		return s;
	}

	LogWriter log(file.get());
	std::string record;
	newdb.encodeTo(&record);
	s = log.addRecord(record);
	if (s.ok())
	{
		s = file->close();
	}

	if (s.ok())
	{
		// Make "CURRENT" file that points to the new manifest file.
		s = setCurrentFile(options.env, dbname, 1);
	}
	else
	{
		options.env->deleteFile(manifest);
	}
	return s;
}

Status DBImpl::recover(VersionEdit *edit, bool *saveManifest)
{
	Status s;
	options.env->createDir(dbname);

	if (!options.env->fileExists(currentFileName(dbname)))
	{
		if (options.createIfMissing)
		{
			s = newDB();
			if (!s.ok())
			{
				return s;
			}
		}
		else
		{
			return Status::invalidArgument(
				dbname, "does not exist (create_if_missing is false)");
		}
	}
	else
	{
		if (options.errorIfExists)
		{
			return Status::invalidArgument(
				dbname, "exists (error_if_exists is true)");
		}
	}

	s = versions->recover(saveManifest);
	if (!s.ok())
	{
		return s;
	}

	// Recover from all newer log files than the ones named in the
	// descriptor (new log files may have been added by the previous
	// incarnation without registering them in the descriptor).
	//
	// Note that PrevLogNumber() is no longer used, but we pay
	// attention to it in case we are recovering a database
	// produced by an older version of leveldb.

	const uint64_t minLog = versions->getLogNumber();
	const uint64_t prevLog = versions->getPrevLogNumber();

	std::vector<std::string> filenames;
	s = options.env->getChildren(dbname, &filenames);
	if (!s.ok())
	{
		return s;
	}

	uint64_t maxSequence(0);
	std::set<uint64_t> expected;
	versions->addLiveFiles(&expected);
	uint64_t number;

	FileType type;
	std::vector<uint64_t> logs;
	for (size_t i = 0; i < filenames.size(); i++)
	{
		if (parseFileName(filenames[i], &number, &type))
		{
			expected.erase(number);
			if (type == kLogFile && ((number >= minLog) || (number == prevLog)))
			{
				logs.push_back(number);
			}
		}
	}

	std::sort(logs.begin(), logs.end());
	for (size_t i = 0; i < logs.size(); i++)
	{
		s = recoverLogFile(logs[i], (i == logs.size() - 1), saveManifest, edit, &maxSequence);
		if (!s.ok())
		{
			return s;
		}

		// The previous incarnation may not have written any MANIFEST
		// records after allocating this log number.  So we manually
		// update the file number allocation counter in VersionSet.
		versions->markFileNumberUsed(logs[i]);
	}

	if (versions->getLastSequence() < maxSequence)
	{
		versions->setLastSequence(maxSequence);
	}
	return s;
}

Status DBImpl::recoverLogFile(uint64_t logNumber, bool lastLog,
	bool *saveManifest, VersionEdit *edit, uint64_t *maxSequence)
{
	// Open the log file
	std::string fname = logFileName(dbname, logNumber);
	std::shared_ptr<PosixSequentialFile> file;
	Status status = options.env->newSequentialFile(fname, file);
	if (!status.ok())
	{
		//MaybeIgnoreError(&status);
		return status;
	}

	LogReporter reporter;
	// We intentionally make log::Reader do checksumming even if
	// paranoid_checks==false so that corruptions cause entire commits
	// to be skipped instead of propagating bad information (like overly
	// large sequence numbers).
	LogReader reader(file.get(), &reporter, true/*checksum*/, 0/*initial_offset*/);
	std::string scratch;
	std::string_view record;
	WriteBatch batch;
	int compactions = 0;

	std::shared_ptr<MemTable> mem = nullptr;
	  
	while (reader.readRecord(&record, &scratch) && status.ok())
	{
		if (record.size() < 12)
		{
			reporter.corruption(record.size(), Status::corruption("log record too small"));
			continue;
		}
		
		if (mem == nullptr)
		{
			mem.reset(new MemTable);
		}

		WriteBatchInternal::setContents(&batch, record);
		status = WriteBatchInternal::insertInto(&batch, mem);
		if (!status.ok())
		{
			break;
		}

		const uint64_t lastSeq = WriteBatchInternal::getSequence(&batch) + WriteBatchInternal::count(&batch) - 1;
		if (lastSeq > *maxSequence)
		{
			*maxSequence = lastSeq;
		}
		
		if (mem->getMemoryUsage() > options.writeBufferSize) 
		{
			compactions++;
			*saveManifest = true;
			status = writeLevel0Table(edit, nullptr);
			mem.reset();
			if (!status.ok()) 
			{
				// Reflect errors immediately so that conditions like full
				// file-systems cause the DB::Open() to fail.
				break;
			}
		}
	}
	printf("logNumber %d# memtable:size %d\n", logNumber, mem->getTableSize());
	
	// See if we should keep reusing the last log file.
	if (status.ok() && lastLog && compactions == 0) 
	{
		 uint64_t lfileSize;
		 
		if (options.env->getFileSize(fname, &lfileSize).ok() &&
			options.env->newAppendableFile(fname, logfile).ok()) 
		{
			printf("Reusing old log %s \n", fname.c_str());
			logfileNumber = logNumber;
			if (mem != nullptr) 
			{
				this->mem = mem;
				mem.reset();
			} 
			else 
			{
				// mem can be nullptr if lognum exists but was empty
			}
		}
	}
	
	if (mem != nullptr) 
	{
		// mem did not get reused; compact it.
		if (status.ok()) 
		{
			*saveManifest = true;
			status = writeLevel0Table(edit, nullptr);
		}
		mem.reset();
	}	
	return status;
}

Status DBImpl::put(const WriteOptions &opt, const std::string_view &key, const std::string_view &value)
{
	WriteBatch batch;
	batch.put(key, value);
	return write(opt, &batch);
}

Status DBImpl::del(const WriteOptions &opt, const std::string_view &key)
{
	WriteBatch batch;
	batch.del(key);
	return write(opt, &batch);
}

Status DBImpl::makeRoomForWrite(bool force)
{
	assert(!writers.empty());
	bool allowDelay = !force;
	Status s;
	while (true)
	{
		if (!force && mem->getMemoryUsage() <= options.writeBufferSize)
		{
			break;
		}
		else if (imm != nullptr)
		{

		}
		else if (versions->numLevelFiles(0) >= kL0_StopWritesTrigger)
		{	
			
		}
		else
		{
			// Attempt to switch to a new memtable and trigger compaction of old
   			assert(versions->getPrevLogNumber() == 0);
   			uint64_t newLogNumber = versions->newFileNumber();	
   			std::shared_ptr<PosixWritableFile> lfile;
   			s = options.env->newWritableFile(logFileName(dbname, newLogNumber), lfile);
			if (!s.ok()) 
			{
				// Avoid chewing through file number space in a tight loop.
				versions->reuseFileNumber(newLogNumber);
				break;
			}

			logfile = lfile;
			logfileNumber = newLogNumber;
			log.reset(new LogWriter(lfile.get()));
			imm = mem;
			mem.reset(new MemTable());
			force = false;   // Do not force another compaction if have room
			maybeScheduleCompaction();
		}
	}
	return s;
}

WriteBatch *DBImpl::buildBatchGroup(Writer **lastWriter)
{
	assert(!writers.empty());
	Writer *first = writers.front();
	WriteBatch *result = first->batch;
	assert(result != nullptr);
	size_t size = WriteBatchInternal::byteSize(first->batch);
	size_t maxSize = 1 << 20;
	if (size <= (128 << 10))
	{
		maxSize = size + (128 << 10);
	}

	*lastWriter = first;
	auto iter = writers.begin();
	++iter;

	for (; iter != writers.end(); ++iter)
	{
		Writer *w = *iter;
		if (w->sync && !first->sync)
		{
			// Do not include a sync write into a batch handled by a non-sync write.
			break;
		}

		if (w->batch != nullptr)
		{
			size += WriteBatchInternal::byteSize(w->batch);
			if (size > maxSize)
			{
				// Do not make batch too big
				break;
			}

			// Append to *result
			if (result == first->batch)
			{
				// Switch to temporary batch instead of disturbing caller's batch
				result = tmpBatch.get();
				assert(WriteBatchInternal::count(result) == 0);
				WriteBatchInternal::append(result, first->batch);
			}
			WriteBatchInternal::append(result, w->batch);
		}
		*lastWriter = w;
	}
	return result;
}

Status DBImpl::write(const WriteOptions &opt, WriteBatch *myBatch)
{
	Writer w;
	w.batch = myBatch;
	w.sync = opt.sync;
	w.done = false;
	writers.push_back(&w);
	// May temporarily unlock and wait.
	Status status = makeRoomForWrite(myBatch == nullptr);
	uint64_t lastSequence = versions->getLastSequence();
	Writer *lastWriter = &w;
	if (status.ok() && myBatch != nullptr)
	{
		WriteBatch *updates = buildBatchGroup(&lastWriter);
		WriteBatchInternal::setSequence(updates, lastSequence + 1);
		lastSequence += WriteBatchInternal::count(updates);
		status = log->addRecord(WriteBatchInternal::contents(updates));
		bool err = false;
		if (status.ok() && opt.sync)
		{
			status = logfile->sync();
			if (!status.ok())
			{
				err = true;
			}
		}

		if (status.ok())
		{
			status = WriteBatchInternal::insertInto(updates, mem);
		}

		if (updates == tmpBatch.get()) { tmpBatch->clear(); }
		versions->setLastSequence(lastSequence);
	}

	writers.pop_back();
	return status;
}

Status DBImpl::get(const ReadOptions &opt, const std::string_view &key, std::string *value)
{
	Status s;
	uint64_t snapshot = versions->getLastSequence();
	LookupKey lkey(key, snapshot);
	if (mem->get(lkey, value, &s))
	{
		// Done
	}
	else if (imm != nullptr && imm->get(lkey, value, &s))
	{
		// Done
	}
	else
	{
		//	  s = current->Get(options, lkey, value, &stats);
		//	  have_stat_update = true;
		s = Status::notFound(std::string_view());
	}
	return s;
}

Status DBImpl::writeLevel0Table(VersionEdit *edit, Version *base)
{
	const uint64_t startMicros = options.env->nowMicros();
	FileMetaData meta;
	meta.number = versions->newFileNumber();
	pendingOutPuts.insert(meta.number);
	printf("Level-0 table #%llu: started",
		(unsigned long long) meta.number);

	Status s = buildTable(&meta);
	printf("Level-0 table #%llu: %lld bytes %s", (unsigned long long) meta.number,
		(unsigned long long) meta.fileSize, s.toString().c_str());
		
	pendingOutPuts.erase(meta.number);
	
	// Note that if file_size is zero, the file has been deleted and
	// should not be added to the manifest.
	
	int level = 0;
	if (s.ok() && meta.fileSize > 0) 
	{
		const std::string_view minUserKey = meta.smallest.userKey();
		const std::string_view maxUserKey = meta.largest.userKey();
		if (base != nullptr) 
		{
			level = base->pickLevelForMemTableOutput(minUserKey, maxUserKey);
		}
		
		edit->addFile(level, meta.number, meta.fileSize,
				  meta.smallest, meta.largest);
	}
	
	CompactionStats sta;
	sta.micros = options.env->nowMicros() - startMicros;
	sta.bytesWritten = meta.fileSize;
	stats[level].add(sta);
	return s;
}

void DBImpl::maybeScheduleCompaction()
{
	while (1)
	{
		if (imm == nullptr &&
			 manualCompaction == nullptr &&
			 !versions->needsCompaction()) 

		{
			 // No work to be done
			break;
		}
		else
		{
			backgroundCompaction();
		}
	}			 
}

void DBImpl::backgroundCompaction() 
{
	if (imm != nullptr) 
	{
		compactMemTable();
		return;
	}
}

void DBImpl::compactMemTable() 
{
	assert(imm != nullptr);
	// Save the contents of the memtable as a new Table
	VersionEdit edit;
	auto base = versions->current();
	Status s = writeLevel0Table(&edit, base.get());

	// Replace immutable memtable with the generated Table
	if (s.ok()) 
	{
		edit.setPrevLogNumber(0);
		edit.setLogNumber(logfileNumber);  // Earlier logs no longer needed
		s = versions->logAndApply(&edit);
	}

	if (s.ok()) 
	{
		imm.reset();
		deleteObsoleteFiles();
	}
	else 
	{
		//RecordBackgroundError(s);
	}
}

Status DBImpl::buildTable(FileMetaData *meta)
{
	assert(mem->getMemoryUsage() > 0);
	Status s;
	meta->fileSize = 0;
	
	std::string fname = tableFileName(dbname, meta->number);
	std::shared_ptr<PosixWritableFile> file;
	s = options.env->newWritableFile(fname, file);
	if (!s.ok())
	{
		return s;
	}
	
	auto &table = mem->getTable();
	std::shared_ptr<TableBuilder> builder(new TableBuilder(options, file.get()));
	auto iter = table.begin();
	
	const char *entry = *iter;
	uint32_t keyLength;
	const char *keyPtr = getVarint32Ptr(entry, entry + 5, &keyLength);
	std::string_view key = std::string_view(keyPtr, keyLength - 8);
	meta->smallest.decodeFrom(key);
	
	for (auto &it : table)
	{
		const char *entry = it;
		const char *keyPtr = getVarint32Ptr(entry, entry + 5, &keyLength);
		uint32_t keyLength;
		std::string_view key = std::string_view(keyPtr, keyLength - 8);
		meta->largest.decodeFrom(key);
		
		std::string_view value = getLengthPrefixedSlice(keyPtr + keyLength);
		builder->add(key, value);
	}
	
	 // Finish and check for builder errors
    s = builder->finish();
    if (s.ok()) 
	{
		meta->fileSize = builder->fileSize();
		assert(meta->fileSize > 0);
    }
	
	// Finish and check for file errors
    if (s.ok()) 
	{
		s = file->sync();
    }
	
    if (s.ok()) 
	{
		s = file->close();
    }
	
	if (s.ok() && meta->fileSize > 0) 
	{
		// Keep it
	} 
	else 
	{
		options.env->deleteFile(fname);
	}
	return s;
}
