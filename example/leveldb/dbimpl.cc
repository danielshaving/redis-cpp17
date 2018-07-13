#include "dbimpl.h"
#include "filename.h"
#include "log-reader.h"

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer 
{
	Status status;
	WriteBatch *batch;
	bool sync;
	bool done;
};

DBImpl::DBImpl(const Options &options,const std::string &dbname)
:options(options),
 dbname(dbname)
{

}

DBImpl::~DBImpl()
{

}

Status DBImpl::open()
{
	Status s = options.env->createDir(dbname);
	printf("%s\n",s.toString().c_str());
	versions.reset(new VersionSet(dbname,&options));
	mem.reset(new MemTable);
	imm.reset(new MemTable);
	VersionEdit edit;
	bool saveManifest = false;
	s = recover(&edit,&saveManifest);
	if (s.ok())
	{
		uint64_t newLogNumber = versions->newFileNumber();
		std::shared_ptr<PosixWritableFile> r;
		s = options.env->newWritableFile(logFileName(dbname,newLogNumber),r);
		if (s.ok())
		{
			logfile = r;
			log.reset(new LogWriter(logfile.get()));
			logfileNumber = newLogNumber;
		}
	}
	return s;
}

Status DBImpl::recover(VersionEdit *edit,bool *saveManifest)
{
	Status s;
	options.env->createDir(dbname);
//	s = versions->recover(saveManifest);
//	if (!s.ok())
//	{
//		return s;
//	}

	// Recover from all newer log files than the ones named in the
	// descriptor (new log files may have been added by the previous
	// incarnation without registering them in the descriptor).
	//
	// Note that PrevLogNumber() is no longer used, but we pay
	// attention to it in case we are recovering a database
	// produced by an older version of leveldb.

	std::vector<std::string> filenames;
	s = options.env->getChildren(dbname,&filenames);
	if (!s.ok())
	{
		return s;
	}

	uint64_t maxSequence(0);
	FileType type;
	std::vector<uint64_t> logs;
	uint64_t number;
	for (size_t i = 0; i < filenames.size(); i++)
	{
		if (parseFileName(filenames[i],&number,&type))
		{
			 if (type == kLogFile)
			 {
				 logs.push_back(number);
			 }
		}
	}

	std::sort(logs.begin(),logs.end());
	for (size_t i = 0; i < logs.size(); i++)
	{
		s = recoverLogFile(logs[i],(i == logs.size() - 1),saveManifest,edit,&maxSequence);
		if (!s.ok())
		{
			return s;
		}
	}
}

Status DBImpl::recoverLogFile(uint64_t logNumber,bool lastLog,
		bool *saveManifest,VersionEdit *edit,uint64_t *maxSequence)
{
	// Open the log file
	std::string fname = logFileName(dbname,logNumber);
	std::shared_ptr<PosixSequentialFile> file;
	Status status = options.env->newSequentialFile(fname,file);
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
	Reader reader(file.get(),&reporter,true/*checksum*/,0/*initial_offset*/);
	std::string scratch;
	std::string_view record;
	WriteBatch batch;
	int compactions = 0;

	while (reader.readRecord(&record,&scratch) && status.ok())
	{
	    if (record.size() < 12)
	    {
			reporter.corruption(record.size(),Status::corruption("log record too small"));
			continue;
	    }

	    WriteBatchInternal::setContents(&batch,record);
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
	}
}

Status DBImpl::put(const WriteOptions &opt,const std::string_view &key,const std::string_view &value)
{
    WriteBatch batch;
    batch.put(key,value);
    return write(opt,&batch);
}

Status DBImpl::del(const WriteOptions &opt,const std::string_view &key)
{
	WriteBatch batch;
	batch.del(key);
	return write(opt,&batch);
}

Status DBImpl::makeRoomForWrite(bool force)
{
    assert(!writers.empty());
    bool allowDelay = !force;
    Status s;
    while(true)
    {
        if (!force && mem->getMemoryUsage() <= options.writeBufferSize)
        {
            break;
        }
        else if (imm != nullptr)
        {

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
	if (size <= (128<<10))
	{
		maxSize = size + (128<<10);
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
				WriteBatchInternal::append(result,first->batch);
			}
			WriteBatchInternal::append(result,w->batch);
		}
		*lastWriter = w;
	}
	return result;
}

Status DBImpl::write(const WriteOptions &opt,WriteBatch *myBatch)
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
		WriteBatchInternal::setSequence(updates,lastSequence + 1);
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
			status = WriteBatchInternal::insertInto(updates,mem);
		}

		if (updates == tmpBatch.get()) { tmpBatch->clear(); }
		versions->setLastSequence(lastSequence);
    }

    writers.pop_back();
    return status;
}

Status DBImpl::get(const ReadOptions &opt,const std::string_view &key,std::string *value)
{
	Status s;
	uint64_t snapshot = versions->getLastSequence();
	LookupKey lkey(key,snapshot);
	if (mem->get(lkey,value,&s))
	{
	  // Done
	}
	else if (imm != nullptr && imm->get(lkey,value,&s))
	{
	  // Done
	}
	else
	{
//	  s = current->Get(options, lkey, value, &stats);
//	  have_stat_update = true;
	}
	return s;
}

