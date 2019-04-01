#include "logreader.h"
#include "tablebuilder.h"
#include "dbimpl.h"
#include "filename.h"
#include "coding.h"
#include "merger.h"
#include "dbiter.h"
#include "logging.h"

const int kNumNonTableCacheFiles = 10;

static int tableCacheSize(const Options &options) {
    // Reserve ten files or so for other uses and give the rest to TableCache.
    return kNumNonTableCacheFiles * kNumNonTableCacheFiles;
}

// Information kept for every waiting writer
struct DBImpl::Writer {
    Status status;
    WriteBatch *batch;
    bool sync;
    bool done;
};

struct DBImpl::CompactionState {
    Compaction *const compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    uint64_t smallestSnapshot;

    // Files produced by compaction
    struct Output {
        uint64_t number;
        uint64_t fileSize;
        InternalKey smallest, largest;
    };
    std::vector <Output> outputs;

    // State kept for output being generated
    std::shared_ptr <WritableFile> outfile;
    std::shared_ptr <TableBuilder> builder;

    uint64_t totalBytes;

    Output *currentOutput() { return &outputs[outputs.size() - 1]; }

    explicit CompactionState(Compaction *c)
            : compaction(c),
              outfile(nullptr),
              builder(nullptr),
              totalBytes(0) {

    }
};

struct IterState {
    std::shared_ptr <Version> const version;
    std::shared_ptr <MemTable> const mem;
    std::shared_ptr <MemTable> const imm;

    IterState(const std::shared_ptr <MemTable> &mem,
              const std::shared_ptr <MemTable> &imm,
              const std::shared_ptr <Version> &version)
            : version(version), mem(mem), imm(imm) {}
};

std::shared_ptr <Iterator> DBImpl::newInternalIterator(const ReadOptions &options,
                                                       uint64_t *latestSnapshot, uint32_t *seed) {
    *latestSnapshot = versions->getLastSequence();

    // Collect together all needed child iterators
    std::vector <std::shared_ptr<Iterator>> list;
    list.push_back(mem->newIterator());
    if (imm != nullptr) {
        list.push_back(imm->newIterator());
    }

    versions->current->addIterators(options, &list);
    std::shared_ptr <Iterator> internalIter =
            newMergingIterator(&comparator, list, list.size());

    std::shared_ptr <IterState> cleanup(new IterState(mem, imm, versions->current));
    internalIter->registerCleanup(cleanup);

    *seed = ++this->seed;
    return internalIter;
}

std::shared_ptr <Iterator> DBImpl::newIterator(const ReadOptions &options) {
    uint64_t latestSnapshot;
    uint32_t seed;
    std::shared_ptr <Iterator> iter = newInternalIterator(options, &latestSnapshot, &seed);
    return newDBIterator(this, &comparator, iter, latestSnapshot, seed);
}

Options DBImpl::sanitizeOptions(const std::string &dbname,
                                const InternalKeyComparator *icmp,
                                const Options &src) {
    Options result = src;
    result.comparator = icmp;
    return result;
}

DBImpl::DBImpl(const Options &op, const std::string &dbname)
        : comparator(op.comparator),
          options(sanitizeOptions(dbname, &comparator, op)),
          dbname(dbname),
          mem(nullptr),
          imm(nullptr),
          manualCompaction(nullptr),
          seed(0) {
    tableCache.reset(new TableCache(dbname, options, tableCacheSize(options)));
    versions.reset(new VersionSet(dbname, options, tableCache, &comparator));
}

DBImpl::~DBImpl() {

}

Status DBImpl::open() {
    VersionEdit edit;
    bool saveManifest = false;
    Status s = recover(&edit, &saveManifest);
    if (s.ok() && mem == nullptr) {
        uint64_t newLogNumber = versions->newFileNumber();
        s = options.env->newWritableFile(logFileName(dbname, newLogNumber), logfile);
        if (s.ok()) {
            log.reset(new LogWriter(logfile.get()));
            logfileNumber = newLogNumber;
            mem.reset(new MemTable(comparator));
        }
    }

    if (s.ok() && saveManifest) {
        edit.setPrevLogNumber(0);  // No older logs needed after recovery.
        edit.setLogNumber(logfileNumber);
        s = versions->logAndApply(&edit);
    }

    if (s.ok()) {
        deleteObsoleteFiles();
        maybeScheduleCompaction();
    } else {
        assert(false);
    }
    return s;
}

void DBImpl::deleteObsoleteFiles() {
    // Make a set of all of the live files
    std::set <uint64_t> live = pendingOutPuts;
    versions->addLiveFiles(&live);

    std::vector <std::string> filenames;
    options.env->getChildren(dbname, &filenames);  // Ignoring errors on purpose
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (parseFileName(filenames[i], &number, &type)) {
            bool keep = true;
            switch (type) {
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

            if (!keep) {
            	if (type == kTableFile) {
            		tableCache->evict(number);
            	}

                printf("Delete type=%d #%lld\n",
                       static_cast<int>(type),
                       static_cast<unsigned long long>(number));
                options.env->deleteFile(dbname + "/" + filenames[i]);
            }
        }
    }
}

Status DBImpl::newDB() {
    VersionEdit newdb;
    newdb.setLogNumber(0);
    newdb.setNextFile(2);
    newdb.setLastSequence(0);
    const std::string manifest = descriptorFileName(dbname, 1);
    std::shared_ptr <WritableFile> file;
    Status s = options.env->newWritableFile(manifest, file);
    if (!s.ok()) {
        return s;
    }

    LogWriter log(file.get());
    std::string record;
    newdb.encodeTo(&record);
    s = log.addRecord(record);
    if (s.ok()) {
        s = file->close();
    }

    if (s.ok()) {
        // Make "CURRENT" file that points to the new manifest file.
        s = setCurrentFile(options.env.get(), dbname, 1);
    } else {
        options.env->deleteFile(manifest);
    }
    return s;
}

Status DBImpl::recover(VersionEdit *edit, bool *saveManifest) {
    Status s;
    options.env->createDir(dbname);
    if (!options.env->fileExists(currentFileName(dbname))) {
        if (options.createIfMissing) {
            s = newDB();
            if (!s.ok()) {
                return s;
            }
        } else {
            return Status::invalidArgument(
                    dbname, "does not exist (create_if_missing is false)");
        }
    } else {
        if (options.errorIfExists) {
            return Status::invalidArgument(
                    dbname, "exists (error_if_exists is true)");
        }
    }

    s = versions->recover(saveManifest);
    if (!s.ok()) {
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

    std::vector <std::string> filenames;
    s = options.env->getChildren(dbname, &filenames);
    if (!s.ok()) {
        return s;
    }

    uint64_t maxSequence = 0;
    std::set <uint64_t> expected;
    versions->addLiveFiles(&expected);
    uint64_t number;

    FileType type;
    std::vector <uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (parseFileName(filenames[i], &number, &type)) {
            expected.erase(number);
            if (type == kLogFile && ((number >= minLog) || (number == prevLog))) {
                logs.push_back(number);
            }
        }
    }

    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
        s = recoverLogFile(logs[i], (i == logs.size() - 1), saveManifest, edit, &maxSequence);
        if (!s.ok()) {
            return s;
        }

        // The previous incarnation may not have written any MANIFEST
        // records after allocating this log number.  So we manually
        // update the file number allocation counter in VersionSet.
        versions->markFileNumberUsed(logs[i]);
    }

    if (versions->getLastSequence() < maxSequence) {
        versions->setLastSequence(maxSequence);
    }
    return s;
}

Status DBImpl::recoverLogFile(uint64_t logNumber, bool lastLog,
                              bool *saveManifest, VersionEdit *edit, uint64_t *maxSequence) {
    // Open the log file
    std::string fname = logFileName(dbname, logNumber);
    std::shared_ptr <PosixSequentialFile> file;
    Status status = options.env->newSequentialFile(fname, file);
    if (!status.ok()) {
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

    std::shared_ptr <MemTable> mem = nullptr;
    while (reader.readRecord(&record, &scratch) && status.ok()) {
        if (record.size() < 12) {
            reporter.corruption(record.size(), Status::corruption("log record too small"));
            continue;
        }

        if (mem == nullptr) {
            mem.reset(new MemTable(comparator));
        }

        WriteBatchInternal::setContents(&batch, record);
        status = WriteBatchInternal::insertInto(&batch, mem);
        if (!status.ok()) {
            break;
        }

        const uint64_t lastSeq = WriteBatchInternal::getSequence(&batch) + WriteBatchInternal::count(&batch) - 1;
        if (lastSeq > *maxSequence) {
            *maxSequence = lastSeq;
        }

        if (mem->getMemoryUsage() > options.writeBufferSize) {
            compactions++;
            *saveManifest = true;
            status = writeLevel0Table(mem, edit, nullptr);
            mem.reset();
            if (!status.ok()) {
                // Reflect errors immediately so that conditions like full
                // file-systems cause the DB::Open() to fail.
                break;
            }
        }
    }
    printf("logNumber %d# memtable:size %d\n", logNumber, mem == nullptr ? 0 : mem->getTableSize());

    // See if we should keep reusing the last log file.
    if (status.ok() && lastLog && compactions == 0) {
        uint64_t lfileSize;

        if (options.env->getFileSize(fname, &lfileSize).ok() &&
            options.env->newAppendableFile(fname, logfile).ok()) {
            printf("Reusing old log %s \n", fname.c_str());
            log.reset(new LogWriter(logfile.get(), lfileSize));
            logfileNumber = logNumber;
            if (mem != nullptr) {
                this->mem = mem;
                mem.reset();
            } else {
                // mem can be nullptr if lognum exists but was empty
                this->mem.reset(new MemTable(comparator));
            }
        }
    }

    if (mem != nullptr) {
        // mem did not get reused; compact it.
        if (status.ok()) {
            *saveManifest = true;
            status = writeLevel0Table(mem, edit, nullptr);
        }
    }
    return status;
}

Status DBImpl::put(const WriteOptions &opt, const std::string_view &key, const std::string_view &value) {
    WriteBatch batch;
    batch.put(key, value);
    return write(opt, &batch);
}

Status DBImpl::del(const WriteOptions &opt, const std::string_view &key) {
    WriteBatch batch;
    batch.del(key);
    return write(opt, &batch);
}

Status DBImpl::makeRoomForWrite(bool force) {
    assert(!writers.empty());
    bool allowDelay = !force;
    Status s;
    while (true) {
        if (!force && mem->getMemoryUsage() <= options.writeBufferSize) {
            break;
        } else {
            // Attempt to switch to a new memtable and trigger compaction of old
            assert(versions->getPrevLogNumber() == 0);
            uint64_t newLogNumber = versions->newFileNumber();
            std::shared_ptr <WritableFile> lfile;
            s = options.env->newWritableFile(logFileName(dbname, newLogNumber), lfile);
            if (!s.ok()) {
                // Avoid chewing through file number space in a tight loop.
                versions->reuseFileNumber(newLogNumber);
                break;
            }

            logfile = lfile;
            logfileNumber = newLogNumber;
            log.reset(new LogWriter(lfile.get()));
            imm = mem;
            mem.reset(new MemTable(comparator));
            force = false;   // Do not force another compaction if have room
            maybeScheduleCompaction();
        }
    }
    return s;
}

WriteBatch *DBImpl::buildBatchGroup(Writer **lastWriter) {
    assert(!writers.empty());
    Writer *first = writers.front();
    WriteBatch *result = first->batch;
    assert(result != nullptr);
    size_t size = WriteBatchInternal::byteSize(first->batch);
    size_t maxSize = 1 << 20;
    if (size <= (128 << 10)) {
        maxSize = size + (128 << 10);
    }

    *lastWriter = first;
    auto iter = writers.begin();
    ++iter;

    for (; iter != writers.end(); ++iter) {
        Writer *w = *iter;
        if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
        }

        if (w->batch != nullptr) {
            size += WriteBatchInternal::byteSize(w->batch);
            if (size > maxSize) {
                // Do not make batch too big
                break;
            }

            // Append to *result
            if (result == first->batch) {
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

Status DBImpl::destroyDB(const std::string &dbname, const Options &options) {
    auto env = options.env;
    std::vector <std::string> filenames;
    Status result = options.env->getChildren(dbname, &filenames);
    if (!result.ok()) {
        // Ignore error in case directory does not exist
        return Status::OK();
    }

    const std::string lockname = lockFileName(dbname);
    if (result.ok()) {
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (parseFileName(filenames[i], &number, &type) &&
                type != kDBLockFile) {     // Lock file will be deleted at end
                Status del = env->deleteFile(dbname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                    result = del;
                }
            }
        }

        env->deleteFile(lockname);
        env->deleteDir(dbname);  // Ignore error in case dir contains other files
    }
    return result;
}

Status DBImpl::write(const WriteOptions &opt, WriteBatch *myBatch) {
    Writer w;
    w.batch = myBatch;
    w.sync = opt.sync;
    w.done = false;
    writers.push_back(&w);
    // May temporarily unlock and wait.
    Status status = makeRoomForWrite(myBatch == nullptr);
    uint64_t lastSequence = versions->getLastSequence();
    Writer *lastWriter = &w;
    if (status.ok() && myBatch != nullptr) {
        WriteBatch *updates = buildBatchGroup(&lastWriter);
        WriteBatchInternal::setSequence(updates, lastSequence + 1);
        lastSequence += WriteBatchInternal::count(updates);
        status = log->addRecord(WriteBatchInternal::contents(updates));
        bool err = false;
        if (status.ok() && opt.sync) {
            status = logfile->sync();
            if (!status.ok()) {
                err = true;
            }
        }

        if (status.ok()) {
            status = WriteBatchInternal::insertInto(updates, mem);
        }

        if (updates == tmpBatch.get()) { tmpBatch->clear(); }
        versions->setLastSequence(lastSequence);
    }

    writers.pop_back();
    return status;
}

Status DBImpl::get(const ReadOptions &opt, const std::string_view &key, std::string *value) {
    Status s;
    uint64_t snapshot = versions->getLastSequence();
    bool haveStatUpdate = false;
    auto current = versions->current;
    Version::GetStats stats;

    LookupKey lkey(key, snapshot);
    if (mem->get(lkey, value, &s)) {
        // Done
    } else if (imm != nullptr && imm->get(lkey, value, &s)) {
        // Done
    } else {
        s = current->get(opt, lkey, value, &stats);
        haveStatUpdate = true;
    }

    if (haveStatUpdate && current->updateStats(stats)) {
        maybeScheduleCompaction();
    }
    return s;
}

Status DBImpl::writeLevel0Table(const std::shared_ptr <MemTable> &mem, VersionEdit *edit, Version *base) {
    const uint64_t startMicros = options.env->nowMicros();
    FileMetaData meta;
    meta.number = versions->newFileNumber();
    pendingOutPuts.insert(meta.number);
    printf("Level-0 table #%llu: started\n",
           (unsigned long long) meta.number);

    std::shared_ptr <Iterator> iter = mem->newIterator();
    Status s = buildTable(&meta, iter);
    printf("Level-0 table #%llu: %lld bytes %s\n", (unsigned long long) meta.number,
           (unsigned long long) meta.fileSize, s.toString().c_str());

    pendingOutPuts.erase(meta.number);

    // Note that if file_size is zero, the file has been deleted and
    // should not be added to the manifest.

    int level = 0;
    if (s.ok() && meta.fileSize > 0) {
        const std::string_view minUserKey = meta.smallest.userKey();
        const std::string_view maxUserKey = meta.largest.userKey();
        if (base != nullptr) {
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

void DBImpl::maybeScheduleCompaction() {
    while (1) {
        if (imm == nullptr &&
            manualCompaction == nullptr &&
            !versions->needsCompaction()) {
            // No work to be done
            break;
        } else {
            backgroundCompaction();
        }
    }
}

void DBImpl::backgroundCompaction() {
    if (imm != nullptr) {
        compactMemTable();
        return;
    }

    std::shared_ptr <Compaction> c;
    bool ismanual = (manualCompaction != nullptr);
    InternalKey manualend;
    if (ismanual) {
        auto m = manualCompaction;
        c = versions->compactRange(m->level, m->begin, m->end);
        m->done = (c == nullptr);
        if (c != nullptr) {
            manualend = c->input(0, c->numInputFiles(0) - 1)->largest;
        }

        printf("Manual compaction at level-%d from %s .. %s; will stop at %s\n",
               m->level,
               (m->begin ? m->begin->debugString().c_str() : "(begin)"),
               (m->end ? m->end->debugString().c_str() : "(end)"),
               (m->done ? "(end)" : manualend.debugString().c_str()));
    } else {
        c = versions->pickCompaction();
    }

    Status status;
    if (c == nullptr) {
        // Nothing to do
    } else if (!ismanual && c->isTrivialMove()) {
        // Move file to next level
        assert(c->numInputFiles(0) == 1);
        auto f = c->input(0, 0);
        c->getEdit()->deleteFile(c->getLevel(), f->number);
        c->getEdit()->addFile(c->getLevel() + 1, f->number, f->fileSize,
                              f->smallest, f->largest);
        status = versions->logAndApply(c->getEdit());
        assert(status.ok());

        VersionSet::LevelSummaryStorage tmp;
        printf("Moved #%lld to level-%d %lld bytes %s: %s\n",
               static_cast<unsigned long long>(f->number),
               c->getLevel() + 1,
               static_cast<unsigned long long>(f->fileSize),
               status.toString().c_str(),
               versions->levelSummary(&tmp));
    } else {
        std::shared_ptr <CompactionState> compact(new CompactionState(c.get()));
        status = doCompactionWork(compact.get());
        assert(status.ok());

        cleanupCompaction(compact.get());
        c->releaseInputs();
        deleteObsoleteFiles();
    }

    if (status.ok()) {
        // Done
    } else {
        printf("Compaction error: %s", status.toString().c_str());
    }

    if (ismanual) {
        auto m = manualCompaction;
        if (!status.ok()) {
            m->done = true;
        }

        if (!m->done) {
            // We only compacted part of the requested range.  Update *m
            // to the range that is left to be compacted.
            m->tmpStorage = manualend;
            m->begin = &m->tmpStorage;
        }
        manualCompaction = nullptr;
    }
}

void DBImpl::cleanupCompaction(CompactionState *compact) {
    if (compact->builder != nullptr) {
        // May happen if we get a shutdown call in the middle of compaction
        compact->builder->abandon();
		compact->builder.reset();
		compact->outfile.reset();
    } else {
        assert(compact->outfile == nullptr);
    }

    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output &out = compact->outputs[i];
        pendingOutPuts.erase(out.number);
    }
}

Status DBImpl::finishCompactionOutputFile(CompactionState *compact,
                                          const std::shared_ptr <Iterator> &input) {
    assert(compact != nullptr);
    assert(compact->outfile != nullptr);
    assert(compact->builder != nullptr);

    const uint64_t outputNumber = compact->currentOutput()->number;
    assert(outputNumber != 0);

    // Check for iterator errors
    Status s = input->status();
    const uint64_t currentEntries = compact->builder->numEntries();
    if (s.ok()) {
        s = compact->builder->finish();
    } else {
        compact->builder->abandon();
    }

    const uint64_t currentBytes = compact->builder->fileSize();
    compact->currentOutput()->fileSize = currentBytes;
    compact->totalBytes += currentBytes;
	compact->builder.reset();
    // Finish and check for file errors
    if (s.ok()) {
        s = compact->outfile->sync();
    }

    if (s.ok()) {
        s = compact->outfile->close();
    }
	
	compact->outfile.reset();

    if (s.ok() && currentEntries > 0) {
        // Verify that the table is usable
        std::shared_ptr <Iterator> iter = tableCache->newIterator(ReadOptions(),
                                                                  outputNumber,
                                                                  currentBytes);
        s = iter->status();
        if (s.ok()) {
            printf("Generated table #%llu@%d: %lld keys, %lld bytes",
                   (unsigned long long) outputNumber,
                   compact->compaction->getLevel(),
                   (unsigned long long) currentEntries,
                   (unsigned long long) currentBytes);
        }
    }
    return s;
}

Status DBImpl::doCompactionWork(CompactionState *compact) {
    const uint64_t startMicros = options.env->nowMicros();
    int64_t immMicros = 0;  // Micros spent doing imm_ compactions
    printf("Compacting %d@%d + %d@%d files",
           compact->compaction->numInputFiles(0),
           compact->compaction->getLevel(),
           compact->compaction->numInputFiles(1),
           compact->compaction->getLevel() + 1);

    assert(versions->numLevelFiles(compact->compaction->getLevel()) > 0);
    assert(compact->builder == nullptr);
    assert(compact->outfile == nullptr);

    compact->smallestSnapshot = versions->getLastSequence();
    std::shared_ptr <Iterator> input = versions->makeInputIterator(compact->compaction);
    input->seekToFirst();

    Status status;
    ParsedInternalKey ikey;
    std::string currentUserKey;
    bool hasCurrentUserKey = false;
    uint64_t lastSequenceForKey = kMaxSequenceNumber;
    for (; input->valid();) {
        const uint64_t immStart = options.env->nowMicros();
        if (imm != nullptr) {
            compactMemTable();
        }

        immMicros += (options.env->nowMicros() - immStart);
        std::string_view key = input->key();
        if (compact->compaction->shouldStopBefore(key) &&
            compact->builder != nullptr) {
            status = finishCompactionOutputFile(compact, input);
            if (!status.ok()) {
                break;
            }
        }
        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!parseInternalKey(key, &ikey)) {
            // Do not hide error keys
            currentUserKey.clear();
            hasCurrentUserKey = false;
            lastSequenceForKey = kMaxSequenceNumber;
        } else {
            if (!hasCurrentUserKey ||
                ikey.userKey.compare(std::string_view(currentUserKey)) != 0) {
                // First occurrence of this user key
                currentUserKey.assign(ikey.userKey.data(), ikey.userKey.size());
                hasCurrentUserKey = true;
                lastSequenceForKey = kMaxSequenceNumber;
            }

            if (lastSequenceForKey <= compact->smallestSnapshot) {
                // Hidden by an newer entry for same user key
                drop = true;    // (A)
            } else if (ikey.type == kTypeDeletion &&
                       ikey.sequence <= compact->smallestSnapshot &&
                       compact->compaction->isBaseLevelForKey(ikey.userKey)) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence numbers
                // (3) data in layers that are being compacted here and have
                //     smaller sequence numbers will be dropped in the next
                //     few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be dropped.
                drop = true;
            }
            lastSequenceForKey = ikey.sequence;
        }
#if 0
        printf("  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
            "%d smallest_snapshot: %d",
            ikey.userKey.toString().c_str(),
            (int)ikey.sequence, ikey.type, kTypeValue, drop,
            compact->compaction->isBaseLevelForKey(ikey.userKey),
            (int)lastSequenceForKey, (int)compact->smallestSnapshot);
#endif

        if (!drop) {
            // Open output file if necessary
            if (compact->builder == nullptr) {
                status = openCompactionOutputFile(compact);
                if (!status.ok()) {
                    break;
                }
            }

            if (compact->builder->numEntries() == 0) {
                compact->currentOutput()->smallest.decodeFrom(key);
            }

            compact->currentOutput()->largest.decodeFrom(key);
            compact->builder->add(key, input->value());

            // Close output file if it is big enough
            if (compact->builder->fileSize() >=
                compact->compaction->getMaxOutputFileSize()) {
                status = finishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }
        }
        input->next();
    }
	
    /*if (status.ok()) {
        status = Status::ioError("Deleting DB during compaction");
    }
	*/

    if (status.ok() && compact->builder != nullptr) {
        status = finishCompactionOutputFile(compact, input);
    }

    if (status.ok()) {
        status = input->status();
    }

	input.reset();

    CompactionStats stats;
    stats.micros = options.env->nowMicros() - startMicros - immMicros;
    for (int which = 0; which < 2; which++) {
        for (int i = 0; i < compact->compaction->numInputFiles(which); i++) {
            stats.bytesRead += compact->compaction->input(which, i)->fileSize;
        }
    }

    for (size_t i = 0; i < compact->outputs.size(); i++) {
        stats.bytesWritten += compact->outputs[i].fileSize;
    }

    this->stats[compact->compaction->getLevel() + 1].add(stats);
    if (status.ok()) {
        status = installCompactionResults(compact);
    }

    assert(status.ok());

    VersionSet::LevelSummaryStorage tmp;
    printf("compacted to: %s", versions->levelSummary(&tmp));
    return status;
}

Status DBImpl::openCompactionOutputFile(CompactionState *compact) {
    assert(compact != nullptr);
    assert(compact->builder == nullptr);
    uint64_t fileNumber;
    fileNumber = versions->newFileNumber();
    pendingOutPuts.insert(fileNumber);
    CompactionState::Output out;
    out.number = fileNumber;
    out.smallest.clear();
    out.largest.clear();
    compact->outputs.push_back(out);

    // Make the output file
    std::string fname = tableFileName(dbname, fileNumber);
    Status s = options.env->newWritableFile(fname, compact->outfile);
    if (s.ok()) {
        compact->builder.reset(new TableBuilder(options, compact->outfile));
    }
    return s;
}

Status DBImpl::installCompactionResults(CompactionState *compact) {
    printf("Compacted %d@%d + %d@%d files => %lld bytes",
           compact->compaction->numInputFiles(0),
           compact->compaction->getLevel(),
           compact->compaction->numInputFiles(1),
           compact->compaction->getLevel() + 1,
           static_cast<long long>(compact->totalBytes));

    // Add compaction outputs
    compact->compaction->addInputDeletions(compact->compaction->getEdit());
    const int level = compact->compaction->getLevel();
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output &out = compact->outputs[i];
        compact->compaction->getEdit()->addFile(
                level + 1,
                out.number, out.fileSize, out.smallest, out.largest);
    }
    return versions->logAndApply(compact->compaction->getEdit());
}

void DBImpl::compactMemTable() {
    assert(imm != nullptr);
    // Save the contents of the memtable as a new Table
    VersionEdit edit;
    auto base = versions->current;
    Status s = writeLevel0Table(imm, &edit, base.get());

    // Replace immutable memtable with the generated Table
    if (s.ok()) {
        edit.setPrevLogNumber(0);
        edit.setLogNumber(logfileNumber);  // Earlier logs no longer needed
        s = versions->logAndApply(&edit);
    }

    if (s.ok()) {
        imm.reset();
        deleteObsoleteFiles();
    } else {
		assert(false);
    }
}

Status DBImpl::buildTable(FileMetaData *meta, const std::shared_ptr <Iterator> &iter) {
    Status s;
    meta->fileSize = 0;
    iter->seekToFirst();

    std::string fname = tableFileName(dbname, meta->number);
    std::shared_ptr <WritableFile> file;
    s = options.env->newWritableFile(fname, file);
    if (!s.ok()) {
        return s;
    }

    std::shared_ptr <TableBuilder> builder(new TableBuilder(options, file));
    meta->smallest.decodeFrom(iter->key());
    for (; iter->valid(); iter->next()) {
        std::string_view key = iter->key();
        meta->largest.decodeFrom(key);
        builder->add(key, iter->value());
    }

    // Finish and check for builder errors
    s = builder->finish();
    if (s.ok()) {
        meta->fileSize = builder->fileSize();
        assert(meta->fileSize > 0);
    }

    // Finish and check for file errors
    if (s.ok()) {
        s = file->sync();
    }

    if (s.ok()) {
        s = file->close();
    }

    if (s.ok()) {
        // Verify that the table is usable
        std::shared_ptr <Iterator> it = tableCache->newIterator(ReadOptions(),
                                                                meta->number,
                                                                meta->fileSize);
        s = it->status();
    }

    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }

    if (s.ok() && meta->fileSize > 0) {
        // Keep it
    } else {
        options.env->deleteFile(fname);
    }
    return s;
}

std::shared_ptr <Iterator> DBImpl::testNewInternalIterator() {
    uint64_t ignored;
    uint32_t seed;
    return newInternalIterator(ReadOptions(), &ignored, &seed);
}

Status DBImpl::testCompactMemTable() {
    // nullptr batch means just wait for earlier writes to be done
    Status s = write(WriteOptions(), nullptr);
    if (s.ok()) {

    }
    return s;
}

void DBImpl::getApproximateSizes(const Range *range, int n, uint64_t *sizes) {

}

bool DBImpl::getProperty(const std::string_view &property, std::string *value) {
    value->clear();
    std::string_view in = property;
    std::string_view prefix("leveldb.");
    if (!startsWith(in, prefix)) {
        return false;
    }

    in.remove_prefix(prefix.size());
    if (startsWith(in, "num-files-at-level")) {
        in.remove_prefix(strlen("num-files-at-level"));
        uint64_t level;
        bool ok = consumeDecimalNumber(&in, &level) && in.empty();
        if (!ok || level >= kNumLevels) {
            return false;
        } else {
            char buf[100];
            snprintf(buf, sizeof(buf), "%d",
                     versions->numLevelFiles(static_cast<int>(level)));
            *value = buf;
            return true;
        }
    } else if (in == "status") {
        char buf[200];
        snprintf(buf, sizeof(buf),
                 "                               Compactions\n"
                 "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                 "--------------------------------------------------\n"
        );

        value->append(buf);
        for (int level = 0; level < kNumLevels; level++) {
            int files = versions->numLevelFiles(level);
            if (stats[level].micros > 0 || files > 0) {
                snprintf(
                        buf, sizeof(buf),
                        "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                        level,
                        files,
                        versions->numLevelBytes(level) / 1048576.0,
                        stats[level].micros / 1e6,
                        stats[level].bytesRead / 1048576.0,
                        stats[level].bytesWritten / 1048576.0);
                value->append(buf);
            }
        }
        return true;
    } else if (in == "sstables") {
        *value = versions->current->debugString();
    } else if (in == "approximate-memory-usage") {
        size_t totalUsage = tableCache->getCache()->totalCharge();
        if (mem) {
            totalUsage += mem->getMemoryUsage();
        }

        if (imm) {
            totalUsage += imm->getMemoryUsage();
        }

        char buf[50];
        snprintf(buf, sizeof(buf), "%llu",
                 static_cast<unsigned long long>(totalUsage));
        value->append(buf);
        return true;
    }
    return false;
}
