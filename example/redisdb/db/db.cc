#include "logreader.h"
#include "tablebuilder.h"
#include "filename.h"
#include "coding.h"
#include "db.h"
#include "merger.h"
#include "dbiter.h"
#include "logging.h"

const int kNumNonTableCacheFiles = 10;

static int tableCacheSize(const Options& options) {
	// Reserve ten files or so for other uses and give the rest to TableCache.
	return kNumNonTableCacheFiles * kNumNonTableCacheFiles;
}

// Information kept for every waiting writer
struct DB::Writer {
	Status status;
	WriteBatch* batch;
	bool sync;
	bool done;
	std::condition_variable cv;
};

struct DB::CompactionState {
	Compaction* const compaction;

	// Sequence numbers< smallest_snapshot are not significant since we
	// will never have to service a snapshot below smallest_snapshot.
	// Therefore if we have seen a sequence number S<= smallest_snapshot,
	// we can drop all entries for the same key with sequence numbers< S.
	uint64_t smallestSnapshot;

	// Files produced by compaction
	struct Output {
		uint64_t number;
		uint64_t FileSize;
		InternalKey smallest, largest;
	};
	std::vector<Output> outputs;

	// State kept for output being generated
	std::shared_ptr<WritableFile> outfile;
	std::shared_ptr<TableBuilder> builder;

	uint64_t totalBytes;

	Output* currentOutput() { return &outputs[outputs.size() - 1]; }

	explicit CompactionState(Compaction* c)
		: compaction(c),
		outfile(nullptr),
		builder(nullptr),
		totalBytes(0) {

	}
};

static void CleanupIteratorState(const std::any& arg) {

}

struct IterState {
	std::shared_ptr<Version> const version;
	std::shared_ptr<MemTable> const mem;
	std::shared_ptr<MemTable> const imm;

	IterState(const std::shared_ptr<MemTable>& mem,
		const std::shared_ptr<MemTable>& imm,
		const std::shared_ptr<Version>& version)
		: version(version), mem(mem), imm(imm) {}
};

std::shared_ptr<Iterator> DB::NewInternalIterator(const ReadOptions& options,
	uint64_t* latestSnapshot, uint32_t* seed) {
	mutex.lock();
	*latestSnapshot = versions->GetLastSequence();

	// Collect together all needed child iterators
	std::vector<std::shared_ptr<Iterator>> list;
	list.push_back(mem->NewIterator());
	if (imm != nullptr) {
		list.push_back(imm->NewIterator());
	}

	versions->current()->AddIterators(options, &list);
	std::shared_ptr<Iterator> internalIter =
		NewMergingIterator(&comparator, list, list.size());

	std::shared_ptr<IterState> cleanup(new IterState(mem, imm, versions->current()));
	internalIter->RegisterCleanup(std::bind(CleanupIteratorState, cleanup));

	*seed = ++this->seed;
	mutex.unlock();
	return internalIter;
}

std::shared_ptr<Iterator> DB::NewIterator(const ReadOptions& options) {
	uint64_t latestSnapshot;
	uint32_t seed;
	std::shared_ptr<Iterator> iter = NewInternalIterator(options, &latestSnapshot, &seed);
	return NewDBIterator(this, &comparator, iter, (options.snapshot != nullptr
		? options.snapshot->GetSequenceNumber(): latestSnapshot), seed);
}

Options DB::SanitizeOptions(const std::string& dbname,
	const InternalKeyComparator* icmp,
	const Options& src) {
	Options result = src;
	result.comparator = icmp;
	if (result.blockcache == nullptr) {
		result.blockcache = NewLRUCache(8 << 20);
	}
	return result;
}

DB::DB(const Options& op, const std::string& dbname)
	: comparator(op.comparator),
	options(SanitizeOptions(dbname, &comparator, op)),
	dbname(dbname),
	mem(nullptr),
	imm(nullptr),
	manualcompaction(nullptr),
	seed(0),
	hasimm(false),
	shuttingdown(false),
	backgroundCompactionScheduled(false) {
	tablecache.reset(new TableCache(dbname, options, tableCacheSize(options)));
	versions.reset(new VersionSet(dbname, options, tablecache, &comparator));
	snapshots.reset(new SnapshotList());
}

DB::~DB() {
	// Wait for background work to Finish.
	std::unique_lock<std::mutex> lck(mutex);
	shuttingdown.store(true, std::memory_order_release);

	while (backgroundCompactionScheduled) {
		backgroundworkFinishedSignal.wait(lck);
	}

	lck.unlock();
	if (dblock != nullptr) {
		options.env->UnlockFile(dblock);
	}

	options.env->ExitSchedule();
	versions.reset();
	log.reset();
	logfile.reset();
	tablecache.reset();
}


Status DB::Open() {
	std::unique_lock<std::mutex> lk(mutex);
	VersionEdit edit;
	bool saveManifest = false;
	Status s = Recover(&edit, &saveManifest);
	if (s.ok() && mem == nullptr) {
		uint64_t newLogNumber = versions->NewFileNumber();
		s = options.env->NewWritableFile(LogFileName(dbname, newLogNumber), logfile);
		if (s.ok()) {
			log.reset(new LogWriter(logfile.get()));
			logfileNumber = newLogNumber;
			mem.reset(new MemTable(comparator));
		}
	}

	if (s.ok() && saveManifest) {
		edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
		edit.SetLogNumber(logfileNumber);
		s = versions->LogAndApply(&edit, &mutex);
	}

	if (s.ok()) {
		DeleteObsoleteFiles();
		MaybeScheduleCompaction();
	}
	return s;
}

const std::shared_ptr<Snapshot> DB::GetSnapshot() {
	std::unique_lock<std::mutex> lk(mutex);
	return snapshots->NewSnapshot(versions->GetLastSequence());
}

void DB::ReleaseSnapshot(const std::shared_ptr<Snapshot>& shapsnot) {
	std::unique_lock<std::mutex> lk(mutex);
	snapshots->DeleteSnapshot(shapsnot);
}

void DB::DeleteObsoleteFiles() {
	// Make a Set of all of the live files
	std::set<uint64_t> live = pendingoutputs;
	versions->AddLiveFiles(&live);

	std::vector<std::string> filenames;
	options.env->GetChildren(dbname, &filenames);  // Ignoring errors on purpose
	uint64_t number;
	FileType type;
	for (size_t i = 0; i< filenames.size(); i++) {
		if (ParseFileName(filenames[i], &number, &type)) {
			bool keep = true;
			switch (type) {
			case kLogFile:
				keep = ((number >= versions->GetLogNumber()) ||
					(number == versions->GetPrevLogNumber()));
				break;
			case kDescriptorFile:
				// Keep my manifest file, and any newer incarnations'
				// (in case there is a race that allows other incarnations)
				keep = (number >= versions->GetManifestFileNumber());
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
					tablecache->evict(number);
				}

				printf("Delete type=%d #%lld\n",
					static_cast<int>(type),
					static_cast<unsigned long long>(number));
				options.env->DeleteFile(dbname + "/" + filenames[i]);
			}
		}
	}
}

Status DB::NewDB() {
	VersionEdit newdb;
	newdb.SetLogNumber(0);
	newdb.SetNextFile(2);
	newdb.SetLastSequence(0);
	const std::string manifest = DescriptorFileName(dbname, 1);
	std::shared_ptr<WritableFile> file;
	Status s = options.env->NewWritableFile(manifest, file);
	if (!s.ok()) {
		return s;
	}

	LogWriter log(file.get());
	std::string record;
	newdb.EncodeTo(&record);
	s = log.AddRecord(record);
	if (s.ok()) {
		s = file->close();
	}

	if (s.ok()) {
		// Make "CURRENT" file that points to the new manifest file.
		s = SetCurrentFile(options.env, dbname, 1);
	}
	else {
		options.env->DeleteFile(manifest);
	}
	return s;
}

Status DB::Recover(VersionEdit* edit, bool* saveManifest) {
	Status s;
	options.env->CreateDir(dbname);

	assert(dblock == nullptr);
	s = options.env->LockFile(LockFileName(dbname), dblock);
	if (!s.ok()) {
		return s;
	}

	if (!options.env->FileExists(CurrentFileName(dbname))) {
		if (options.createifmissing) {
			s = NewDB();
			if (!s.ok()) {
				return s;
			}
		}
		else {
			return Status::InvalidArgument(
				dbname, "does not exist (create_if_missing is false)");
		}
	}
	else {
		if (options.errorifexists) {
			return Status::InvalidArgument(
				dbname, "exists (error_if_exists is true)");
		}
	}

	s = versions->Recover(saveManifest);
	if (!s.ok()) {
		return s;
	}

	// Recover from all newer log files than the ones named in the
	// descriptor (new log files may have been added by the previous
	// incarnation without registering them in the descriptor).
	//
	// Note that PrevLogNumber() is no longer used, but we pay
	// attention to it in case we are recovering a database
	// produced by an older current() of leveldb.

	const uint64_t minLog = versions->GetLogNumber();
	const uint64_t prevLog = versions->GetPrevLogNumber();

	std::vector<std::string> filenames;
	s = options.env->GetChildren(dbname, &filenames);
	if (!s.ok()) {
		return s;
	}

	uint64_t maxSequence = 0;
	std::set<uint64_t> expected;
	versions->AddLiveFiles(&expected);
	uint64_t number;

	FileType type;
	std::vector<uint64_t> logs;
	for (size_t i = 0; i< filenames.size(); i++) {
		if (ParseFileName(filenames[i], &number, &type)) {
			expected.erase(number);
			if (type == kLogFile && ((number >= minLog) || (number == prevLog))) {
				logs.push_back(number);
			}
		}
	}

	std::sort(logs.begin(), logs.end());
	for (size_t i = 0; i< logs.size(); i++) {
		s = RecoverLogFile(logs[i], (i == logs.size() - 1), saveManifest, edit, &maxSequence);
		if (!s.ok()) {
			return s;
		}

		// The previous incarnation may not have written any MANIFEST
		// records after allocating this log number.  So we manually
		// Update the file number allocation counter in VersionSet.
		versions->MarkFileNumberUsed(logs[i]);
	}

	if (versions->GetLastSequence()< maxSequence) {
		versions->SetLastSequence(maxSequence);
	}
	return s;
}

void DB::MaybeIgnoreError(Status* s) const {
	if (s->ok() || options.paranoidchecks) {
		// No change needed
	}
	else {
		printf("Ignoring error %s\n", s->toString().c_str());
		*s = Status::OK();
	}
}

Status DB::RecoverLogFile(uint64_t logNumber, bool lastLog,
	bool* saveManifest, VersionEdit* edit, uint64_t* maxSequence) {
	// Open the log file
	std::string fname = LogFileName(dbname, logNumber);
	std::shared_ptr<SequentialFile> file;
	Status status = options.env->NewSequentialFile(fname, file);
	if (!status.ok()) {
		MaybeIgnoreError(&status);
		return status;
	}

	LogReporter reporter;
	// We intentionally make log::Reader do checksumming even if
	// paranoid_checks==false so that corruptions cause entire commits
	// to be skipped instead of propagating bad information (like overly
	// large sequence numbers).
	LogReader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
	std::string scratch;
	std::string_view record;
	WriteBatch batch;
	int compactions = 0;

	std::shared_ptr<MemTable> mem = nullptr;
	while (reader.ReadRecord(&record, &scratch) && status.ok()) {
		if (record.size()< 12) {
			reporter.Corruption(record.size(), Status::Corruption("log record too small"));
			continue;
		}

		if (mem == nullptr) {
			mem.reset(new MemTable(comparator));
		}

		WriteBatchInternal::SetContents(&batch, record);
		status = WriteBatchInternal::InsertInto(&batch, mem);
		MaybeIgnoreError(&status);

		if (!status.ok()) {
			break;
		}

		const uint64_t lastSeq = WriteBatchInternal::GetSequence(&batch) + WriteBatchInternal::Count(&batch) - 1;
		if (lastSeq > * maxSequence) {
			*maxSequence = lastSeq;
		}

		if (mem->GetMemoryUsage() > options.writebuffersize) {
			compactions++;
			*saveManifest = true;
			status = WriteLevel0Table(mem, edit, nullptr);
			mem.reset();
			if (!status.ok()) {
				// Reflect errors immediately so that conditions like full
				// file-systems cause the DB::Open() to fail.
				break;
			}
		}
	}

	// See if we should keep reusing the last log file.
	if (status.ok() && options.reuselogs && lastLog && compactions == 0) {
		uint64_t lfileSize;

		if (options.env->GetFileSize(fname, &lfileSize).ok() &&
			options.env->NewAppendableFile(fname, logfile).ok()) {
			printf("Reusing old log %s \n", fname.c_str());
			log.reset(new LogWriter(logfile.get(), lfileSize));
			logfileNumber = logNumber;
			if (mem != nullptr) {
				this->mem = mem;
				mem.reset();
				mem = nullptr;
			}
			else {
				// mem can be nullptr if lognum exists but was empty
				this->mem.reset(new MemTable(comparator));
			}
		}
	}

	if (mem != nullptr) {
		// mem did not get reused; compact it.
		if (status.ok()) {
			*saveManifest = true;
			status = WriteLevel0Table(mem, edit, nullptr);
		}
	}
	return status;
}

Status DB::Put(const WriteOptions& opt, const std::string_view& key, const std::string_view& value) {
	WriteBatch batch;
	batch.Put(key, value);
	return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const std::string_view& key) {
	WriteBatch batch;
	batch.Delete(key);
	return Write(opt, &batch);
}

Status DB::MakeRoomForWrite(std::unique_lock<std::mutex>& lk, bool force) {
	assert(!writers.empty());
	bool allowDelay = !force;
	Status s;
	while (true) {
		if (!bgerror.ok()) {
			// Yield previous error
			s = bgerror;
			break;
		}
		else if (allowDelay && versions->NumLevelFiles(0) >= kL0_SlowdownWritesTrigger) {
			// We are getting close to hitting a hard limit on the number of
			// L0 files.  Rather than delaying a single Write by several
			// seconds when we hit the hard limit, start delaying each
			// individual Write by 1ms to reduce latency variance.  Also,
			// this delay hands over some CPU to the compaction thread in
			// case it is sharing the same core as the writer.

			lk.unlock();
			options.env->SleepForMicroseconds(1000);
			allowDelay = false;  // Do not delay a single Write more than once
			lk.lock();
		}
		else if (!force && mem->GetMemoryUsage()<= options.writebuffersize) {
			// There is room in current memtable
			break;
		}
		else if (imm != nullptr) {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			printf("Current memtable full; waiting...\n");
			backgroundworkFinishedSignal.wait(lk);
		}
		else if (versions->NumLevelFiles(0) >= kL0_StopWritesTrigger) {
			// There are too many level-0 files.
			printf("Too many L0 files; waiting...\n");
			backgroundworkFinishedSignal.wait(lk);
		}
		else {
			// Attempt to switch to a new memtable and trigger compaction of old
			assert(versions->GetPrevLogNumber() == 0);
			uint64_t newLogNumber = versions->NewFileNumber();
			std::shared_ptr<WritableFile> lfile;
			s = options.env->NewWritableFile(LogFileName(dbname, newLogNumber), lfile);
			if (!s.ok()) {
				// Avoid chewing through file number space in a tight loop.
				versions->ReuseFileNumber(newLogNumber);
				break;
			}

			logfile = lfile;
			logfileNumber = newLogNumber;
			log.reset(new LogWriter(lfile.get()));
			imm = mem;
			hasimm.store(true, std::memory_order_release);
			mem.reset(new MemTable(comparator));
			force = false;   // Do not force another compaction if have room
			MaybeScheduleCompaction();
		}
	}
	return s;
}

WriteBatch* DB::BuildBatchGroup(Writer** lastWriter) {
	assert(!writers.empty());
	Writer* first = writers.front();
	WriteBatch* result = first->batch;
	assert(result != nullptr);
	size_t size = WriteBatchInternal::ByteSize(first->batch);
	size_t maxSize = 1<< 20;
	if (size<= (128<< 10)) {
		maxSize = size + (128<< 10);
	}

	*lastWriter = first;
	auto iter = writers.begin();
	++iter;

	for (; iter != writers.end(); ++iter) {
		Writer* w = *iter;
		if (w->sync && !first->sync) {
			// Do not include a sync Write into a batch handled by a non-sync Write.
			break;
		}

		if (w->batch != nullptr) {
			size += WriteBatchInternal::ByteSize(w->batch);
			if (size > maxSize) {
				// Do not make batch too big
				break;
			}

			// Append to *result
			if (result == first->batch) {
				// Switch to temporary batch instead of disturbing caller's batch
				result = tmpbatch.get();
				assert(WriteBatchInternal::Count(result) == 0);
				WriteBatchInternal::append(result, first->batch);
			}
			WriteBatchInternal::append(result, w->batch);
		}
		*lastWriter = w;
	}
	return result;
}

Status DB::DestroyDB(const std::string& dbname, const Options& options) {
	std::vector<std::string> filenames;
	Status result = options.env->GetChildren(dbname, &filenames);
	if (!result.ok()) {
		// Ignore error in case directory does not exist
		return Status::OK();
	}

	const std::string lockname = LockFileName(dbname);
	if (result.ok()) {
		uint64_t number;
		FileType type;
		for (size_t i = 0; i< filenames.size(); i++) {
			if (ParseFileName(filenames[i], &number, &type) &&
				type != kDBLockFile) {     // Lock file will be deleted at end
				Status Delete = options.env->DeleteFile(dbname + "/" + filenames[i]);
				if (result.ok() && !Delete.ok()) {
					result = Delete;
				}
			}
		}

		options.env->DeleteFile(lockname);
		options.env->DeleteDir(dbname);  // Ignore error in case dir Contains other files
	}
	return result;
}

Status DB::Write(const WriteOptions& opt, WriteBatch* myBatch) {
	Writer w;
	w.batch = myBatch;
	w.sync = opt.sync;
	w.done = false;

	std::unique_lock<std::mutex> lk(mutex);
	writers.push_back(&w);

	while (!w.done && &w != writers.front()) {
		w.cv.wait(lk);
	}

	if (w.done) {
		return w.status;
	}

	// May temporarily unlock and wait.
	Status status = MakeRoomForWrite(lk, myBatch == nullptr);
	uint64_t lastSequence = versions->GetLastSequence();
	Writer * lastWriter = &w;
	if (status.ok() && myBatch != nullptr) {
		WriteBatch* updates = BuildBatchGroup(&lastWriter);
		WriteBatchInternal::SetSequence(updates, lastSequence + 1);
		lastSequence += WriteBatchInternal::Count(updates);

		// Add to log and Apply to memtable.  We can Release the lock
		// during this phase since &w is currently responsible for logging
		// and protects against concurrent loggers and concurrent writes
		// into mem_.

		{
			lk.unlock();
			status = log->AddRecord(WriteBatchInternal::Contents(updates));
			bool syncerror = false;
			if (status.ok() && opt.sync) {
				status = logfile->sync();
				if (!status.ok()) {
					syncerror = true;
				}
			}

			if (status.ok()) {
				status = WriteBatchInternal::InsertInto(updates, mem);
			}

			lk.lock();
			if (syncerror) {
				// The state of the log file is indeterminate: the log record we
				// just added may or may not show up when the DB is re-opened.
				// So we force the DB into a mode where all future writes fail.
				RecordBackgroundError(status);
			}
		}

		if (updates == tmpbatch.get()) { tmpbatch->clear(); }
		versions->SetLastSequence(lastSequence);
	}

	while (true) {
		Writer* ready = writers.front();
		writers.pop_front();
		if (ready != &w) {
			ready->status = status;
			ready->done = true;
			ready->cv.notify_one();
		}

		if (ready == lastWriter) break;
	}

	// Notify new head of Write queue
	if (!writers.empty()) {
		writers.front()->cv.notify_one();
	}

	return status;
}

Status DB::Get(const ReadOptions& opt, const std::string_view& key, std::string* value) {
	Status s;
	uint64_t snapshot;

	std::unique_lock<std::mutex> lk(mutex);
	if (opt.snapshot != nullptr) {
		snapshot = opt.snapshot->GetSequenceNumber();
	}
	else {
		snapshot = versions->GetLastSequence();
	}

	bool haveStatUpdate = false;
	auto current = versions->current();
	Version::GetStats stats;

	{
		lk.unlock();
		LookupKey lkey(key, snapshot);
		if (mem->Get(lkey, value, &s)) {
			// Done
		}
		else if (imm != nullptr && imm->Get(lkey, value, &s)) {
			// Done
		}
		else {
			s = current->Get(opt, lkey, value, &stats);
			haveStatUpdate = true;
		}
		lk.lock();
	}

	if (haveStatUpdate && current->UpdateStats(stats)) {
		MaybeScheduleCompaction();
	}
	return s;
}

Status DB::WriteLevel0Table(const std::shared_ptr<MemTable>& mem, VersionEdit* edit, Version* base) {
	const uint64_t startMicros = options.env->NowMicros();
	FileMetaData meta;
	meta.number = versions->NewFileNumber();
	pendingoutputs.insert(meta.number);
	printf("Level-0 table #%llu: started\n",
		(unsigned long long) meta.number);

	Status s;
	{
		mutex.unlock();
		std::shared_ptr<Iterator> iter = mem->NewIterator();
		Status s = BuildTable(&meta, iter);
		mutex.lock();
	}
	printf("Level-0 table #%llu: %lld bytes %s\n", (unsigned long long) meta.number,
		(unsigned long long) meta.FileSize, s.toString().c_str());

	pendingoutputs.erase(meta.number);

	// Note that if file_size is zero, the file has been deleted and
	// should not be added to the manifest.

	int level = 0;
	if (s.ok() && meta.FileSize > 0) {
		const std::string_view minUserKey = meta.smallest.UserKey();
		const std::string_view maxUserKey = meta.largest.UserKey();
		if (base != nullptr) {
			level = base->PickLevelForMemTableOutput(minUserKey, maxUserKey);
		}

		edit->AddFile(level, meta.number, meta.FileSize,
			meta.smallest, meta.largest);
	}

	CompactionStats sta;
	sta.micros = options.env->NowMicros() - startMicros;
	sta.byteswritten = meta.FileSize;
	stats[level].Add(sta);
	return s;
}

void DB::BackgroundCallback() {
	std::unique_lock<std::mutex> lck(mutex);
	assert(backgroundCompactionScheduled);
	if (shuttingdown.load(std::memory_order_acquire)) {
		// No more background work when shutting down.
	}
	else if (!bgerror.ok()) {
		// No more background work after a background error.
	}
	else {
		BackgroundCompaction();
	}

	backgroundCompactionScheduled = false;

	// Previous compaction may have produced too many files in a level,
	// so reschedule another compaction if needed.
	MaybeScheduleCompaction();
	backgroundworkFinishedSignal.notify_all();
}

void DB::MaybeScheduleCompaction() {
	if (backgroundCompactionScheduled) {
		// Already scheduled
	}
	else if (shuttingdown.load(std::memory_order_acquire)) {
		// DB is being deleted; no more background compactions
	}
	else if (!bgerror.ok()) {
		// Already got an error; no more changes
	}
	else if (imm == nullptr &&
		manualcompaction == nullptr &&
		!versions->NeedsCompaction()) {
		// No work to be done
	}
	else {
		backgroundCompactionScheduled = true;
		options.env->Schedule(std::bind(&DB::BackgroundCallback, this));
	}
}

void DB::CompactRange(const std::string_view* begin, const std::string_view* end) {
	int files = 1;
	{
		std::unique_lock<std::mutex> lck(mutex);
		auto base = versions->current();
		for (int level = 1; level< kNumLevels; level++) {
			if (base->OverlapInLevel(level, begin, end)) {
				files = level;
			}
		}
	}

	TESTCompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
	for (int level = 0; level< files; level++) {
		TESTCompactRange(level, begin, end);
	}
}

void DB::TESTCompactRange(int level, const std::string_view* begin, const std::string_view* end) {
	assert(level >= 0);
	assert(level + 1< kNumLevels);

	InternalKey beginStorage, endStorage;

	ManualCompaction manual;
	manual.level = level;
	manual.done = false;
	if (begin == nullptr) {
		manual.begin = nullptr;
	}
	else {
		beginStorage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
		manual.begin = &beginStorage;
	}

	if (end == nullptr) {
		manual.end = nullptr;
	}
	else {
		endStorage = InternalKey(*end, 0, static_cast<ValueType>(0));
		manual.end = &endStorage;
	}

	std::unique_lock<std::mutex> lck(mutex);
	while (!manual.done && !shuttingdown.load(std::memory_order_acquire) && bgerror.ok()) {
		if (manualcompaction == nullptr) {  // Idle
			manualcompaction = &manual;
			MaybeScheduleCompaction();
		}
		else {
			// Running either my compaction or another compaction.
			backgroundworkFinishedSignal.wait(lck);
		}
	}

	if (manualcompaction == &manual) {
		// Cancel my manual compaction since we aborted early for some reason.
		manualcompaction = nullptr;
	}
}

void DB::BackgroundCompaction() {
	if (imm != nullptr) {
		CompactMemTable();
		return;
	}

	std::shared_ptr<Compaction> c;
	bool ismanual = (manualcompaction != nullptr);
	InternalKey manualend;
	if (ismanual) {
		auto m = manualcompaction;
		c = versions->CompactRange(m->level, m->begin, m->end);
		m->done = (c == nullptr);
		if (c != nullptr) {
			manualend = c->input(0, c->numInputFiles(0) - 1)->largest;
		}

		printf("Manual compaction at level-%d from %s .. %s; will stop at %s\n",
			m->level,
			(m->begin ? m->begin->DebugString().c_str() : "(begin)"),
			(m->end ? m->end->DebugString().c_str() : "(end)"),
			(m->done ? "(end)" : manualend.DebugString().c_str()));
	}
	else {
		c = versions->PickCompaction();
	}

	Status status;
	if (c == nullptr) {
		// Nothing to do
	}
	else if (!ismanual && c->isTrivialMove()) {
		// Move file to Next level
		assert(c->numInputFiles(0) == 1);
		auto f = c->input(0, 0);
		c->getEdit()->DeleteFile(c->getLevel(), f->number);
		c->getEdit()->AddFile(c->getLevel() + 1, f->number, f->FileSize,
			f->smallest, f->largest);
		status = versions->LogAndApply(c->getEdit(), &mutex);
		assert(status.ok());

		VersionSet::LevelSummaryStorage tmp;
		printf("Moved #%lld to level-%d %lld bytes %s: %s\n",
			static_cast<unsigned long long>(f->number),
			c->getLevel() + 1,
			static_cast<unsigned long long>(f->FileSize),
			status.toString().c_str(),
			versions->LevelSummary(&tmp));
	}
	else {
		std::shared_ptr<CompactionState> compact(new CompactionState(c.get()));
		status = DoCompactionWork(compact.get());
		if (!status.ok()) {
			RecordBackgroundError(status);
		}

		CleanupCompaction(compact.get());
		c->releaseInputs();
		DeleteObsoleteFiles();
	}

	if (status.ok()) {
		// Done
	}
	else {
		printf("Compaction error: %s", status.toString().c_str());
	}

	if (ismanual) {
		auto m = manualcompaction;
		if (!status.ok()) {
			m->done = true;
		}

		if (!m->done) {
			// We only compacted part of the requested range.  Update *m
			// to the range that is left to be compacted.
			m->tmpStorage = manualend;
			m->begin = &m->tmpStorage;
		}
		manualcompaction = nullptr;
	}
}

void DB::CleanupCompaction(CompactionState* compact) {
	if (compact->builder != nullptr) {
		// May happen if we get a shutdown call in the middle of compaction
		compact->builder->Abandon();
		compact->builder.reset();
		compact->outfile.reset();
	}
	else {
		assert(compact->outfile == nullptr);
	}

	for (size_t i = 0; i< compact->outputs.size(); i++) {
		const CompactionState::Output& out = compact->outputs[i];
		pendingoutputs.erase(out.number);
	}
}

Status DB::FinishCompactionOutputFile(CompactionState * compact,
	const std::shared_ptr<Iterator> & input) {
	assert(compact != nullptr);
	assert(compact->outfile != nullptr);
	assert(compact->builder != nullptr);

	const uint64_t outputNumber = compact->currentOutput()->number;
	assert(outputNumber != 0);

	// Check for iterator errors
	Status s = input->status();
	const uint64_t currentEntries = compact->builder->NumEntries();
	if (s.ok()) {
		s = compact->builder->Finish();
	}
	else {
		compact->builder->Abandon();
	}

	const uint64_t currentBytes = compact->builder->FileSize();
	compact->currentOutput()->FileSize = currentBytes;
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
		std::shared_ptr<Iterator> iter = tablecache->NewIterator(ReadOptions(), outputNumber, currentBytes);
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

Status DB::DoCompactionWork(CompactionState* compact) {
	const uint64_t startMicros = options.env->NowMicros();
	int64_t immMicros = 0;  // Micros spent doing imm_ compactions
	printf("Compacting %d@%d + %d@%d files",
		compact->compaction->numInputFiles(0),
		compact->compaction->getLevel(),
		compact->compaction->numInputFiles(1),
		compact->compaction->getLevel() + 1);

	assert(versions->NumLevelFiles(compact->compaction->getLevel()) > 0);
	assert(compact->builder == nullptr);
	assert(compact->outfile == nullptr);

	if (snapshots->empty()) {
		compact->smallestSnapshot = versions->GetLastSequence();
	}
	else {
		compact->smallestSnapshot = snapshots->oldest()->GetSequenceNumber();
	}

	// Release mutex while we're actually doing the compaction work
	mutex.unlock();

	std::shared_ptr<Iterator> input = versions->MakeInputIterator(compact->compaction);
	input->SeekToFirst();

	Status status;
	ParsedInternalKey ikey;
	std::string currentUserKey;
	bool hasCurrentUserKey = false;
	uint64_t lastSequenceForKey = kMaxSequenceNumber;
	for (; input->Valid() && !shuttingdown.load(std::memory_order_acquire);) {
		// Prioritize immutable compaction work
		if (hasimm.load(std::memory_order_relaxed)) {
			const uint64_t immStart = options.env->NowMicros();
			mutex.lock();
			if (imm != nullptr) {
				CompactMemTable();
				// Wake up MakeRoomForWrite() if necessary.
				backgroundworkFinishedSignal.notify_all();
			}

			mutex.unlock();
			immMicros += (options.env->NowMicros() - immStart);
		}

		std::string_view key = input->key();
		if (compact->compaction->shouldStopBefore(key) &&
			compact->builder != nullptr) {
			status = FinishCompactionOutputFile(compact, input);
			if (!status.ok()) {
				break;
			}
		}
		// Handle key/value, Add to state, etc.
		bool drop = false;
		if (!ParseInternalKey(key, &ikey)) {
			// Do not hide error keys
			currentUserKey.clear();
			hasCurrentUserKey = false;
			lastSequenceForKey = kMaxSequenceNumber;
		}
		else {
			if (!hasCurrentUserKey ||
				ikey.userKey.compare(std::string_view(currentUserKey)) != 0) {
				// First occurrence of this user key
				currentUserKey.assign(ikey.userKey.data(), ikey.userKey.size());
				hasCurrentUserKey = true;
				lastSequenceForKey = kMaxSequenceNumber;
			}

			if (lastSequenceForKey<= compact->smallestSnapshot) {
				// Hidden by an newer entry for same user key
				drop = true;    // (A)
			}
			else if (ikey.type == kTypeDeletion &&
				ikey.sequence<= compact->smallestSnapshot &&
				compact->compaction->isBaseLevelForKey(ikey.userKey)) {
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger sequence numbers
				// (3) data in layers that are being compacted here and have
				//     smaller sequence numbers will be dropped in the Next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				drop = true;
			}
			lastSequenceForKey = ikey.sequence;
		}
		/*
		printf("  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
			"%d smallest_snapshot: %d",
			ikey.userKey.toString().c_str(),
			(int)ikey.sequence, ikey.type, kTypeValue, drop,
			compact->compaction->isBaseLevelForKey(ikey.userKey),
			(int)lastSequenceForKey, (int)compact->smallestSnapshot);
			*/

		if (!drop) {
			// Open output file if necessary
			if (compact->builder == nullptr) {
				status = OpenCompactionOutputFile(compact);
				if (!status.ok()) {
					break;
				}
			}

			if (compact->builder->NumEntries() == 0) {
				compact->currentOutput()->smallest.DecodeFrom(key);
			}

			compact->currentOutput()->largest.DecodeFrom(key);
			compact->builder->Add(key, input->value());

			// Close output file if it is big enough
			if (compact->builder->FileSize() >=
				compact->compaction->getMaxOutputFileSize()) {
				status = FinishCompactionOutputFile(compact, input);
				if (!status.ok()) {
					break;
				}
			}
		}
		input->Next();
	}

	if (status.ok() && shuttingdown.load(std::memory_order_acquire)) {
		status = Status::IOError("Deleting DB during compaction\n");
	}

	if (status.ok() && compact->builder != nullptr) {
		status = FinishCompactionOutputFile(compact, input);
	}

	if (status.ok()) {
		status = input->status();
	}

	input.reset();

	CompactionStats stats;
	stats.micros = options.env->NowMicros() - startMicros - immMicros;
	for (int which = 0; which< 2; which++) {
		for (int i = 0; i< compact->compaction->numInputFiles(which); i++) {
			stats.bytesread += compact->compaction->input(which, i)->FileSize;
		}
	}

	for (size_t i = 0; i< compact->outputs.size(); i++) {
		stats.byteswritten += compact->outputs[i].FileSize;
	}

	mutex.lock();
	this->stats[compact->compaction->getLevel() + 1].Add(stats);
	if (status.ok()) {
		status = InstallCompactionResults(compact);
	}

	if (!status.ok()) {
		RecordBackgroundError(status);
	}

	VersionSet::LevelSummaryStorage tmp;
	printf("compacted to: %s", versions->LevelSummary(&tmp));
	return status;
}

int64_t DB::TESTMaxNextLevelOverlappingBytes() {
	std::unique_lock<std::mutex> lk(mutex);
	return versions->MaxNextLevelOverlappingBytes();
}

Status DB::OpenCompactionOutputFile(CompactionState* compact) {
	assert(compact != nullptr);
	assert(compact->builder == nullptr);
	uint64_t fileNumber;

	{
		std::unique_lock<std::mutex> lk(mutex);
		fileNumber = versions->NewFileNumber();
		pendingoutputs.insert(fileNumber);
		CompactionState::Output out;
		out.number = fileNumber;
		out.smallest.clear();
		out.largest.clear();
		compact->outputs.push_back(out);
	}

	// Make the output file
	std::string fname = TableFileName(dbname, fileNumber);
	Status s = options.env->NewWritableFile(fname, compact->outfile);
	if (s.ok()) {
		compact->builder.reset(new TableBuilder(options, compact->outfile));
	}
	return s;
}

Status DB::InstallCompactionResults(CompactionState* compact) {
	printf("Compacted %d@%d + %d@%d files => %lld bytes",
		compact->compaction->numInputFiles(0),
		compact->compaction->getLevel(),
		compact->compaction->numInputFiles(1),
		compact->compaction->getLevel() + 1,
		static_cast<long long>(compact->totalBytes));

	// Add compaction outputs
	compact->compaction->addInputDeletions(compact->compaction->getEdit());
	const int level = compact->compaction->getLevel();
	for (size_t i = 0; i< compact->outputs.size(); i++) {
		const CompactionState::Output& out = compact->outputs[i];
		compact->compaction->getEdit()->AddFile(
			level + 1,
			out.number, out.FileSize, out.smallest, out.largest);
	}
	return versions->LogAndApply(compact->compaction->getEdit(), &mutex);
}

void DB::CompactMemTable() {
	assert(imm != nullptr);
	// Save the Contents of the memtable as a new Table
	VersionEdit edit;
	auto base = versions->current();
	Status s = WriteLevel0Table(imm, &edit, base.get());

	if (s.ok() && shuttingdown.load(std::memory_order_acquire)) {
		s = Status::IOError("Deleting DB during memtable compaction\n");
	}

	// Replace immutable memtable with the generated Table
	if (s.ok()) {
		edit.SetPrevLogNumber(0);
		edit.SetLogNumber(logfileNumber);  // Earlier logs no longer needed
		s = versions->LogAndApply(&edit, &mutex);
	}

	if (s.ok()) {
		imm.reset();
		hasimm.store(false, std::memory_order_release);
		DeleteObsoleteFiles();
	}
	else {
		RecordBackgroundError(s);
	}
}

void DB::RecordBackgroundError(const Status & s) {
	if (bgerror.ok()) {
		bgerror = s;
		backgroundworkFinishedSignal.notify_all();
	}
}

Status DB::BuildTable(FileMetaData* meta, const std::shared_ptr<Iterator>& iter) {
	Status s;
	meta->FileSize = 0;
	iter->SeekToFirst();

	std::string fname = TableFileName(dbname, meta->number);
	std::shared_ptr<WritableFile> file;
	s = options.env->NewWritableFile(fname, file);
	if (!s.ok()) {
		return s;
	}

	std::shared_ptr<TableBuilder> builder(new TableBuilder(options, file));
	meta->smallest.DecodeFrom(iter->key());
	for (; iter->Valid(); iter->Next()) {
		std::string_view key = iter->key();
		meta->largest.DecodeFrom(key);
		builder->Add(key, iter->value());
	}

	// Finish and check for builder errors
	s = builder->Finish();
	if (s.ok()) {
		meta->FileSize = builder->FileSize();
		assert(meta->FileSize > 0);
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
		std::shared_ptr<Iterator> it = tablecache->NewIterator(ReadOptions(),
			meta->number,
			meta->FileSize);
		s = it->status();
	}

	// Check for input iterator errors
	if (!iter->status().ok()) {
		s = iter->status();
	}

	if (s.ok() && meta->FileSize > 0) {
		// Keep it
	}
	else {
		options.env->DeleteFile(fname);
	}
	return s;
}

std::shared_ptr<Iterator> DB::TESTNewInternalIterator() {
	uint64_t ignored;
	uint32_t seed;
	return NewInternalIterator(ReadOptions(), &ignored, &seed);
}

Status DB::TESTCompactMemTable() {
	// nullptr batch means just wait for earlier writes to be done
	Status s = Write(WriteOptions(), nullptr);
	if (s.ok()) {
		// Wait until the compaction completes
		std::unique_lock<std::mutex> lck(mutex);
		while (imm != nullptr && bgerror.ok()) {
			backgroundworkFinishedSignal.wait(lck);
		}

		if (imm != nullptr) {
			s = bgerror;
		}
	}
	return s;
}

void DB::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
	for (int i = 0; i< n; i++) {
		// Convert user_key into a corresponding internal key.
		InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
		InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
		uint64_t start = versions->ApproximateOffsetOf(k1);
		uint64_t limit = versions->ApproximateOffsetOf(k2);
		sizes[i] = (limit >= start ? limit - start : 0);
	}
}

bool DB::GetProperty(const std::string_view& property, std::string* value) {
	value->clear();
	std::unique_lock<std::mutex> lk(mutex);

	std::string_view in = property;
	std::string_view prefix("leveldb.");
	if (!StartsWith(in, prefix)) {
		return false;
	}

	in.remove_prefix(prefix.size());
	if (StartsWith(in, "num-files-at-level")) {
		in.remove_prefix(strlen("num-files-at-level"));
		uint64_t level;
		bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
		if (!ok || level >= kNumLevels) {
			return false;
		}
		else {
			char buf[100];
			snprintf(buf, sizeof(buf), "%d",
				versions->NumLevelFiles(static_cast<int>(level)));
			*value = buf;
			return true;
		}
	}
	else if (in == "status") {
		char buf[200];
		snprintf(buf, sizeof(buf),
			"                               Compactions\n"
			"Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
			"--------------------------------------------------\n"
		);

		value->append(buf);
		for (int level = 0; level< kNumLevels; level++) {
			int files = versions->NumLevelFiles(level);
			if (stats[level].micros > 0 || files > 0) {
				snprintf(
					buf, sizeof(buf),
					"%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
					level,
					files,
					versions->NumLevelBytes(level) / 1048576.0,
					stats[level].micros / 1e6,
					stats[level].bytesread / 1048576.0,
					stats[level].byteswritten / 1048576.0);
				value->append(buf);
			}
		}
		return true;
	}
	else if (in == "sstables") {
		*value = versions->current()->DebugString();
	}
	else if (in == "approximate-memory-usage") {
		size_t totalUsage = tablecache->GetCache()->TotalCharge();
		if (mem) {
			totalUsage += mem->GetMemoryUsage();
		}

		if (imm) {
			totalUsage += imm->GetMemoryUsage();
		}

		char buf[50];
		snprintf(buf, sizeof(buf), "%llu",
			static_cast<unsigned long long>(totalUsage));
		value->append(buf);
		return true;
	}
	return false;
}
