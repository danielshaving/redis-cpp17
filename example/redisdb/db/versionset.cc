#include "logreader.h"
#include "versionset.h"
#include "logging.h"
#include "filename.h"
#include "merger.h"

enum SaverState {
	kNotFound,
	kFound,
	kDeleted,
	kCorrupt,
};

struct Saver {
	SaverState state;
	const Comparator* ucmp;
	std::string_view userkey;
	std::string* value;
};

static bool AfterFile(const Comparator* ucmp,
	const std::string_view* userkey, const FileMetaData* f) {
	// null user_key occurs before all keys and is therefore never after *f
	return (userkey != nullptr &&
		ucmp->Compare(*userkey, f->largest.UserKey()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
	const std::string_view* userkey, const FileMetaData* f) {
	// null user_key occurs after all keys and is therefore never before *f
	return (userkey != nullptr &&
		ucmp->Compare(*userkey, f->smallest.UserKey()) < 0);
}

static size_t TargetFileSize(const Options* options) {
	return options->maxfilesize;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
	return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file Set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
	return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
	// Note: the result for level zero is not really used since we Set
	// the level-0 compaction threshold based on number of files.

	// Result for both level-0 and level-1
	double result = 10. * 1048576.0;
	while (level > 1) {
		result *= 10;
		level--;
	}
	return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
	// We could vary per level to reduce number of files?
	return TargetFileSize(options);
}

static bool NewestFirst(const std::shared_ptr<FileMetaData>& a, const std::shared_ptr<FileMetaData>& b) {
	return a->number > b->number;
}

static int64_t TotalFileSize(const std::vector<std::shared_ptr<FileMetaData>>& files) {
	int64_t sum = 0;
	for (size_t i = 0; i < files.size(); i++) {
		sum += files[i]->filesize;
	}
	return sum;
}

int FindFile(const InternalKeyComparator& icmp,
	const std::vector<std::shared_ptr<FileMetaData>>& files,
	const std::string_view& key) {
	uint32_t left = 0;
	uint32_t right = files.size();
	while (left < right) {
		uint32_t mid = (left + right) / 2;
		auto f = files[mid];
		if (icmp.Compare(f->largest.Encode(), key) < 0) {
			// Key at "mid.largest" is< "target".  Therefore all
			// files at or before "mid" are uninteresting.
			left = mid + 1;
		}
		else {
			// Key at "mid.largest" is >= "target".  Therefore all files
			// after "mid" are uninteresting.
			right = mid;
		}
	}
	return right;
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjointSortedFiles,
	const std::vector<std::shared_ptr<FileMetaData>>& files,
	const std::string_view * smallestuserkey,
	const std::string_view * largestuserkey) {
	const Comparator* ucmp = icmp.GetComparator();
	if (!disjointSortedFiles) {
		// Need to check against all files
		for (size_t i = 0; i < files.size(); i++) {
			std::shared_ptr<FileMetaData> f = files[i];
			if (AfterFile(ucmp, smallestuserkey, f.get()) ||
				BeforeFile(ucmp, largestuserkey, f.get())) {
				// No overlap
			}
			else {
				return true;  // Overlap
			}
		}
		return false;
	}

	// Binary search over file list
	uint32_t index = 0;
	if (smallestuserkey != nullptr) {
		// Find the earliest possible internal key for smallest_user_key
		InternalKey sm(*smallestuserkey, kMaxSequenceNumber, kValueTypeForSeek);
		index = FindFile(icmp, files, sm.Encode());
	}

	if (index >= files.size()) {
		// beginning of range is after all files, so no overlap.
		return false;
	}
	return !BeforeFile(ucmp, largestuserkey, files[index].get());
}

// An internal iterator.  For a given current()/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
public:
	LevelFileNumIterator(const InternalKeyComparator& icmp,
		const std::vector<std::shared_ptr<FileMetaData>>* flist)
		: icmp(icmp),
		flist(flist),
		index(flist->size()) {
		// Marks as invalid
	}

	virtual bool Valid() const {
		return index < flist->size();
	}

	virtual void Seek(const std::string_view& target) {
		index = FindFile(icmp, *flist, target);
	}

	virtual void SeekToFirst() { index = 0; }

	virtual void SeekToLast() {
		index = flist->empty() ? 0 : flist->size() - 1;
	}

	virtual void Next() {
		assert(Valid());
		index++;
	}

	virtual void Prev() {
		assert(Valid());
		if (index == 0) {
			index = flist->size();  // Marks as invalid
		}
		else {
			index--;
		}
	}

	virtual std::string_view key() const {
		assert(Valid());
		return (*flist)[index]->largest.Encode();
	}

	virtual std::string_view value() const {
		assert(Valid());
		EncodeFixed64(valueBuf, (*flist)[index]->number);
		EncodeFixed64(valueBuf + 8, (*flist)[index]->filesize);
		return std::string_view(valueBuf, sizeof(valueBuf));
	}

	virtual Status status() const { return Status::OK(); }

	virtual void RegisterCleanup(const std::any& arg) {}

private:
	const InternalKeyComparator icmp;
	const std::vector<std::shared_ptr<FileMetaData>>* const flist;
	uint32_t index;

	// Backing store for value().  Holds the file number and size.
	mutable char valueBuf[16];
};


bool Version::OverlapInLevel(int level, const std::string_view* smallestuserkey,
	const std::string_view * largestuserkey) {
	return SomeFileOverlapsRange(vset->icmp, level > 0, files[level],
		smallestuserkey, largestuserkey);
}

std::shared_ptr<Iterator> Version::NewConcatenatingIterator(const ReadOptions& op, int level) const {
	std::shared_ptr<Iterator> iter(new Version::LevelFileNumIterator(vset->icmp, &files[level]));
	return NewTwoLevelIterator(
		iter, op, std::bind(&TableCache::GetFileIterator, vset->tablecache, std::placeholders::_1, std::placeholders::_2));
}

void Version::AddIterators(const ReadOptions& ops, std::vector<std::shared_ptr<Iterator>>* iters) {
	// Merge all level zero files together since they may overlap
	for (size_t i = 0; i < files[0].size(); i++) {
		iters->push_back(
			vset->tablecache->NewIterator(
				ops, files[0][i]->number, files[0][i]->filesize));
	}

	// For levels > 0, we can use a concatenating iterator that sequentially
	// walks through the non-overlapping files in the level, opening them
	// lazily.
	for (int level = 1; level < kNumLevels; level++) {
		if (!files[level].empty()) {
			iters->push_back(NewConcatenatingIterator(ops, level));
		}
	}
}

bool Version::UpdateStats(const GetStats& stats) {
	auto f = stats.seekFile;
	if (f != nullptr) {
		f->allowedseeks--;
		if (f->allowedseeks <= 0 && filetocompact == nullptr) {
			filetocompact = f;
			filetocompactlevel = stats.seekFileLevel;
			return true;
		}
	}
	return false;
}

Status Version::Get(const ReadOptions& options, const LookupKey& key, std::string* value, GetStats* stats) {
	std::string_view ikey = key.InternalKey();
	std::string_view userkey = key.UserKey();
	const Comparator* ucmp = vset->icmp.GetComparator();
	Status s;

	stats->seekFile = nullptr;
	stats->seekFileLevel = -1;
	std::shared_ptr<FileMetaData> lastfileread = nullptr;
	int lastFileReadLevel = -1;

	// We can search level-by-level since entries never hop across
	// levels.  Therefore we are guaranteed that if we find data
	// in an smaller level, later levels are irrelevant.
	std::vector<std::shared_ptr<FileMetaData>> tmp;
	std::shared_ptr<FileMetaData> tmp2;
	for (int level = 0; level < kNumLevels; level++) {
		size_t numFiles = files[level].size();
		if (numFiles == 0) continue;

		// Get the list of files to search in this level
		auto fs = files[level];
		if (level == 0) {
			// Level-0 files may overlap each other.  Find all files that
			// overlap user_key and process them in order from newest to oldest.
			tmp.reserve(numFiles);
			for (uint32_t i = 0; i < numFiles; i++) {
				const auto& f = fs[i];
				if (ucmp->Compare(userkey, f->smallest.UserKey()) >= 0 &&
					ucmp->Compare(userkey, f->largest.UserKey()) <= 0) {
					tmp.push_back(f);
				}
			}

			if (tmp.empty()) continue;

			std::sort(tmp.begin(), tmp.end(), NewestFirst);
			fs = tmp;
			numFiles = tmp.size();
		}
		else {
			// Binary search to find earliest index whose largest key >= ikey.
			uint32_t index = FindFile(vset->icmp, files[level], ikey);
			if (index >= numFiles) {
				fs.clear();
				numFiles = 0;
			}
			else {
				tmp2 = fs[index];
				if (ucmp->Compare(userkey, tmp2->smallest.UserKey()) < 0) {
					// All of "tmp2" is past any data for user_key
					fs.clear();
					numFiles = 0;
				}
				else {
					fs.clear();
					fs.push_back(tmp2);
					numFiles = 1;
				}
			}
		}

		for (uint32_t i = 0; i < numFiles; ++i) {
			if (lastfileread != nullptr && stats->seekFile == nullptr) {
				// We have had more than one Seek for this read.  Charge the 1st file.
				stats->seekFile = lastfileread;
				stats->seekFileLevel = lastFileReadLevel;
			}

			std::shared_ptr<FileMetaData> f = fs[i];
			lastfileread = f;
			lastFileReadLevel = level;

			Saver saver;
			saver.state = kNotFound;
			saver.ucmp = ucmp;
			saver.userkey = userkey;
			saver.value = value;
			s = vset->GetTableCache()->Get(options, f->number, f->filesize,
				ikey, &saver, std::bind(&Version::SaveValue, this,
					std::placeholders::_1, std::placeholders::_2,
					std::placeholders::_3));

			if (!s.ok()) {
				return s;
			}

			switch (saver.state) {
			case kNotFound:
				break;      // Keep searching in other files
			case kFound:
				return s;
			case kDeleted:
				s = Status::NotFound(std::string_view());  // Use empty error message for speed
				return s;
			case kCorrupt:
				s = Status::Corruption("corrupted key for ", userkey);
				return s;
			}
		}
	}
	return Status::NotFound(std::string_view());  // Use an empty error message for speed
}

void Version::SaveValue(const std::any& arg, const std::string_view& ikey, const std::string_view& v) {
	Saver* s = std::any_cast<Saver*>(arg);
	ParsedInternalKey parsedKey;
	if (!ParseInternalKey(ikey, &parsedKey)) {
		s->state = kCorrupt;
	}
	else {
		if (s->ucmp->Compare(parsedKey.userkey, s->userkey) == 0) {
			s->state = (parsedKey.type == kTypeValue) ? kFound : kDeleted;
			if (s->state == kFound) {
				s->value->assign(v.data(), v.size());
			}
		}
	}
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(
	int level,
	const InternalKey* begin,         // nullptr means before all keys
	const InternalKey* end,           // nullptr means after all keys
	std::vector<std::shared_ptr<FileMetaData>>* inputs) {
	assert(level >= 0);
	assert(level < kNumLevels);

	inputs->clear();
	std::string_view userBegin, userEnd;

	if (begin != nullptr) {
		userBegin = begin->UserKey();
	}

	if (end != nullptr) {
		userEnd = end->UserKey();
	}

	const Comparator* ucmp = vset->icmp.GetComparator();

	for (size_t i = 0; i < files[level].size();) {
		const std::shared_ptr<FileMetaData>& f = files[level][i++];
		const std::string_view fileStart = f->smallest.UserKey();
		const std::string_view fileLimit = f->largest.UserKey();

		if (begin != nullptr && ucmp->Compare(fileLimit, userBegin) < 0) {
			// "f" is completely before specified range; Skip it
		}
		else if (end != nullptr && ucmp->Compare(fileStart, userEnd) > 0) {
			// "f" is completely after specified range; Skip it
		}
		else {
			inputs->push_back(f);
			if (level == 0) {
				// Level-0 files may overlap each other.  So check if the newly
				// added file has expanded the range.  If so, restart search.
				if (begin != nullptr && ucmp->Compare(fileStart, userBegin) < 0) {
					userBegin = fileStart;
					inputs->clear();
					i = 0;
				}
				else if (end != nullptr && ucmp->Compare(fileLimit, userEnd) > 0) {
					userEnd = fileLimit;
					inputs->clear();
					i = 0;
				}
			}
		}
	}
}

std::string Version::DebugString() const {
	std::string r;
	for (int level = 0; level < kNumLevels; level++) {
		// E.g.,
		//   --- level 1 ---
		//   17:123['a' .. 'd']
		//   20:43['e' .. 'g']
		r.append("--- level ");
		AppendNumberTo(&r, level);
		r.append(" ---\n");
		const std::vector<std::shared_ptr<FileMetaData>>& f = files[level];
		for (size_t i = 0; i < f.size(); i++) {
			r.push_back(' ');
			AppendNumberTo(&r, f[i]->number);
			r.push_back(':');
			AppendNumberTo(&r, f[i]->filesize);
			r.append("[");
			r.append(f[i]->smallest.DebugString());
			r.append(" .. ");
			r.append(f[i]->largest.DebugString());
			r.append("]\n");
		}
	}
	return r;
}

int Version::PickLevelForMemTableOutput(const std::string_view& smallestuserkey,
	const std::string_view& largestuserkey) {
	int level = 0;
	if (!OverlapInLevel(0, &smallestuserkey, &largestuserkey)) {
		// Push to Next level if there is no overlap in Next level,
		// and the #bytes overlapping in the level after that are limited.
		InternalKey start(smallestuserkey, kMaxSequenceNumber, kValueTypeForSeek);
		InternalKey limit(largestuserkey, 0, static_cast<ValueType>(0));
		std::vector<std::shared_ptr<FileMetaData>> overlaps;

		while (level < kMaxMemCompactLevel) {
			if (OverlapInLevel(level + 1, &smallestuserkey, &largestuserkey)) {
				break;
			}

			if (level + 2 < kNumLevels) {
				// Check that file does not overlap too many grandparent bytes.
				GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
				const int64_t sum = TotalFileSize(overlaps);
				if (sum > MaxGrandParentOverlapBytes(&vset->options)) {
					break;
				}
			}
			level++;
		}
	}
	return level;
}


Builder::Builder(VersionSet* vset, const std::shared_ptr<Version>& base)
	: vset(vset),
	base(base) {
	BySmallestKey cmp;
	cmp.internalComparator = &(vset->icmp);
	for (int level = 0; level < kNumLevels; level++) {
		levels[level].addedFiles.reset(new FileSet(cmp));
	}
}

Builder::~Builder() {

}

VersionSet::VersionSet(const std::string& dbname,
	const Options& options, const std::shared_ptr<TableCache>& tablecache,
	const InternalKeyComparator* cmp)
	: dbname(dbname),
	options(options),
	lastsequence(0),
	nextfilenumber(2),
	lognumber(0),
	prevlognumber(0),
	manifestfilenumber(0),
	descriptorlog(nullptr),
	descriptorfile(nullptr),
	tablecache(tablecache),
	icmp(*cmp) {
	std::shared_ptr<Version> v(new Version(this));
	AppendVersion(v);
}

VersionSet::~VersionSet() {

}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
	int64_t result = 0;
	std::vector<std::shared_ptr<FileMetaData>> overlaps;
	for (int level = 1; level < kNumLevels - 1; level++) {
		for (size_t i = 0; i < current()->files[level].size(); i++) {
			const auto f = current()->files[level][i];
			current()->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
				&overlaps);
			const int64_t sum = TotalFileSize(overlaps);
			if (sum > result) {
				result = sum;
			}
		}
	}
	return result;
}

uint64_t VersionSet::ApproximateOffsetOf(const InternalKey& ikey) {
	uint64_t result = 0;
	for (int level = 0; level < kNumLevels; level++) {
		const auto& files = current()->files[level];
		for (size_t i = 0; i < files.size(); i++) {
			if (icmp.Compare(files[i]->largest, ikey) <= 0) {
				// Entire file is before "ikey", so just Add the file size
				result += files[i]->filesize;
			}
			else if (icmp.Compare(files[i]->smallest, ikey) > 0) {
				// Entire file is after "ikey", so ignore
				if (level > 0) {
					// Files other than level 0 are sorted by meta->smallest, so
					// no further files in this level will contain data for
					// "ikey".
					break;
				}
			}
			else {
				// "ikey" falls in the range for this table.  Add the
				// approximate offset of "ikey" within the table.
				std::shared_ptr<Table> table;
				std::shared_ptr<Iterator> iter = tablecache->NewIterator(
					ReadOptions(), files[i]->number, files[i]->filesize, table);
				if (table != nullptr) {
					result += table->ApproximateOffsetOf(ikey.Encode());
				}
			}
		}
	}
	return result;
}

Status VersionSet::Recover(bool* manifest) {
	// Read "CURRENT" file, which Contains a pointer to the current() manifest file
	std::string cur;
	Status s = ReadFileToString(options.env, CurrentFileName(dbname), &cur);
	if (!s.ok()) {
		return s;
	}

	if (cur.empty() || cur[cur.size() - 1] != '\n') {
		return Status::Corruption("CURRENT file does not end with newline");
	}

	cur.resize(cur.size() - 1);
	std::string dscname = dbname + "/" + cur;
	std::shared_ptr<SequentialFile> file;
	s = options.env->NewSequentialFile(dscname, file);
	if (!s.ok()) {
		if (s.IsNotFound()) {
			return Status::Corruption(
				"CURRENT points to a non-existent file", s.ToString());
		}
		return s;
	}

	bool haveLogNumber = false;
	bool havePrevLogNumber = false;
	bool haveNextFile = false;
	bool haveLastSequence = false;

	uint64_t nextFile = 0;
	uint64_t lastsequence = 0;
	uint64_t lognumber = 0;
	uint64_t prevlognumber = 0;

	Builder builder(this, current());
	LogReporter reporter;
	reporter.status = &s;

	LogReader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
	std::string_view record;
	std::string scratch;
	while (reader.ReadRecord(&record, &scratch) && s.ok()) {
		VersionEdit edit;
		s = edit.DecodeFrom(record);
		if (s.ok()) {
			builder.Apply(&edit);
		}

		if (edit.haslognumber) {
			lognumber = edit.lognumber;
			haveLogNumber = true;
		}

		if (edit.hasprevlognumber) {
			prevlognumber = edit.prevlognumber;
			havePrevLogNumber = true;
		}

		if (edit.hasnextfilenumber) {
			nextFile = edit.nextfilenumber;
			haveNextFile = true;
		}

		if (edit.haslastsequence) {
			lastsequence = edit.lastsequence;
			haveLastSequence = true;
		}
	}

	if (s.ok()) {
		if (!haveNextFile) {
			s = Status::Corruption("no meta-nextfile entry in descriptor");
		}
		else if (!haveLogNumber) {
			s = Status::Corruption("no meta-lognumber entry in descriptor");
		}
		else if (!haveLastSequence) {
			s = Status::Corruption("no last-sequence-number entry in descriptor");
		}

		if (!havePrevLogNumber) {
			prevlognumber = 0;
		}

		MarkFileNumberUsed(prevlognumber);
		MarkFileNumberUsed(lognumber);
	}

	if (s.ok()) {
		std::shared_ptr<Version> v(new Version(this));
		builder.SaveTo(v.get());
		AppendVersion(v); // Install recovered current()
		Finalize(v.get());

		this->manifestfilenumber = nextFile;
		this->nextfilenumber = nextFile + 1;
		this->lastsequence = lastsequence;
		this->lognumber = lognumber;
		this->prevlognumber = prevlognumber;
		//See if we can reuse the existing MANIFEST file.
		if (ReuseManifest(dscname, cur)) {
			// No need to save new manifest
		}
		else {
			*manifest = true;
		}
	}
	return s;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
	for (auto &it : dummyversions) {
		for (int level = 0; level < kNumLevels; level++) {
			auto& files = it->files[level];
			for (size_t i = 0; i < files.size(); i++) {
				live->insert(files[i]->number);
			}
		}
	}
}

std::shared_ptr<Iterator> VersionSet::MakeInputIterator(Compaction* c) {
	ReadOptions ops;
	ops.verifychecksums = options.paranoidchecks;
	ops.fillcache = false;
	// Level-0 files have to be merged together.  For other levels,
	// we will make a concatenating iterator per level.
	// TODO(opt): use concatenating iterator for level-0 if there is no overlap
	const int space = (c->getLevel() == 0 ? c->inputs[0].size() + 1 : 2);

	std::vector<std::shared_ptr<Iterator>> list;
	list.resize(space);

	int num = 0;
	for (int which = 0; which < 2; which++) {
		if (!c->inputs[which].empty()) {
			if (c->getLevel() + which == 0) {
				const auto& files = c->inputs[which];
				for (size_t i = 0; i < files.size(); i++) {
					std::shared_ptr<Iterator> iter = tablecache->NewIterator(
						ops, files[i]->number, files[i]->filesize, nullptr);
					list[num++] = iter;
				}
			}
			else {
				// Create concatenating iterator for the files from this level
				std::shared_ptr<Iterator> iter(new Version::LevelFileNumIterator(icmp, &c->inputs[which]));
				list[num++] = NewTwoLevelIterator(
					iter, ops, std::bind(&TableCache::GetFileIterator, tablecache, std::placeholders::_1, std::placeholders::_2));
			}
		}
	}

	assert(num <= space);
	std::shared_ptr<Iterator> result = NewMergingIterator(&icmp, list, num);
	return result;
}

void VersionSet::Finalize(Version* v) {
	// Precomputed best level for Next compaction
	int bestLevel = -1;
	double bestScore = -1;

	for (int level = 0; level < kNumLevels - 1; level++) {
		double score;
		if (level == 0) {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger Write-buffer sizes, it is nice not to do too
			// many level-0 compactions.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small Write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = v->files[level].size() /
				static_cast<double>(kL0_CompactionTrigger);
		}
		else {
			// Compute the ratio of current() size to size limit.
			const uint64_t levelBytes = TotalFileSize(v->files[level]);
			score = static_cast<double>(levelBytes) / MaxBytesForLevel(&options, level);
		}

		if (score > bestScore) {
			bestLevel = level;
			bestScore = score;
		}
	}

	v->compactionlevel = bestLevel;
	v->compactionscore = bestScore;
}

bool VersionSet::ReuseManifest(const std::string& dscname, const std::string& dscbase) {
	if (!options.reuselogs) {
		return false;
	}

	FileType manifestType;
	uint64_t manifestNumber;
	uint64_t manifestSize;
	if (!ParseFileName(dscbase, &manifestNumber, &manifestType) ||
		manifestType != kDescriptorFile ||
		!(options.env->GetFileSize(dscname, &manifestSize).ok()) ||
		// Make new compacted MANIFEST if old one is too big
		manifestSize >= TargetFileSize(&options)) {
		return false;
	}

	assert(descriptorfile == nullptr);
	assert(descriptorlog == nullptr);

	Status r = options.env->NewAppendableFile(dscname, descriptorfile);
	if (!r.ok()) {
		printf("Reuse MANIFEST: %s\n", r.ToString().c_str());
		assert(descriptorfile == nullptr);
		return false;
	}

	printf("Reuse MANIFEST: %s\n", dscname.c_str());
	descriptorlog.reset(new LogWriter(descriptorfile.get(), manifestSize));
	manifestfilenumber = manifestNumber;
	return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
	if (nextfilenumber <= number) {
		nextfilenumber = number + 1;
	}
}

int64_t VersionSet::NumLevelBytes(int level) const {
	assert(level >= 0);
	assert(level < kNumLevels);
	return TotalFileSize(current()->files[level]);
}

int VersionSet::NumLevelFiles(int level) const {
	assert(level >= 0);
	assert(level < kNumLevels);
	return current()->files[level].size();
}

void VersionSet::AppendVersion(const std::shared_ptr<Version>& v) {
	if (!dummyversions.empty()) {
		dummyversions.pop_front();
	}
	dummyversions.push_back(v);
}

Status VersionSet::LogAndApply(VersionEdit* edit, std::mutex* mutex) {
	if (edit->haslognumber) {
		assert(edit->lognumber >= lognumber);
		assert(edit->lognumber < nextfilenumber);
	}
	else {
		edit->SetLogNumber(lognumber);
	}

	if (!edit->hasprevlognumber) {
		edit->SetPrevLogNumber(prevlognumber);
	}

	edit->SetNextFile(nextfilenumber);
	edit->SetLastSequence(lastsequence);

	std::shared_ptr<Version> v(new Version(this));
	{
		Builder builder(this, current());
		builder.Apply(edit);
		builder.SaveTo(v.get());
	}

	Finalize(v.get());
	// Initialize new descriptor log file if necessary by creating
	// a temporary file that Contains a snapshot of the current() current().
	std::string newManifestFile;
	Status s;
	if (descriptorlog == nullptr) {
		// No reason to unlock *mu here since we only hit this path in the
		// first call to LogAndApply (when opening the database).
		assert(descriptorfile == nullptr);
		newManifestFile = DescriptorFileName(dbname, manifestfilenumber);
		edit->SetNextFile(nextfilenumber);
		s = options.env->NewWritableFile(newManifestFile, descriptorfile);
		if (s.ok()) {
			descriptorlog.reset(new LogWriter(descriptorfile.get()));
			s = WriteSnapshot();
		}
	}

	mutex->unlock();
	// Write new record to MANIFEST log
	if (s.ok()) {
		std::string record;
		edit->EncodeTo(&record);
		s = descriptorlog->AddRecord(record);
		if (s.ok()) {
			s = descriptorfile->sync();
		}

		if (!s.ok()) {
			printf("MANIFEST Write: %s\n", s.ToString().c_str());
		}
	}

	// If we just created a new descriptor file, install it by writing a
	// new CURRENT file that points to it.
	if (s.ok() && !newManifestFile.empty()) {
		s = SetCurrentFile(options.env, dbname, manifestfilenumber);
	}

	mutex->lock();

	// Install the new current()
	if (s.ok()) {
		AppendVersion(v);
		lognumber = edit->lognumber;
		prevlognumber = edit->prevlognumber;
	}
	else {
		if (!newManifestFile.empty()) {
			descriptorlog.reset();
			descriptorfile.reset();
			options.env->DeleteFile(newManifestFile);
		}
	}
	return s;
}

Status VersionSet::WriteSnapshot() {
	// TODO: Break up into multiple records to reduce memory usage on recovery?

	// Save metadata
	VersionEdit edit;
	edit.SetComparatorName(icmp.GetComparator()->Name());

	// Save compaction pointers
	for (int level = 0; level < kNumLevels; level++) {
		if (!compactPointer[level].empty()) {
			InternalKey key;
			key.DecodeFrom(compactPointer[level]);
			edit.SetCompactPointer(level, key);
		}
	}

	// Save files
	for (int level = 0; level < kNumLevels; level++) {
		const auto& files = current()->files[level];
		for (size_t i = 0; i < files.size(); i++) {
			const auto f = files[i];
			edit.AddFile(level, f->number, f->filesize, f->smallest, f->largest);
		}
	}

	std::string record;
	edit.EncodeTo(&record);
	return descriptorlog->AddRecord(record);
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
	// Update code if kNumLevels changes
	assert(kNumLevels == 7);
	snprintf(scratch->buffer, sizeof(scratch->buffer),
		"files[ %d %d %d %d %d %d %d ]",
		int(current()->files[0].size()),
		int(current()->files[1].size()),
		int(current()->files[2].size()),
		int(current()->files[3].size()),
		int(current()->files[4].size()),
		int(current()->files[5].size()),
		int(current()->files[6].size()));
	return scratch->buffer;
}

std::shared_ptr<Compaction> VersionSet::PickCompaction() {
	std::shared_ptr<Compaction> c;
	int level;
	// We prefer compactions triggered by too much data in a level over
	// the compactions triggered by seeks.

	const bool sizeCompaction = (current()->compactionscore >= 1);
	const bool seekCompaction = (current()->filetocompact != nullptr);
	if (sizeCompaction) {
		level = current()->compactionlevel;
		assert(level >= 0);
		assert(level + 1 < kNumLevels);
		c.reset(new Compaction(&options, level));

		// Pick the first file that comes after compact_pointer_[level]
		for (size_t i = 0; i < current()->files[level].size(); i++) {
			auto f = current()->files[level][i];
			if (compactPointer[level].empty() ||
				icmp.Compare(f->largest.Encode(), compactPointer[level]) > 0) {
				c->inputs[0].push_back(f);
				break;
			}
		}

		if (c->inputs[0].empty()) {
			// Wrap-around to the beginning of the key space
			c->inputs[0].push_back(current()->files[level][0]);
		}
	}
	else if (seekCompaction) {
		level = current()->filetocompactlevel;
		c.reset(new Compaction(&options, level));
		c->inputs[0].push_back(current()->filetocompact);
	}
	else {
		return nullptr;
	}

	c->inputversion = current();
	// Files in level 0 may overlap each other, so pick up all overlapping ones
	if (level == 0) {
		InternalKey smallest, largest;
		GetRange(c->inputs[0], &smallest, &largest);
		// Note that the Next call will discard the file we placed in
		// c->inputs_[0] earlier and replace it with an overlapping Set
		// which will include the picked file.
		current()->GetOverlappingInputs(0, &smallest, &largest, &c->inputs[0]);
		assert(!c->inputs[0].empty());
	}

	SetupOtherInputs(c);
	return c;
}

std::shared_ptr<Compaction> VersionSet::CompactRange(
	int level,
	const InternalKey* begin,
	const InternalKey* end) {
	std::vector<std::shared_ptr<FileMetaData>> inputs;
	current()->GetOverlappingInputs(level, begin, end, &inputs);
	if (inputs.empty()) {
		return nullptr;
	}

	// Avoid compacting too much in one shot in case the range is large.
	// But we cannot do this for level-0 since level-0 files can overlap
	// and we must not pick one file and drop another older file if the
	// two files overlap.
	if (level > 0) {
		const uint64_t limit = MaxFileSizeForLevel(&options, level);
		uint64_t total = 0;
		for (size_t i = 0; i < inputs.size(); i++) {
			uint64_t s = inputs[i]->filesize;
			total += s;
			if (total >= limit) {
				inputs.resize(i + 1);
				break;
			}
		}
	}

	std::shared_ptr<Compaction> c(new Compaction(&options, level));
	c->inputversion = current();
	c->inputs[0] = inputs;
	SetupOtherInputs(c);
	return c;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<std::shared_ptr<FileMetaData>>& inputs,
	InternalKey* smallest,
	InternalKey* largest) {
	assert(!inputs.empty());
	smallest->clear();
	largest->clear();
	for (size_t i = 0; i < inputs.size(); i++) {
		auto f = inputs[i];
		if (i == 0) {
			*smallest = f->smallest;
			*largest = f->largest;
		}
		else {
			if (icmp.Compare(f->smallest, *smallest) < 0) {
				*smallest = f->smallest;
			}

			if (icmp.Compare(f->largest, *largest) > 0) {
				*largest = f->largest;
			}
		}
	}
}

void VersionSet::GetRange2(const std::vector<std::shared_ptr<FileMetaData>>& inputs1,
	const std::vector<std::shared_ptr<FileMetaData>>& inputs2,
	InternalKey * smallest,
	InternalKey * largest) {
	std::vector<std::shared_ptr<FileMetaData>> all = inputs1;
	all.insert(all.end(), inputs2.begin(), inputs2.end());
	GetRange(all, smallest, largest);
}

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool findLargestKey(const InternalKeyComparator& icmp,
	const std::vector<std::shared_ptr<FileMetaData>>& files,
	InternalKey* largestkey) {
	if (files.empty()) {
		return false;
	}

	*largestkey = files[0]->largest;
	for (size_t i = 1; i < files.size(); ++i) {
		std::shared_ptr<FileMetaData> f = files[i];
		if (icmp.Compare(f->largest, *largestkey) > 0) {
			*largestkey = f->largest;
		}
	}
	return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
std::shared_ptr<FileMetaData> findSmallestBoundaryFile(
	const InternalKeyComparator& icmp,
	const std::vector<std::shared_ptr<FileMetaData>>& levelfiles,
	const InternalKey& largestkey) {
	const Comparator* usercmp = icmp.GetComparator();
	std::shared_ptr<FileMetaData> smallestboundaryfile = nullptr;
	for (size_t i = 0; i < levelfiles.size(); ++i) {
		std::shared_ptr<FileMetaData> f = levelfiles[i];
		if (icmp.Compare(f->smallest, largestkey) > 0 &&
			usercmp->Compare(f->smallest.UserKey(), largestkey.UserKey()) == 0) {
			if (smallestboundaryfile == nullptr ||
				icmp.Compare(f->smallest, smallestboundaryfile->smallest) < 0) {
				smallestboundaryfile = f;
			}
		}
	}
	return smallestboundaryfile;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |levelfiles| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     levelfiles:      List of files to search for boundary files.
//   in/out compaction_files: List of files to Extend by adding boundary files.
void addBoundaryInputs(const InternalKeyComparator& icmp,
	const std::vector<std::shared_ptr<FileMetaData>>& levelfiles,
	std::vector<std::shared_ptr<FileMetaData>>* compactionfiles) {
	InternalKey largestkey;

	// Quick return if compaction_files is empty.
	if (!findLargestKey(icmp, *compactionfiles, &largestkey)) {
		return;
	}

	bool continuesearching = true;
	while (continuesearching) {
		std::shared_ptr<FileMetaData> smallestboundaryfile = findSmallestBoundaryFile(icmp, levelfiles, largestkey);

		// If a boundary file was found advance largestkey, otherwise we're done.
		if (smallestboundaryfile != nullptr) {
			compactionfiles->push_back(smallestboundaryfile);
			largestkey = smallestboundaryfile->largest;
		}
		else {
			continuesearching = false;
		}
	}
}

void VersionSet::SetupOtherInputs(const std::shared_ptr<Compaction>& c) {
	const int level = c->getLevel();
	InternalKey smallest, largest;

	addBoundaryInputs(icmp, current()->files[level], &c->inputs[0]);
	GetRange(c->inputs[0], &smallest, &largest);
	current()->GetOverlappingInputs(level + 1, &smallest, &largest, &c->inputs[1]);

	// Get entire range covered by compaction
	InternalKey allstart, alllimit;
	GetRange2(c->inputs[0], c->inputs[1], &allstart, &alllimit);

	// See if we can grow the number of inputs in "level" without
	// changing the number of "level+1" files we pick up.
	if (!c->inputs[1].empty()) {
		std::vector<std::shared_ptr<FileMetaData>> expanded0;
		current()->GetOverlappingInputs(level, &allstart, &alllimit, &expanded0);
		addBoundaryInputs(icmp, current()->files[level], &expanded0);

		const int64_t inputs0size = TotalFileSize(c->inputs[0]);
		const int64_t inputs1size = TotalFileSize(c->inputs[1]);
		const int64_t expanded0Size = TotalFileSize(expanded0);
		if (expanded0.size() > c->inputs[0].size() &&
			inputs1size + expanded0Size <
			ExpandedCompactionByteSizeLimit(&options)) {
			InternalKey newstart, newlimit;
			GetRange(expanded0, &newstart, &newlimit);
			std::vector<std::shared_ptr<FileMetaData>> expanded1;
			current()->GetOverlappingInputs(level + 1, &newstart, &newlimit,
				&expanded1);
			if (expanded1.size() == c->inputs[1].size()) {
				Debug(options.infolog, "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
					level,
					int(c->inputs[0].size()),
					int(c->inputs[1].size()),
					long(inputs0size), long(inputs1size),
					int(expanded0.size()),
					int(expanded1.size()),
					long(expanded0Size), long(inputs1size));
				smallest = newstart;
				largest = newlimit;
				c->inputs[0] = expanded0;
				c->inputs[1] = expanded1;
				GetRange2(c->inputs[0], c->inputs[1], &allstart, &alllimit);
			}
		}
	}

	// Compute the Set of grandparent files that overlap this compaction
	// (parent == level+1; grandparent == level+2)
	if (level + 2 < kNumLevels) {
		current()->GetOverlappingInputs(level + 2, &allstart, &alllimit,
			&c->grandparents);
	}

	// Update the place where we will do the Next compaction for this level.
	// We Update this immediately instead of waiting for the VersionEdit
	// to be applied so that if the compaction fails, we will try a different
	// key range Next time.
	compactPointer[level] = std::string(largest.Encode().data(), largest.Encode().size());
	c->edit.SetCompactPointer(level, largest);
}

// Apply all of the edits in *edit to the current() state.
void Builder::Apply(VersionEdit* edit) {
	//Update compaction pointers
	for (size_t i = 0; i < edit->compactpointers.size(); i++) {
		const int level = edit->compactpointers[i].first;
		std::string_view view = edit->compactpointers[i].second.Encode();
		vset->compactPointer[level] = std::string(view.data(), view.size());
	}

	// Delete files
	auto& Delete = edit->deletedfiles;
	for (auto iter = Delete.begin(); iter != Delete.end(); ++iter) {
		const int level = iter->first;
		const uint64_t number = iter->second;
		levels[level].deletedfiles.insert(number);
	}

	// Add new files
	for (size_t i = 0; i < edit->newfiles.size(); i++) {
		const int level = edit->newfiles[i].first;
		std::shared_ptr<FileMetaData> f(new FileMetaData(edit->newfiles[i].second));

		// We arrange to automatically compact this file after
		// a certain number of seeks.  Let's assume:
		//   (1) One Seek costs 10ms
		//   (2) Writing or reading 1MB costs 10ms (100MB/s)
		//   (3) A compaction of 1MB does 25MB of IO:
		//         1MB read from this level
		//         10-12MB read from Next level (boundaries may be misaligned)
		//         10-12MB written to Next level
		// This implies that 25 seeks cost the same as the compaction
		// of 1MB of data.  I.e., one Seek costs approximately the
		// same as the compaction of 40KB of data.  We are a little
		// conservative and allow approximately one Seek for every 16KB
		// of data before triggering a compaction.
		f->allowedseeks = (f->filesize / 16384);
		if (f->allowedseeks < 100) f->allowedseeks = 100;

		levels[level].deletedfiles.erase(f->number);
		levels[level].addedFiles->insert(f);
	}
}

// Save the current() state in *v.
void Builder::SaveTo(Version* v) {
	BySmallestKey cmp;
	cmp.internalComparator = &(vset->icmp);
	for (int level = 0; level < kNumLevels; level++) {
		// Merge the Set of added files with the Set of pre-existing files.
		// Drop any deleted files.  Store the result in *v.
		auto& baseFiles = base->files[level];
		auto baseIter = baseFiles.begin();
		auto baseEnd = baseFiles.end();
		auto added = levels[level].addedFiles;
		v->files[level].reserve(baseFiles.size() + added->size());
		for (auto addedIter = added->begin();
			addedIter != added->end();
			++addedIter) {
			// Add all smaller files listed in base_
			for (auto bpos = std::upper_bound(baseIter, baseEnd, (*addedIter), cmp);
				baseIter != bpos; ++baseIter) {
				MaybeAddFile(v, level, (*baseIter));
			}
			MaybeAddFile(v, level, (*addedIter));
		}

		// Add remaining base files
		for (; baseIter != baseEnd; ++baseIter) {
			MaybeAddFile(v, level, (*baseIter));
		}

#ifdef NDEBUG
		// Make sure there is no overlap in levels > 0
		if (level > 0)
		{
			for (uint32_t i = 1; i < v->files[level].size(); i++)
			{
				const InternalKey& prevEnd = v->files[level][i - 1]->largest;
				const InternalKey& thisBegin = v->files[level][i]->smallest;
				if (vset->icmp.compare(prevEnd, thisBegin) >= 0)
				{
					fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
						prevEnd.DebugString().c_str(),
						thisBegin.DebugString().c_str());
					abort();
				}
			}
		}
#endif
	}
}

void Builder::MaybeAddFile(Version* v, int level, const std::shared_ptr<FileMetaData>& f) {
	if (levels[level].deletedfiles.count(f->number) > 0) {
		// File is deleted: do nothing
	}
	else {
		auto& files = v->files[level];
		if (level > 0 && !files.empty()) {
			// Must not overlap
			assert(vset->icmp.Compare(files[files.size() - 1]->largest,
				f->smallest) < 0);
		}
		files.push_back(f);
	}
}

Compaction::Compaction(const Options* options, int level)
	: level(level),
	maxoutputfilesize(MaxFileSizeForLevel(options, level)),
	inputversion(nullptr),
	grandparentindex(0),
	seenkey(false),
	overlappedbytes(0) {
	for (int i = 0; i < kNumLevels; i++) {
		levelptrs[i] = 0;
	}
}

Compaction::~Compaction() {

}

bool Compaction::isTrivialMove() const {
	const VersionSet* vset = inputversion->vset;
	// Avoid a move if there is lots of overlapping grandparent data.
	// Otherwise, the move could create a parent file that will require
	// a very expensive merge later on.
	return (numInputFiles(0) == 1 && numInputFiles(1) == 0 &&
		TotalFileSize(grandparents) <= MaxGrandParentOverlapBytes(&vset->options));
}

void Compaction::addInputDeletions(VersionEdit* edit) {
	for (int which = 0; which < 2; which++) {
		for (size_t i = 0; i < inputs[which].size(); i++) {
			edit->DeleteFile(level + which, inputs[which][i]->number);
		}
	}
}

bool Compaction::isBaseLevelForKey(const std::string_view& userkey) {
	// Maybe use binary search to find right entry instead of linear search?
	const Comparator* cmp = inputversion->vset->icmp.GetComparator();
	for (int lvl = level + 2; lvl < kNumLevels; lvl++) {
		auto& files = inputversion->files[lvl];
		for (; levelptrs[lvl] < files.size();) {
			auto f = files[levelptrs[lvl]];
			if (cmp->Compare(userkey, f->largest.UserKey()) <= 0) {
				// We've advanced far enough
				if (cmp->Compare(userkey, f->smallest.UserKey()) >= 0) {
					// Key falls in this file's range, so definitely not base level
					return false;
				}
				break;
			}
			levelptrs[lvl]++;
		}
	}
	return true;
}

bool Compaction::shouldStopBefore(const std::string_view& internalKey) {
	const VersionSet* vset = inputversion->vset;
	// Scan to find earliest grandparent file that Contains key.
	const InternalKeyComparator* icmp = &vset->icmp;
	while (grandparentindex < grandparents.size() &&
		icmp->Compare(internalKey,
			grandparents[grandparentindex]->largest.Encode()) > 0) {
		if (seenkey) {
			overlappedbytes += grandparents[grandparentindex]->filesize;
		}
		grandparentindex++;
	}

	seenkey = true;

	if (overlappedbytes > MaxGrandParentOverlapBytes(&vset->options)) {
		// Too much overlap for current() output; start new output
		overlappedbytes = 0;
		return true;
	}
	else {
		return false;
	}
}

void Compaction::releaseInputs() {
	inputversion.reset();
}

