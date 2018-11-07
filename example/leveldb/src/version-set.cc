#include "version-set.h"
#include "logging.h"
#include "filename.h"
#include "log-reader.h"

static bool afterFile(const InternalKeyComparator *ucmp,
	const std::string_view *userKey, const FileMetaData *f)
{
	// null user_key occurs before all keys and is therefore never after *f
	return (userKey != nullptr &&
		ucmp->compare(*userKey, f->largest.userKey()) > 0);
}

static bool beforeFile(const InternalKeyComparator *ucmp,
	const std::string_view *userKey, const FileMetaData *f)
{
	// null user_key occurs after all keys and is therefore never before *f
	return (userKey != nullptr &&
		ucmp->compare(*userKey, f->smallest.userKey()) < 0);
}

static size_t targetFileSize(const Options *options)
{
	return options->maxFileSize;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t maxGrandParentOverlapBytes(const Options *options)
{
	return 10 * targetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t expandedCompactionByteSizeLimit(const Options *options)
{
	return 25 * targetFileSize(options);
}

static double maxBytesForLevel(const Options *options, int level)
{
	// Note: the result for level zero is not really used since we set
	// the level-0 compaction threshold based on number of files.

	// Result for both level-0 and level-1
	double result = 10. * 1048576.0;
	while (level > 1)
	{
		result *= 10;
		level--;
	}
	return result;
}

static uint64_t maxFileSizeForLevel(const Options *options, int level)
{
	// We could vary per level to reduce number of files?
	return targetFileSize(options);
}

static int64_t totalFileSize(const std::vector<std::shared_ptr<FileMetaData>> &files)
{
	int64_t sum = 0;
	for (size_t i = 0; i < files.size(); i++)
	{
		sum += files[i]->fileSize;
	}
	return sum;
}

int findFile(const InternalKeyComparator &icmp,
	const std::vector<std::shared_ptr<FileMetaData>> &files,
	const std::string_view &key)
{
	uint32_t left = 0;
	uint32_t right = files.size();
	while (left < right)
	{
		uint32_t mid = (left + right) / 2;
		std::shared_ptr<FileMetaData> f = files[mid];
		if (icmp.compare(f->largest.encode(), key) < 0)
		{
			// Key at "mid.largest" is < "target".  Therefore all
			// files at or before "mid" are uninteresting.
			left = mid + 1;
		}
		else
		{
			// Key at "mid.largest" is >= "target".  Therefore all files
			// after "mid" are uninteresting.
			right = mid;
		}
	}
	return right;
}

bool someFileOverlapsRange(bool disjointSortedFiles,
	const std::vector<std::shared_ptr<FileMetaData>> &files,
	const std::string_view *smallestUserKey,
	const std::string_view *largestUserKey)
{
	InternalKeyComparator cmp;
	if (!disjointSortedFiles)
	{
		// Need to check against all files
		for (size_t i = 0; i < files.size(); i++)
		{
			std::shared_ptr<FileMetaData> f = files[i];
			if (afterFile(&cmp, smallestUserKey, f.get()) ||
				beforeFile(&cmp, largestUserKey, f.get()))
			{
				// No overlap
			}
			else
			{
				return true;  // Overlap
			}
		}
		return false;
	}

	// Binary search over file list
	uint32_t index = 0;
	if (smallestUserKey != nullptr)
	{
		// Find the earliest possible internal key for smallest_user_key
		InternalKey small(*smallestUserKey, kMaxSequenceNumber, kValueTypeForSeek);
		index = findFile(cmp, files, small.encode());
	}

	if (index >= files.size())
	{
		// beginning of range is after all files, so no overlap.
		return false;
	}
	return !beforeFile(&cmp, largestUserKey, files[index].get());
}

bool Version::overlapInLevel(int level, const std::string_view *smallestUserKey,
	const std::string_view *largestUserKey)
{
	return someFileOverlapsRange(level > 0, files[level],
		smallestUserKey, largestUserKey);
}

static bool newestFirst(const std::shared_ptr<FileMetaData> &a, const std::shared_ptr<FileMetaData> &b) 
{
	return a->number > b->number;
}

Status Version::get(const ReadOptions&, const LookupKey &key, std::string *val, GetStats *stats)
{
	std::string_view ikey = key.internalKey();
	std::string_view userKey = key.userKey();
	InternalKeyComparator cmp;
	Status s;

	stats->seekFile = nullptr;
	stats->seekFileLevel = -1;
	std::shared_ptr<FileMetaData> lastFileRead = nullptr;
	int lastFileReadLevel = -1;

	// We can search level-by-level since entries never hop across
	// levels.  Therefore we are guaranteed that if we find data
	// in an smaller level, later levels are irrelevant.
	std::vector<std::shared_ptr<FileMetaData>> tmp;
	std::shared_ptr<FileMetaData> tmp2;
	for (int level = 0; level < kNumLevels; level++) 
	{
		size_t numFiles = files[level].size();
		if (numFiles == 0) continue;
		
		// Get the list of files to search in this level
		std::vector<std::shared_ptr<FileMetaData>> fs = files[level];
		if (level == 0) 
		{
			 // Level-0 files may overlap each other.  Find all files that
			// overlap user_key and process them in order from newest to oldest.
			tmp.reserve(numFiles);
			for (uint32_t i = 0; i < numFiles; i++) 
			{
				const std::shared_ptr<FileMetaData> &f = fs[i];
				if (cmp.compare(userKey, f->smallest.userKey()) >= 0 &&
					cmp.compare(userKey, f->largest.userKey()) <= 0) 
				{
					tmp.push_back(f);
				}
			}
			
			if (tmp.empty()) continue;
			
			std::sort(tmp.begin(), tmp.end(), newestFirst);
			fs = tmp;
			numFiles = tmp.size();
		}
		else
		{
			// Binary search to find earliest index whose largest key >= ikey.
			uint32_t index = findFile(cmp, files[level], ikey);
			if (index >= numFiles) 
			{
				fs.clear();
				numFiles = 0;
			} 
			else 
			{
				tmp2 = fs[index];
				if (cmp.compare(userKey, tmp2->smallest.userKey()) < 0) 
				{
					// All of "tmp2" is past any data for user_key
					fs.clear();
					numFiles = 0;
				}
				else 
				{
					fs.clear();
					fs.push_back(tmp2);	
				    numFiles = 1;
				}
			}
		}
	}
		 
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::getOverlappingInputs(
	int level,
	const InternalKey *begin,         // nullptr means before all keys
	const InternalKey *end,           // nullptr means after all keys
	std::vector<std::shared_ptr<FileMetaData>> *inputs)
{
	assert(level >= 0);
	assert(level < kNumLevels);

	inputs->clear();
	std::string_view userBegin, userEnd;

	if (begin != nullptr)
	{
		userBegin = begin->userKey();
	}

	if (userEnd != nullptr)
	{
		userEnd = end->userKey();
	}

	InternalKeyComparator cmp;

	for (size_t i = 0; i < files[level].size(); )
	{
		std::shared_ptr<FileMetaData> f = files[level][i++];
		const std::string_view fileStart = f->smallest.userKey();
		const std::string_view fileLimit = f->largest.userKey();

		if (begin != nullptr && cmp.compare(fileLimit, userBegin) < 0)
		{
			// "f" is completely before specified range; skip it
		}
		else if (end != nullptr && cmp.compare(fileStart, userEnd) > 0)
		{
			// "f" is completely after specified range; skip it
		}
		else
		{
			inputs->push_back(f);
			if (level == 0)
			{
				// Level-0 files may overlap each other.  So check if the newly
				// added file has expanded the range.  If so, restart search.
				if (begin != nullptr && cmp.compare(fileStart, userBegin) < 0)
				{
					userBegin = fileStart;
					inputs->clear();
					i = 0;
				}
				else if (end != nullptr && cmp.compare(fileLimit, userEnd) > 0)
				{
					userEnd = fileLimit;
					inputs->clear();
					i = 0;
				}
			}
		}
	}
}

int Version::pickLevelForMemTableOutput(const std::string_view &smallestUserKey,
	const std::string_view &largestUserKey)
{
	int level = 0;
	if (!overlapInLevel(0, &smallestUserKey, &largestUserKey))
	{
		// Push to next level if there is no overlap in next level,
		// and the #bytes overlapping in the level after that are limited.
		InternalKey start(smallestUserKey, kMaxSequenceNumber, kValueTypeForSeek);
		InternalKey limit(largestUserKey, 0, static_cast<ValueType>(0));
		std::vector<std::shared_ptr<FileMetaData>> overlaps;

		while (level < kMaxMemCompactLevel)
		{
			if (overlapInLevel(level + 1, &smallestUserKey, &largestUserKey))
			{
				break;
			}

			if (level + 2 < kNumLevels)
			{
				// Check that file does not overlap too many grandparent bytes.
				getOverlappingInputs(level + 2, &start, &limit, &overlaps);
				const int64_t sum = totalFileSize(overlaps);
				if (sum > maxGrandParentOverlapBytes(&vset->options))
				{
					break;
				}
			}
			level++;
		}
	}
	return level;
}

VersionSet::VersionSet(const std::string &dbname, const Options &options)
	:dbname(dbname),
	options(options),
	lastSequence(0),
	nextFileNumber(2),
	logNumber(0),
	prevLogNumber(0),
	manifestFileNumber(0),
	descriptorLog(nullptr),
	descriptorFile(nullptr)
{
	std::shared_ptr<Version> v(new Version(this));
	appendVersion(v);
}

VersionSet::~VersionSet()
{

}

std::shared_ptr<Version> VersionSet::current() const
{
	assert(!versions.empty());
	return versions.front();
}

Status VersionSet::recover(bool *manifest)
{
	// Read "CURRENT" file, which contains a pointer to the current manifest file
	std::string current;
	Status s = readFileToString(options.env, currentFileName(dbname), &current);
	if (!s.ok())
	{
		return s;
	}

	if (current.empty() || current[current.size() - 1] != '\n')
	{
		return Status::corruption("CURRENT file does not end with newline");
	}
	current.resize(current.size() - 1);

	std::string dscname = dbname + "/" + current;
	std::shared_ptr<PosixSequentialFile> file;
	s = options.env->newSequentialFile(dscname, file);
	if (!s.ok())
	{
		if (s.isNotFound())
		{
			return Status::corruption(
				"CURRENT points to a non-existent file", s.toString());
		}
		return s;
	}

	bool haveLogNumber = false;
	bool havePrevLogNumber = false;
	bool haveNextFile = false;
	bool haveLastSequence = false;

	uint64_t nextFile = 0;
	uint64_t lastSequence = 0;
	uint64_t logNumber = 0;
	uint64_t prevLogNumber = 0;

	Builder builder(this, versions.back().get());
	LogReporter reporter;
	reporter.status = &s;

	LogReader reader(file.get(), &reporter, true/*checksum*/, 0/*initial_offset*/);
	std::string_view record;
	std::string scratch;
	while (reader.readRecord(&record, &scratch) && s.ok())
	{
		VersionEdit edit;
		s = edit.decodeFrom(record);
		if (s.ok())
		{
			builder.apply(&edit);
		}

		if (edit.hasLogNumber)
		{
			logNumber = edit.logNumber;
			haveLogNumber = true;
		}

		if (edit.hasPrevLogNumber)
		{
			prevLogNumber = edit.prevLogNumber;
			havePrevLogNumber = true;
		}

		if (edit.hasNextFileNumber)
		{
			nextFile = edit.nextFileNumber;
			haveNextFile = true;
		}

		if (edit.hasLastSequence)
		{
			lastSequence = edit.lastSequence;
			haveLastSequence = true;
		}
	}

	if (s.ok())
	{
		if (!haveNextFile)
		{
			s = Status::corruption("no meta-nextfile entry in descriptor");
		}
		else if (!haveLogNumber)
		{
			s = Status::corruption("no meta-lognumber entry in descriptor");
		}
		else if (!haveLastSequence)
		{
			s = Status::corruption("no last-sequence-number entry in descriptor");
		}

		if (!havePrevLogNumber)
		{
			prevLogNumber = 0;
		}

		markFileNumberUsed(prevLogNumber);
		markFileNumberUsed(logNumber);
	}

	if (s.ok())
	{
		std::shared_ptr<Version> v(new Version(this));
		builder.saveTo(v.get());
		appendVersion(v); // Install recovered version
		finalize(v.get());

		this->manifestFileNumber = nextFile;
		this->nextFileNumber = nextFile + 1;
		this->lastSequence = lastSequence;
		this->logNumber = logNumber;
		this->prevLogNumber = prevLogNumber;
		//See if we can reuse the existing MANIFEST file.
		if (reuseManifest(dscname, current))
		{
			// No need to save new manifest
		}
		else
		{
			*manifest = true;
		}
	}
	return s;
}

void VersionSet::addLiveFiles(std::set<uint64_t> *live)
{
	for (auto &iter : versions)
	{
		for (int level = 0; level < kNumLevels; level++)
		{
			auto files = iter->files[level];
			for (size_t i = 0; i < files.size(); i++)
			{
				live->insert(files[i]->number);
			}
		}
	}
}

void VersionSet::finalize(Version *v)
{
	// Precomputed best level for next compaction
	int bestLevel = -1;
	double bestScore = -1;

	for (int level = 0; level < kNumLevels - 1; level++)
	{
		double score;
		if (level == 0)
		{
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compactions.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = v->files[level].size() /
				static_cast<double>(kL0_CompactionTrigger);
		}
		else
		{
			// Compute the ratio of current size to size limit.
			const uint64_t levelBytes = totalFileSize(v->files[level]);
			score = static_cast<double>(levelBytes) / maxBytesForLevel(&options, level);
		}

		if (score > bestScore)
		{
			bestLevel = level;
			bestScore = score;
		}
	}

	v->compactionLevel = bestLevel;
	v->compactionScore = bestScore;
}

bool VersionSet::reuseManifest(const std::string &dscname, const std::string &dscbase)
{
	if (!options.reuseLogs)
	{
		return false;
	}

	FileType manifestType;
	uint64_t manifestNumber;
	uint64_t manifestSize;
	if (!parseFileName(dscbase, &manifestNumber, &manifestType) ||
		manifestType != kDescriptorFile ||
		!options.env->getFileSize(dscname, &manifestSize).ok() ||
		// Make new compacted MANIFEST if old one is too big
		manifestSize >= targetFileSize(&options))
	{
		return false;
	}

	assert(descriptorFile == nullptr);
	assert(descriptorLog == nullptr);
	Status r = options.env->newAppendableFile(dscname, descriptorFile);
	if (!r.ok())
	{
		assert(descriptorFile == nullptr);
		return false;
	}

	descriptorLog.reset(new LogWriter(descriptorFile.get(), manifestSize));
	manifestFileNumber = manifestNumber;
	return true;
}

void VersionSet::markFileNumberUsed(uint64_t number)
{
	if (nextFileNumber <= number)
	{
		nextFileNumber = number + 1;
	}
}

int VersionSet::numLevelFiles(int level) const
{
	assert(level >= 0);
	assert(level < kNumLevels);
	return current()->files[level].size();
}

void VersionSet::appendVersion(const std::shared_ptr<Version> &v)
{
	assert(v->refs == 0);
	if (!versions.empty())
	{
		if (versions.back()->unref())
		{
			versions.pop_back();
		}
	}

	v->ref();
	versions.push_back(v);
}

Status VersionSet::logAndApply(VersionEdit *edit)
{
	if (edit->hasLogNumber)
	{
		assert(edit->logNumber >= logNumber);
		assert(edit->logNumber < nextFileNumber);
	}
	else
	{
		edit->setLogNumber(logNumber);
	}

	if (!edit->hasPrevLogNumber)
	{
		edit->setPrevLogNumber(prevLogNumber);
	}

	edit->setNextFile(nextFileNumber);
	edit->setLastSequence(lastSequence);

	std::shared_ptr<Version> v(new Version(this));
	{
		Builder builder(this, versions.back().get());
		builder.apply(edit);
		builder.saveTo(v.get());
	}
	finalize(v.get());

	// Initialize new descriptor log file if necessary by creating
	// a temporary file that contains a snapshot of the current version.
	std::string newManifestFile;
	Status s;
	if (descriptorLog == nullptr)
	{
		// No reason to unlock *mu here since we only hit this path in the
		// first call to LogAndApply (when opening the database).
		assert(descriptorFile == nullptr);
		newManifestFile = descriptorFileName(dbname, manifestFileNumber);
		edit->setNextFile(nextFileNumber);
		s = options.env->newWritableFile(newManifestFile, descriptorFile);
		if (s.ok())
		{
			descriptorLog.reset(new LogWriter(descriptorFile.get()));
			//s = WriteSnapshot(descriptor_log_);
		}
	}

	// Write new record to MANIFEST log
	if (s.ok())
	{
		std::string record;
		edit->encodeTo(&record);
		s = descriptorLog->addRecord(record);
		if (s.ok())
		{
			s = descriptorFile->sync();
		}

		if (!s.ok())
		{
			printf("MANIFEST write: %s\n", s.toString().c_str());
		}
	}

	// If we just created a new descriptor file, install it by writing a
	// new CURRENT file that points to it.
	if (s.ok() && !newManifestFile.empty())
	{
		s = setCurrentFile(options.env, dbname, manifestFileNumber);
	}

	// Install the new version
	if (s.ok())
	{
		appendVersion(v);
		logNumber = edit->logNumber;
		prevLogNumber = edit->prevLogNumber;
	}
	else
	{
		if (!newManifestFile.empty())
		{
			options.env->deleteFile(newManifestFile);
		}
	}
	return s;
}

// Apply all of the edits in *edit to the current state.
void Builder::apply(VersionEdit *edit)
{
	//Update compaction pointers
	for (size_t i = 0; i < edit->compactPointers.size(); i++)
	{
		const int level = edit->compactPointers[i].first;
		std::string_view view = edit->compactPointers[i].second.encode();
		vset->compactPointer[level] = std::string(view.data(), view.size());
	}

	// Delete files
	const VersionEdit::DeletedFileSet &del = edit->deletedFiles;
	for (auto iter = del.begin(); iter != del.end(); ++iter)
	{
		const int level = iter->first;
		const uint64_t number = iter->second;
		levels[level].deletedFiles.insert(number);
	}

	// Add new files
	for (size_t i = 0; i < edit->newFiles.size(); i++)
	{
		const int level = edit->newFiles[i].first;
		std::shared_ptr<FileMetaData> f(new FileMetaData(edit->newFiles[i].second));
		f->refs = 1;

		// We arrange to automatically compact this file after
		// a certain number of seeks.  Let's assume:
		//   (1) One seek costs 10ms
		//   (2) Writing or reading 1MB costs 10ms (100MB/s)
		//   (3) A compaction of 1MB does 25MB of IO:
		//         1MB read from this level
		//         10-12MB read from next level (boundaries may be misaligned)
		//         10-12MB written to next level
		// This implies that 25 seeks cost the same as the compaction
		// of 1MB of data.  I.e., one seek costs approximately the
		// same as the compaction of 40KB of data.  We are a little
		// conservative and allow approximately one seek for every 16KB
		// of data before triggering a compaction.
		f->allowedSeeks = (f->fileSize / 16384);
		if (f->allowedSeeks < 100) f->allowedSeeks = 100;

		levels[level].deletedFiles.erase(f->number);
		levels[level].addedFiles->insert(f);
	}
}

// Save the current state in *v.
void Builder::saveTo(Version *v)
{
	BySmallestKey cmp;
	for (int level = 0; level < kNumLevels; level++)
	{
		// Merge the set of added files with the set of pre-existing files.
		// Drop any deleted files.  Store the result in *v.
		auto baseFiles = base->files[level];
		auto baseIter = baseFiles.begin();
		auto baseEnd = baseFiles.end();
		auto added = levels[level].addedFiles;
		v->files[level].reserve(baseFiles.size() + added->size());
		for (auto addedIter = added->begin();
			addedIter != added->end();
			++addedIter)
		{
			// Add all smaller files listed in base_
			for (auto bpos = std::upper_bound(baseIter, baseEnd, *addedIter, cmp);
				baseIter != bpos; ++baseIter)
			{
				maybeAddFile(v, level, (*baseIter));
			}

			maybeAddFile(v, level, (*addedIter));
		}

		// Add remaining base files
		for (; baseIter != baseEnd; ++baseIter)
		{
			maybeAddFile(v, level, (*baseIter));
		}

#ifndef NDEBUG
		// Make sure there is no overlap in levels > 0
		if (level > 0)
		{
			for (uint32_t i = 1; i < v->files[level].size(); i++)
			{
				const InternalKey &prevEnd = v->files[level][i - 1]->largest;
				const InternalKey &thisBegin = v->files[level][i]->smallest;
				if (vset->icmp.compare(prevEnd, thisBegin) >= 0)
				{
					fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
						prevEnd.debugString().c_str(),
						thisBegin.debugString().c_str());
					abort();
				}
			}
		}
#endif
	}
}

void Builder::maybeAddFile(Version *v, int level, const std::shared_ptr<FileMetaData> &f)
{
	if (levels[level].deletedFiles.count(f->number) > 0)
	{
		// File is deleted: do nothing
	}
	else
	{
		auto files = v->files[level];
		if (level > 0 && !files.empty())
		{
			// Must not overlap
			assert(vset->icmp.compare(files[files.size() - 1]->largest,
				f->smallest) < 0);
		}

		f->refs++;
		files.push_back(f);
	}
}


