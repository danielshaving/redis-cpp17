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
    const Comparator *ucmp;
    std::string_view userKey;
    std::string *value;
};

static bool afterFile(const Comparator *ucmp,
                      const std::string_view *userKey, const FileMetaData *f) {
    // null user_key occurs before all keys and is therefore never after *f
    return (userKey != nullptr &&
            ucmp->compare(*userKey, f->largest.userKey()) > 0);
}

static bool beforeFile(const Comparator *ucmp,
                       const std::string_view *userKey, const FileMetaData *f) {
    // null user_key occurs after all keys and is therefore never before *f
    return (userKey != nullptr &&
            ucmp->compare(*userKey, f->smallest.userKey()) < 0);
}

static size_t targetFileSize(const Options *options) {
    return options->maxFileSize;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t maxGrandParentOverlapBytes(const Options *options) {
    return 10 * targetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t expandedCompactionByteSizeLimit(const Options *options) {
    return 25 * targetFileSize(options);
}

static double maxBytesForLevel(const Options *options, int level) {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    double result = 10. * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
}

static uint64_t maxFileSizeForLevel(const Options *options, int level) {
    // We could vary per level to reduce number of files?
    return targetFileSize(options);
}

static bool newestFirst(const std::shared_ptr <FileMetaData> &a, const std::shared_ptr <FileMetaData> &b) {
    return a->number > b->number;
}

static int64_t totalFileSize(const std::vector <std::shared_ptr<FileMetaData>> &files) {
    int64_t sum = 0;
    for (size_t i = 0; i < files.size(); i++) {
        sum += files[i]->fileSize;
    }
    return sum;
}

static std::shared_ptr <Iterator> getFileIterator(const std::any &arg,
                                                  const ReadOptions &options,
                                                  const std::string_view &fileValue) {
    std::shared_ptr <TableCache> cache = std::any_cast < std::shared_ptr < TableCache >> (arg);
    if (fileValue.size() != 16) {
        return newErrorIterator(
                Status::corruption("FileReader invoked with unexpected value"));
    } else {
        return cache->newIterator(options,
                                  decodeFixed64(fileValue.data()),
                                  decodeFixed64(fileValue.data() + 8));
    }
}

int findFile(const InternalKeyComparator &icmp,
             const std::vector <std::shared_ptr<FileMetaData>> &files,
             const std::string_view &key) {
    uint32_t left = 0;
    uint32_t right = files.size();
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        auto f = files[mid];
        if (icmp.compare(f->largest.encode(), key) < 0) {
            // Key at "mid.largest" is < "target".  Therefore all
            // files at or before "mid" are uninteresting.
            left = mid + 1;
        } else {
            // Key at "mid.largest" is >= "target".  Therefore all files
            // after "mid" are uninteresting.
            right = mid;
        }
    }
    return right;
}

bool someFileOverlapsRange(const InternalKeyComparator &icmp, bool disjointSortedFiles,
                           const std::vector <std::shared_ptr<FileMetaData>> &files,
                           const std::string_view *smallestUserKey,
                           const std::string_view *largestUserKey) {
    const Comparator *ucmp = icmp.getComparator();
    if (!disjointSortedFiles) {
        // Need to check against all files
        for (size_t i = 0; i < files.size(); i++) {
            std::shared_ptr <FileMetaData> f = files[i];
            if (afterFile(ucmp, smallestUserKey, f.get()) ||
                beforeFile(ucmp, largestUserKey, f.get())) {
                // No overlap
            } else {
                return true;  // Overlap
            }
        }
        return false;
    }

    // Binary search over file list
    uint32_t index = 0;
    if (smallestUserKey != nullptr) {
        // Find the earliest possible internal key for smallest_user_key
        InternalKey small(*smallestUserKey, kMaxSequenceNumber, kValueTypeForSeek);
        index = findFile(icmp, files, small.encode());
    }

    if (index >= files.size()) {
        // beginning of range is after all files, so no overlap.
        return false;
    }
    return !beforeFile(ucmp, largestUserKey, files[index].get());
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
public:
    LevelFileNumIterator(const InternalKeyComparator &icmp,
                         const std::vector <std::shared_ptr<FileMetaData>> *flist)
            : icmp(icmp),
              flist(flist),
              index(flist->size()) {
        // Marks as invalid
    }

    virtual bool valid() const {
        return index < flist->size();
    }

    virtual void seek(const std::string_view &target) {
        index = findFile(icmp, *flist, target);
    }

    virtual void seekToFirst() { index = 0; }

    virtual void seekToLast() {
        index = flist->empty() ? 0 : flist->size() - 1;
    }

    virtual void next() {
        assert(valid());
        index++;
    }

    virtual void prev() {
        assert(valid());
        if (index == 0) {
            index = flist->size();  // Marks as invalid
        } else {
            index--;
        }
    }

    virtual std::string_view key() const {
        assert(valid());
        return (*flist)[index]->largest.encode();
    }

    virtual std::string_view value() const {
        assert(valid());
        encodeFixed64(valueBuf, (*flist)[index]->number);
        encodeFixed64(valueBuf + 8, (*flist)[index]->fileSize);
        return std::string_view(valueBuf, sizeof(valueBuf));
    }

    virtual Status status() const { return Status::OK(); }

    virtual void registerCleanup(const std::any &arg) {}

private:
    const InternalKeyComparator icmp;
    const std::vector <std::shared_ptr<FileMetaData>> *const flist;
    uint32_t index;

    // Backing store for value().  Holds the file number and size.
    mutable char valueBuf[16];
};


bool Version::overlapInLevel(int level, const std::string_view *smallestUserKey,
                             const std::string_view *largestUserKey) {
    return someFileOverlapsRange(vset->icmp, level > 0, files[level],
                                 smallestUserKey, largestUserKey);
}

std::shared_ptr <Iterator> Version::newConcatenatingIterator(const ReadOptions &op, int level) const {
    std::shared_ptr <Iterator> iter(new Version::LevelFileNumIterator(vset->icmp, &files[level]));
    return newTwoLevelIterator(
            iter, std::bind(getFileIterator, std::placeholders::_1,
                            std::placeholders::_2, std::placeholders::_3), vset->tableCache, op);
}

void Version::addIterators(const ReadOptions &ops, std::vector <std::shared_ptr<Iterator>> *iters) {
    // Merge all level zero files together since they may overlap
    for (size_t i = 0; i < files[0].size(); i++) {
        iters->push_back(
                vset->tableCache->newIterator(
                        ops, files[0][i]->number, files[0][i]->fileSize));
    }

    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    for (int level = 1; level < kNumLevels; level++) {
        if (!files[level].empty()) {
            iters->push_back(newConcatenatingIterator(ops, level));
        }
    }
}

bool Version::updateStats(const GetStats &stats) {
    auto f = stats.seekFile;
    if (f != nullptr) {
        f->allowedSeeks--;
        if (f->allowedSeeks <= 0 && fileToCompact == nullptr) {
            fileToCompact = f;
            fileToCompactLevel = stats.seekFileLevel;
            return true;
        }
    }
    return false;
}

Status Version::get(const ReadOptions &options, const LookupKey &key, std::string *value, GetStats *stats) {
    std::string_view ikey = key.internalKey();
    std::string_view userKey = key.userKey();
    const Comparator *ucmp = vset->icmp.getComparator();
    Status s;

    stats->seekFile = nullptr;
    stats->seekFileLevel = -1;
    std::shared_ptr <FileMetaData> lastFileRead = nullptr;
    int lastFileReadLevel = -1;

    // We can search level-by-level since entries never hop across
    // levels.  Therefore we are guaranteed that if we find data
    // in an smaller level, later levels are irrelevant.
    std::vector <std::shared_ptr<FileMetaData>> tmp;
    std::shared_ptr <FileMetaData> tmp2;
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
                const auto &f = fs[i];
                if (ucmp->compare(userKey, f->smallest.userKey()) >= 0 &&
                    ucmp->compare(userKey, f->largest.userKey()) <= 0) {
                    tmp.push_back(f);
                }
            }

            if (tmp.empty()) continue;

            std::sort(tmp.begin(), tmp.end(), newestFirst);
            fs = tmp;
            numFiles = tmp.size();
        } else {
            // Binary search to find earliest index whose largest key >= ikey.
            uint32_t index = findFile(vset->icmp, files[level], ikey);
            if (index >= numFiles) {
                fs.clear();
                numFiles = 0;
            } else {
                tmp2 = fs[index];
                if (ucmp->compare(userKey, tmp2->smallest.userKey()) < 0) {
                    // All of "tmp2" is past any data for user_key
                    fs.clear();
                    numFiles = 0;
                } else {
                    fs.clear();
                    fs.push_back(tmp2);
                    numFiles = 1;
                }
            }
        }

        for (uint32_t i = 0; i < numFiles; ++i) {
            if (lastFileRead != nullptr && stats->seekFile == nullptr) {
                // We have had more than one seek for this read.  Charge the 1st file.
                stats->seekFile = lastFileRead;
                stats->seekFileLevel = lastFileReadLevel;
            }

            std::shared_ptr <FileMetaData> f = fs[i];
            lastFileRead = f;
            lastFileReadLevel = level;

            Saver saver;
            saver.state = kNotFound;
            saver.ucmp = ucmp;
            saver.userKey = userKey;
            saver.value = value;
            s = vset->getTableCache()->get(options, f->number, f->fileSize,
                                           ikey, &saver, std::bind(&Version::saveValue, this,
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
                    s = Status::notFound(std::string_view());  // Use empty error message for speed
                    return s;
                case kCorrupt:
                    s = Status::corruption("corrupted key for ", userKey);
                    return s;
            }
        }
    }
    return Status::notFound(std::string_view());  // Use an empty error message for speed
}

void Version::saveValue(const std::any &arg, const std::string_view &ikey, const std::string_view &v) {
    Saver *s = std::any_cast<Saver *>(arg);
    ParsedInternalKey parsedKey;
    if (!parseInternalKey(ikey, &parsedKey)) {
        s->state = kCorrupt;
    } else {
        if (s->ucmp->compare(parsedKey.userKey, s->userKey) == 0) {
            s->state = (parsedKey.type == kTypeValue) ? kFound : kDeleted;
            if (s->state == kFound) {
                s->value->assign(v.data(), v.size());
            }
        }
    }
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::getOverlappingInputs(
        int level,
        const InternalKey *begin,         // nullptr means before all keys
        const InternalKey *end,           // nullptr means after all keys
        std::vector <std::shared_ptr<FileMetaData>> *inputs) {
    assert(level >= 0);
    assert(level < kNumLevels);

    inputs->clear();
    std::string_view userBegin, userEnd;

    if (begin != nullptr) {
        userBegin = begin->userKey();
    }

    if (end != nullptr) {
        userEnd = end->userKey();
    }

    const Comparator *ucmp = vset->icmp.getComparator();

    for (size_t i = 0; i < files[level].size();) {
        const std::shared_ptr <FileMetaData> &f = files[level][i++];
        const std::string_view fileStart = f->smallest.userKey();
        const std::string_view fileLimit = f->largest.userKey();

        if (begin != nullptr && ucmp->compare(fileLimit, userBegin) < 0) {
            // "f" is completely before specified range; skip it
        } else if (end != nullptr && ucmp->compare(fileStart, userEnd) > 0) {
            // "f" is completely after specified range; skip it
        } else {
            inputs->push_back(f);
            if (level == 0) {
                // Level-0 files may overlap each other.  So check if the newly
                // added file has expanded the range.  If so, restart search.
                if (begin != nullptr && ucmp->compare(fileStart, userBegin) < 0) {
                    userBegin = fileStart;
                    inputs->clear();
                    i = 0;
                } else if (end != nullptr && ucmp->compare(fileLimit, userEnd) > 0) {
                    userEnd = fileLimit;
                    inputs->clear();
                    i = 0;
                }
            }
        }
    }
}

std::string Version::debugString() const {
    std::string r;
    for (int level = 0; level < kNumLevels; level++) {
        // E.g.,
        //   --- level 1 ---
        //   17:123['a' .. 'd']
        //   20:43['e' .. 'g']
        r.append("--- level ");
        appendNumberTo(&r, level);
        r.append(" ---\n");
        const std::vector <std::shared_ptr<FileMetaData>> &f = files[level];
        for (size_t i = 0; i < f.size(); i++) {
            r.push_back(' ');
            appendNumberTo(&r, f[i]->number);
            r.push_back(':');
            appendNumberTo(&r, f[i]->fileSize);
            r.append("[");
            r.append(f[i]->smallest.debugString());
            r.append(" .. ");
            r.append(f[i]->largest.debugString());
            r.append("]\n");
        }
    }
    return r;
}

int Version::pickLevelForMemTableOutput(const std::string_view &smallestUserKey,
                                        const std::string_view &largestUserKey) {
    int level = 0;
    if (!overlapInLevel(0, &smallestUserKey, &largestUserKey)) {
        // Push to next level if there is no overlap in next level,
        // and the #bytes overlapping in the level after that are limited.
        InternalKey start(smallestUserKey, kMaxSequenceNumber, kValueTypeForSeek);
        InternalKey limit(largestUserKey, 0, static_cast<ValueType>(0));
        std::vector <std::shared_ptr<FileMetaData>> overlaps;

        while (level < kMaxMemCompactLevel) {
            if (overlapInLevel(level + 1, &smallestUserKey, &largestUserKey)) {
                break;
            }

            if (level + 2 < kNumLevels) {
                // Check that file does not overlap too many grandparent bytes.
                getOverlappingInputs(level + 2, &start, &limit, &overlaps);
                const int64_t sum = totalFileSize(overlaps);
                if (sum > maxGrandParentOverlapBytes(&vset->options)) {
                    break;
                }
            }
            level++;
        }
    }
    return level;
}


Builder::Builder(VersionSet *vset, const std::shared_ptr <Version> &base)
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

VersionSet::VersionSet(const std::string &dbname,
                       const Options &options, const std::shared_ptr <TableCache> &tableCache,
                       const InternalKeyComparator *cmp)
        : dbname(dbname),
          options(options),
          lastSequence(0),
          nextFileNumber(2),
          logNumber(0),
          prevLogNumber(0),
          manifestFileNumber(0),
          descriptorLog(nullptr),
          descriptorFile(nullptr),
          tableCache(tableCache),
          icmp(*cmp) {
    std::shared_ptr <Version> v(new Version(this));
    appendVersion(v);
}

VersionSet::~VersionSet() {

}

Status VersionSet::recover(bool *manifest) {
    // Read "CURRENT" file, which contains a pointer to the current manifest file
    std::string cur;
    Status s = readFileToString(options.env.get(), currentFileName(dbname), &cur);
    if (!s.ok()) {
        return s;
    }

    if (cur.empty() || cur[cur.size() - 1] != '\n') {
        return Status::corruption("CURRENT file does not end with newline");
    }

    cur.resize(cur.size() - 1);
    std::string dscname = dbname + "/" + cur;
    std::shared_ptr <PosixSequentialFile> file;
    s = options.env->newSequentialFile(dscname, file);
    if (!s.ok()) {
        if (s.isNotFound()) {
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

    Builder builder(this, current);
    LogReporter reporter;
    reporter.status = &s;

    LogReader reader(file.get(), &reporter, true/*checksum*/, 0/*initial_offset*/);
    std::string_view record;
    std::string scratch;
    while (reader.readRecord(&record, &scratch) && s.ok()) {
        VersionEdit edit;
        s = edit.decodeFrom(record);
        if (s.ok()) {
            builder.apply(&edit);
        }

        if (edit.hasLogNumber) {
            logNumber = edit.logNumber;
            haveLogNumber = true;
        }

        if (edit.hasPrevLogNumber) {
            prevLogNumber = edit.prevLogNumber;
            havePrevLogNumber = true;
        }

        if (edit.hasNextFileNumber) {
            nextFile = edit.nextFileNumber;
            haveNextFile = true;
        }

        if (edit.hasLastSequence) {
            lastSequence = edit.lastSequence;
            haveLastSequence = true;
        }
    }

    if (s.ok()) {
        if (!haveNextFile) {
            s = Status::corruption("no meta-nextfile entry in descriptor");
        } else if (!haveLogNumber) {
            s = Status::corruption("no meta-lognumber entry in descriptor");
        } else if (!haveLastSequence) {
            s = Status::corruption("no last-sequence-number entry in descriptor");
        }

        if (!havePrevLogNumber) {
            prevLogNumber = 0;
        }

        markFileNumberUsed(prevLogNumber);
        markFileNumberUsed(logNumber);
    }

    if (s.ok()) {
        std::shared_ptr <Version> v(new Version(this));
        builder.saveTo(v.get());
        appendVersion(v); // Install recovered version
        finalize(v.get());

        this->manifestFileNumber = nextFile;
        this->nextFileNumber = nextFile + 1;
        this->lastSequence = lastSequence;
        this->logNumber = logNumber;
        this->prevLogNumber = prevLogNumber;
        //See if we can reuse the existing MANIFEST file.
        if (reuseManifest(dscname, cur)) {
            // No need to save new manifest
        } else {
            *manifest = true;
        }
    }
    return s;
}

void VersionSet::addLiveFiles(std::set <uint64_t> *live) {
	for (int level = 0; level < kNumLevels; level++) {
		auto &files = current->files[level];
		for (size_t i = 0; i < files.size(); i++) {
			live->insert(files[i]->number);
		}
	}
}

std::shared_ptr <Iterator> VersionSet::makeInputIterator(Compaction *c) {
    ReadOptions ops;
    ops.verifyChecksums = options.paranoidChecks;
    ops.fillCache = false;
    // Level-0 files have to be merged together.  For other levels,
    // we will make a concatenating iterator per level.
    // TODO(opt): use concatenating iterator for level-0 if there is no overlap
    const int space = (c->getLevel() == 0 ? c->inputs[0].size() + 1 : 2);
	
	std::vector <std::shared_ptr<Iterator>> list;
	list.resize(space);
   
    int num = 0;
    for (int which = 0; which < 2; which++) {
        if (!c->inputs[which].empty()) {
            if (c->getLevel() + which == 0) {
                const auto &files = c->inputs[which];
                for (size_t i = 0; i < files.size(); i++) {
					std::shared_ptr <Iterator> iter = tableCache->newIterator(
                            ops, files[i]->number, files[i]->fileSize, nullptr);
                    list[num++] = iter;
                }
            } else {
                // Create concatenating iterator for the files from this level
                std::shared_ptr <Iterator> iter(new Version::LevelFileNumIterator(icmp, &c->inputs[which]));
                list[num++] = newTwoLevelIterator(
                        iter, std::bind(getFileIterator, std::placeholders::_1,
                                        std::placeholders::_2, std::placeholders::_3), tableCache, ops);
            }
        }
    }

    assert(num <= space);
    std::shared_ptr <Iterator> result = newMergingIterator(&icmp, list, num);
    return result;
}

void VersionSet::finalize(Version *v) {
    // Precomputed best level for next compaction
    int bestLevel = -1;
    double bestScore = -1;

    for (int level = 0; level < kNumLevels - 1; level++) {
        double score;
        if (level == 0) {
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
        } else {
            // Compute the ratio of current size to size limit.
            const uint64_t levelBytes = totalFileSize(v->files[level]);
            score = static_cast<double>(levelBytes) / maxBytesForLevel(&options, level);
        }

        if (score > bestScore) {
            bestLevel = level;
            bestScore = score;
        }
    }

    v->compactionLevel = bestLevel;
    v->compactionScore = bestScore;
}

bool VersionSet::reuseManifest(const std::string &dscname, const std::string &dscbase) {
    if (!options.reuseLogs) {
        return false;
    }

    FileType manifestType;
    uint64_t manifestNumber;
    uint64_t manifestSize;
    if (!parseFileName(dscbase, &manifestNumber, &manifestType) ||
        manifestType != kDescriptorFile ||
        !options.env->getFileSize(dscname, &manifestSize).ok() ||
        // Make new compacted MANIFEST if old one is too big
        manifestSize >= targetFileSize(&options)) {
        return false;
    }

    assert(descriptorFile == nullptr);
    assert(descriptorLog == nullptr);
    Status r = options.env->newAppendableFile(dscname, descriptorFile);
    if (!r.ok()) {
        printf("Reuse MANIFEST: %s\n", r.toString().c_str());
        assert(descriptorFile == nullptr);
        return false;
    }

    printf("Reuse MANIFEST: %s\n", dscname.c_str());
    descriptorLog.reset(new LogWriter(descriptorFile.get(), manifestSize));
    manifestFileNumber = manifestNumber;
    return true;
}

void VersionSet::markFileNumberUsed(uint64_t number) {
    if (nextFileNumber <= number) {
        nextFileNumber = number + 1;
    }
}

int64_t VersionSet::numLevelBytes(int level) const {
    assert(level >= 0);
    assert(level < kNumLevels);
    return totalFileSize(current->files[level]);
}

int VersionSet::numLevelFiles(int level) const {
    assert(level >= 0);
    assert(level < kNumLevels);
    return current->files[level].size();
}

void VersionSet::appendVersion(const std::shared_ptr <Version> &v) {
	current = v;
}

Status VersionSet::logAndApply(VersionEdit *edit) {
    if (edit->hasLogNumber) {
        assert(edit->logNumber >= logNumber);
        assert(edit->logNumber < nextFileNumber);
    } else {
        edit->setLogNumber(logNumber);
    }

    if (!edit->hasPrevLogNumber) {
        edit->setPrevLogNumber(prevLogNumber);
    }

    edit->setNextFile(nextFileNumber);
    edit->setLastSequence(lastSequence);

    std::shared_ptr <Version> v(new Version(this));
    {
        Builder builder(this, current);
        builder.apply(edit);
        builder.saveTo(v.get());
    }

    finalize(v.get());
    // Initialize new descriptor log file if necessary by creating
    // a temporary file that contains a snapshot of the current version.
    std::string newManifestFile;
    Status s;
    if (descriptorLog == nullptr) {
        // No reason to unlock *mu here since we only hit this path in the
        // first call to LogAndApply (when opening the database).
        assert(descriptorFile == nullptr);
        newManifestFile = descriptorFileName(dbname, manifestFileNumber);
        edit->setNextFile(nextFileNumber);
        s = options.env->newWritableFile(newManifestFile, descriptorFile);
        if (s.ok()) {
            descriptorLog.reset(new LogWriter(descriptorFile.get()));
            s = writeSnapshot();
        } else {

        }
    }

    // Write new record to MANIFEST log
    if (s.ok()) {
        std::string record;
        edit->encodeTo(&record);
        s = descriptorLog->addRecord(record);
        if (s.ok()) {
            s = descriptorFile->sync();
        }

        if (!s.ok()) {
            printf("MANIFEST write: %s\n", s.toString().c_str());
        }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !newManifestFile.empty()) {
        s = setCurrentFile(options.env.get(), dbname, manifestFileNumber);
    }

    // Install the new version
    if (s.ok()) {
        appendVersion(v);
        logNumber = edit->logNumber;
        prevLogNumber = edit->prevLogNumber;
    } else {
        if (!newManifestFile.empty()) {
			descriptorLog.reset();
			descriptorFile.reset();
            options.env->deleteFile(newManifestFile);
        }
    }
    return s;
}

Status VersionSet::writeSnapshot() {
	// TODO: Break up into multiple records to reduce memory usage on recovery?

	// Save metadata
	VersionEdit edit;
	edit.setComparatorName(icmp.getComparator()->name());

	// Save compaction pointers
	for (int level = 0; level < kNumLevels; level++) {
		if (!compactPointer[level].empty()) {
		  InternalKey key;
		  key.decodeFrom(compactPointer[level]);
		  edit.setCompactPointer(level, key);
		}
	}

	// Save files
	for (int level = 0; level < kNumLevels; level++) {
		const auto &files = current->files[level];
		for (size_t i = 0; i < files.size(); i++) {
			const auto f = files[i];
			edit.addFile(level, f->number, f->fileSize, f->smallest, f->largest);
		}
	}

	std::string record;
	edit.encodeTo(&record);
	return descriptorLog->addRecord(record);
}

const char *VersionSet::levelSummary(LevelSummaryStorage *scratch) const {
    // Update code if kNumLevels changes
    assert(kNumLevels == 7);
    snprintf(scratch->buffer, sizeof(scratch->buffer),
             "files[ %d %d %d %d %d %d %d ]",
             int(current->files[0].size()),
             int(current->files[1].size()),
             int(current->files[2].size()),
             int(current->files[3].size()),
             int(current->files[4].size()),
             int(current->files[5].size()),
             int(current->files[6].size()));
    return scratch->buffer;
}

std::shared_ptr <Compaction> VersionSet::pickCompaction() {
    std::shared_ptr <Compaction> c; 
    int level;
    // We prefer compactions triggered by too much data in a level over
    // the compactions triggered by seeks.

    const bool sizeCompaction = (current->compactionScore >= 1);
    const bool seekCompaction = (current->fileToCompact != nullptr);
    if (sizeCompaction) {
        level = current->compactionLevel;
        assert(level >= 0);
        assert(level + 1 < kNumLevels);
        c.reset(new Compaction(&options, level));

        // Pick the first file that comes after compact_pointer_[level]
        for (size_t i = 0; i < current->files[level].size(); i++) {
            auto f = current->files[level][i];
            if (compactPointer[level].empty() ||
                icmp.compare(f->largest.encode(), compactPointer[level]) > 0) {
                c->inputs[0].push_back(f);
                break;
            }
        }

        if (c->inputs[0].empty()) {
            // Wrap-around to the beginning of the key space
            c->inputs[0].push_back(current->files[level][0]);
        }
    } else if (seekCompaction) {
        level = current->fileToCompactLevel;
        c.reset(new Compaction(&options, level));
        c->inputs[0].push_back(current->fileToCompact);
    } else {
        return nullptr;
    }

    c->inputVersion = current;
    // Files in level 0 may overlap each other, so pick up all overlapping ones
    if (level == 0) {
        InternalKey smallest, largest;
        getRange(c->inputs[0], &smallest, &largest);
        // Note that the next call will discard the file we placed in
        // c->inputs_[0] earlier and replace it with an overlapping set
        // which will include the picked file.
        current->getOverlappingInputs(0, &smallest, &largest, &c->inputs[0]);
        assert(!c->inputs[0].empty());
    }

    setupOtherInputs(c);
    return c;
}

std::shared_ptr <Compaction> VersionSet::compactRange(
        int level,
        const InternalKey *begin,
        const InternalKey *end) {
    std::vector <std::shared_ptr<FileMetaData>> inputs;
    current->getOverlappingInputs(level, begin, end, &inputs);
    if (inputs.empty()) {
        return nullptr;
    }

    // Avoid compacting too much in one shot in case the range is large.
    // But we cannot do this for level-0 since level-0 files can overlap
    // and we must not pick one file and drop another older file if the
    // two files overlap.
    if (level > 0) {
        const uint64_t limit = maxFileSizeForLevel(&options, level);
        uint64_t total = 0;
        for (size_t i = 0; i < inputs.size(); i++) {
            uint64_t s = inputs[i]->fileSize;
            total += s;
            if (total >= limit) {
                inputs.resize(i + 1);
                break;
            }
        }
    }

    std::shared_ptr <Compaction> c(new Compaction(&options, level));
    c->inputVersion = current;
    c->inputs[0] = inputs;
    setupOtherInputs(c);
    return c;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::getRange(const std::vector <std::shared_ptr<FileMetaData>> &inputs,
                          InternalKey *smallest,
                          InternalKey *largest) {
    assert(!inputs.empty());
    smallest->clear();
    largest->clear();
    for (size_t i = 0; i < inputs.size(); i++) {
        auto f = inputs[i];
        if (i == 0) {
            *smallest = f->smallest;
            *largest = f->largest;
        } else {
            if (icmp.compare(f->smallest, *smallest) < 0) {
                *smallest = f->smallest;
            }

            if (icmp.compare(f->largest, *largest) > 0) {
                *largest = f->largest;
            }
        }
    }
}

void VersionSet::getRange2(const std::vector <std::shared_ptr<FileMetaData>> &inputs1,
                           const std::vector <std::shared_ptr<FileMetaData>> &inputs2,
                           InternalKey *smallest,
                           InternalKey *largest) {
    std::vector <std::shared_ptr<FileMetaData>> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end());
    getRange(all, smallest, largest);
}

void VersionSet::setupOtherInputs(const std::shared_ptr <Compaction> &c) {
    const int level = c->getLevel();
    InternalKey smallest, largest;
    getRange(c->inputs[0], &smallest, &largest);

    current->getOverlappingInputs(level + 1, &smallest, &largest, &c->inputs[1]);

    // Get entire range covered by compaction
    InternalKey allStart, allLimit;
    getRange2(c->inputs[0], c->inputs[1], &allStart, &allLimit);

    // See if we can grow the number of inputs in "level" without
    // changing the number of "level+1" files we pick up.
    if (!c->inputs[1].empty()) {
        std::vector <std::shared_ptr<FileMetaData>> expanded0;
        current->getOverlappingInputs(level, &allStart, &allLimit, &expanded0);
        const int64_t inputs0Size = totalFileSize(c->inputs[0]);
        const int64_t inputs1Size = totalFileSize(c->inputs[1]);
        const int64_t expanded0Size = totalFileSize(expanded0);
        if (expanded0.size() > c->inputs[0].size() &&
            inputs1Size + expanded0Size <
            expandedCompactionByteSizeLimit(&options)) {
            InternalKey newStart, newLimit;
            getRange(expanded0, &newStart, &newLimit);
            std::vector <std::shared_ptr<FileMetaData>> expanded1;
            current->getOverlappingInputs(level + 1, &newStart, &newLimit,
                                            &expanded1);
            if (expanded1.size() == c->inputs[1].size()) {
                printf("Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
                       level,
                       int(c->inputs[0].size()),
                       int(c->inputs[1].size()),
                       long(inputs0Size), long(inputs1Size),
                       int(expanded0.size()),
                       int(expanded1.size()),
                       long(expanded0Size), long(inputs1Size));
                smallest = newStart;
                largest = newLimit;
                c->inputs[0] = expanded0;
                c->inputs[1] = expanded1;
                getRange2(c->inputs[0], c->inputs[1], &allStart, &allLimit);
            }
        }
    }

    // Compute the set of grandparent files that overlap this compaction
    // (parent == level+1; grandparent == level+2)
    if (level + 2 < kNumLevels) {
        current->getOverlappingInputs(level + 2, &allStart, &allLimit,
                                        &c->grandparents);
    }

    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    compactPointer[level] = std::string(largest.encode().data(), largest.encode().size());
    c->edit.setCompactPointer(level, largest);
}

// Apply all of the edits in *edit to the current state.
void Builder::apply(VersionEdit *edit) {
    //Update compaction pointers
    for (size_t i = 0; i < edit->compactPointers.size(); i++) {
        const int level = edit->compactPointers[i].first;
        std::string_view view = edit->compactPointers[i].second.encode();
        vset->compactPointer[level] = std::string(view.data(), view.size());
    }

    // Delete files
    auto &del = edit->deletedFiles;
    for (auto iter = del.begin(); iter != del.end(); ++iter) {
        const int level = iter->first;
        const uint64_t number = iter->second;
        levels[level].deletedFiles.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->newFiles.size(); i++) {
        const int level = edit->newFiles[i].first;
        std::shared_ptr <FileMetaData> f(new FileMetaData(edit->newFiles[i].second));
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
void Builder::saveTo(Version *v) {
    BySmallestKey cmp;
    cmp.internalComparator = &(vset->icmp);
    for (int level = 0; level < kNumLevels; level++) {
        // Merge the set of added files with the set of pre-existing files.
        // Drop any deleted files.  Store the result in *v.
        auto &baseFiles = base->files[level];
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
                maybeAddFile(v, level, (*baseIter));
            }
            maybeAddFile(v, level, (*addedIter));
        }

        // Add remaining base files
        for (; baseIter != baseEnd; ++baseIter) {
            maybeAddFile(v, level, (*baseIter));
        }

#ifdef NDEBUG
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

void Builder::maybeAddFile(Version *v, int level, const std::shared_ptr <FileMetaData> &f) {
    if (levels[level].deletedFiles.count(f->number) > 0) {
        // File is deleted: do nothing
    } else {
        auto &files = v->files[level];
        if (level > 0 && !files.empty()) {
            // Must not overlap
            assert(vset->icmp.compare(files[files.size() - 1]->largest,
                                       f->smallest) < 0);
        }

        f->refs++;
        files.push_back(f);
    }
}

Compaction::Compaction(const Options *options, int level)
        : level(level),
          maxOutputfileSize(maxFileSizeForLevel(options, level)),
          inputVersion(nullptr),
          grandparentIndex(0),
          seenKey(false),
          overlappedBytes(0) {
    for (int i = 0; i < kNumLevels; i++) {
        levelPtrs[i] = 0;
    }
}

Compaction::~Compaction() {

}

bool Compaction::isTrivialMove() const {
    const VersionSet *vset = inputVersion->vset;
    // Avoid a move if there is lots of overlapping grandparent data.
    // Otherwise, the move could create a parent file that will require
    // a very expensive merge later on.
    return (numInputFiles(0) == 1 && numInputFiles(1) == 0 &&
            totalFileSize(grandparents) <= maxGrandParentOverlapBytes(&vset->options));
}

void Compaction::addInputDeletions(VersionEdit *edit) {
    for (int which = 0; which < 2; which++) {
        for (size_t i = 0; i < inputs[which].size(); i++) {
            edit->deleteFile(level + which, inputs[which][i]->number);
        }
    }
}

bool Compaction::isBaseLevelForKey(const std::string_view &userKey) {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator *cmp = inputVersion->vset->icmp.getComparator();
    for (int lvl = level + 2; lvl < kNumLevels; lvl++) {
        auto &files = inputVersion->files[lvl];
        for (; levelPtrs[lvl] < files.size();) {
            auto f = files[levelPtrs[lvl]];
            if (cmp->compare(userKey, f->largest.userKey()) <= 0) {
                // We've advanced far enough
                if (cmp->compare(userKey, f->smallest.userKey()) >= 0) {
                    // Key falls in this file's range, so definitely not base level
                    return false;
                }
                break;
            }
            levelPtrs[lvl]++;
        }
    }
    return true;
}

bool Compaction::shouldStopBefore(const std::string_view &internalKey) {
    const VersionSet *vset = inputVersion->vset;
    // Scan to find earliest grandparent file that contains key.
    const InternalKeyComparator *icmp = &vset->icmp;
    while (grandparentIndex < grandparents.size() &&
           icmp->compare(internalKey,
                         grandparents[grandparentIndex]->largest.encode()) > 0) {
        if (seenKey) {
            overlappedBytes += grandparents[grandparentIndex]->fileSize;
        }
        grandparentIndex++;
    }

    seenKey = true;

    if (overlappedBytes > maxGrandParentOverlapBytes(&vset->options)) {
        // Too much overlap for current output; start new output
        overlappedBytes = 0;
        return true;
    } else {
        return false;
    }
}

void Compaction::releaseInputs() {
	inputVersion.reset();
}

