#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include <assert.h>
#include <random>

#include "dbimpl.h"
#include "filename.h"
#include "logwriter.h"
#include "logreader.h"
#include "tablecache.h"
#include "versionset.h"
#include "status.h"
#include "table.h"
#include "tablebuilder.h"
#include "block.h"
#include "merger.h"
#include "option.h"
#include "logging.h"
#include "env.h"

const int kNumNonTableCacheFiles = 10;

std::default_random_engine e;

std::string makeKey(unsigned int num) {
	char buf[30];
	snprintf(buf, sizeof(buf), "%016u", num);
	return std::string(buf);
}

class DBTest {
private:
	// Sequence of option configurations to try
	enum OptionConfig {
		kDefault,
		kReuse,
		kFilter,
		kUncompressed,
		kEnd
	};
	int optionConfig;

public:
	std::string dbname;
	std::shared_ptr <DBImpl> db;
	Options lastOptions;
	Env env;

	DBTest() : optionConfig(kDefault) {
		dbname = "db_test";
		reopen();
	}

	~DBTest() {
		db->destroyDB(dbname, Options());
	}

	void reopen(Options *options = nullptr) {
		assert(tryReopen(options).ok());
	}

	void destroyAndReopen(Options *options = nullptr) {
		assert(tryReopen(options).ok());
	}

	Status tryReopen(Options *options) {
		Options opts;
		if (options != nullptr) {
			opts = *options;
		}
		else {
			opts = currentOptions();
			opts.createIfMissing = true;
		}

		lastOptions = opts;
		db.reset(new DBImpl(opts, dbname));
		db->destroyDB(dbname, Options());
		Status s = db->open();
		return s;
	}

	// Switch to a fresh database with the next option configuration to
	// test.  Return false if there are no more configurations to test.
	bool changeOptions() {
		optionConfig++;
		if (optionConfig >= kEnd) {
			return false;
		}
		else {
			destroyAndReopen();
			return true;
		}
	}

	// Return the current option configuration.
	Options currentOptions() {
		Options options;
		options.reuseLogs = false;
		switch (optionConfig) {
		case kReuse:
			options.reuseLogs = false;
			break;
		case kFilter:
			break;
		case kUncompressed:
			options.compression = kNoCompression;
			break;
		default:
			break;
		}
		return options;
	}

	Status put(const std::string &k, const std::string &v) {
		return db->put(WriteOptions(), k, v);
	}

	Status del(const std::string &k) {
		return db->del(WriteOptions(), k);
	}

	std::string get(const std::string &k) {
		ReadOptions options;
		//options.snapshot = snapshot;
		std::string result;
		Status s = db->get(ReadOptions(), k, &result);
		if (s.isNotFound()) {
			result = "NOT_FOUND";
		}
		else if (!s.ok()) {
			result = s.toString();
		}
		return result;
	}

	std::string iterStatus(const std::shared_ptr <Iterator> &iter) {
		std::string result;
		if (iter->valid()) {
			std::string key = std::string(iter->key().data(), iter->key().size());
			std::string value = std::string(iter->value().data(), iter->value().size());
			result = key + "->" + value;
		}
		else {
			result = "(invalid)";
		}
		return result;
	}

	// Return a string that contains all key,value pairs in order,
	// formatted like "(k1->v1)(k2->v2)".
	std::string contents() {
		std::vector <std::string> forward;
		std::string result;
		std::shared_ptr <Iterator> iter = db->newIterator(ReadOptions());
		for (iter->seekToFirst(); iter->valid(); iter->next()) {
			std::string s = iterStatus(iter);
			result.push_back('(');
			result.append(s);
			result.push_back(')');
			forward.push_back(s);
		}

		// Check reverse iteration results are the reverse of forward results
		size_t matched = 0;
		for (iter->seekToLast(); iter->valid(); iter->prev()) {
			assert(matched < forward.size());
			assert(iterStatus(iter) == forward[forward.size() - matched - 1]);
			matched++;
		}

		assert(matched == forward.size());
		return result;
	}

	std::string allEntriesFor(const std::string_view &userKey) {
		std::shared_ptr <Iterator> iter = db->testNewInternalIterator();
		InternalKey target(userKey, kMaxSequenceNumber, kTypeValue);
		iter->seek(target.encode());
		std::string result;
		if (!iter->status().ok()) {
			result = iter->status().toString();
		}
		else {
			result = "[ ";
			bool first = true;
			while (iter->valid()) {
				ParsedInternalKey ikey;
				if (!parseInternalKey(iter->key(), &ikey)) {
					result += "CORRUPTED";
				}
				else {
					if (lastOptions.comparator->compare(ikey.userKey, userKey) != 0) {
						break;
					}
					if (!first) {
						result += ", ";
					}
					first = false;
					switch (ikey.type) {
					case kTypeValue:
						result += iter->value();
						break;
					case kTypeDeletion:
						result += "DEL";
						break;
					}
				}
				iter->next();
			}
			if (!first) {
				result += " ";
			}
			result += "]";
		}
		return result;
	}

	int numTableFilesAtLevel(int level) {
		std::string property;
		assert(db->getProperty("leveldb.num-files-at-level" + numberToString(level), &property));
		return std::stoi(property);
	}

	int totalTableFiles() {
		int result = 0;
		for (int level = 0; level < kNumLevels; level++) {
			result += numTableFilesAtLevel(level);
		}
		return result;
	}

	// Return spread of files per level
	std::string filesPerLevel() {
		std::string result;
		int lastOffest = 0;
		for (int level = 0; level < kNumLevels; level++) {
			int f = numTableFilesAtLevel(level);
			char buf[100];
			snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
			result += buf;
			if (f > 0) {
				lastOffest = result.size();
			}
		}
		result.resize(lastOffest);
		return result;
	}

	int countFiles() {
		std::vector <std::string> files;
		env.getChildren(dbname, &files);
		return static_cast<int>(files.size());
	}

	uint64_t size(const std::string_view &start, const std::string_view &limit) {
		Range r(start, limit);
		uint64_t size;
		db->getApproximateSizes(&r, 1, &size);
		return size;
	}

	void ompact(const std::string_view &start, const std::string_view &limit) {
		db->compactRange(&start, &limit);
	}

	// Do n memtable compactions, each of which produces an sstable
	// covering the range [small_key,large_key].
	void makeTables(int n, const std::string &smallKey,
		const std::string &largeKey) {
		for (int i = 0; i < n; i++) {
			put(smallKey, "begin");
			put(largeKey, "end");
			db->testCompactMemTable();
		}
	}

	// Prevent pushing of new sstables into deeper levels by adding
	// tables that cover a specified range to all levels.
	void fillLevels(const std::string &smallest, const std::string& largest) {
		makeTables(kNumLevels, smallest, largest);
	}

	void dumpFileCounts(const char* label) {
		fprintf(stderr, "---\n%s:\n", label);
		fprintf(stderr, "maxoverlap: %lld\n",
			static_cast<long long>(
				db->testMaxNextLevelOverlappingBytes()));
		for (int level = 0; level < kNumLevels; level++) {
			int num = numTableFilesAtLevel(level);
			if (num > 0) {
				fprintf(stderr, "  level %3d : %d files\n", level, num);
			}
		}
	}

	std::string dumpSSTableList() {
		std::string property;
		db->getProperty("leveldb.sstables", &property);
		return property;
	}

	std::string IterStatus(const std::shared_ptr <Iterator> &iter) {
		std::string result;
		if (iter->valid()) {
			std::string key = std::string(iter->key().data(), iter->key().size());
			std::string value = std::string(iter->value().data(), iter->value().size());
			result = key + "->" + value;
		}
		else {
			result = "(invalid)";
		}
		return result;
	}

	bool deleteAnSSTFile() {
		std::vector <std::string> filenames;
		assert(env.getChildren(dbname, &filenames).ok());
		uint64_t number;
		FileType type;
		for (size_t i = 0; i < filenames.size(); i++) {
			if (parseFileName(filenames[i], &number, &type) && type == kTableFile) {
				assert(env.deleteFile(tableFileName(dbname, number)).ok());
				return true;
			}
		}
		return false;
	}

	// Returns number of files renamed.
	int renameLDBToSST() {
		std::vector <std::string> filenames;
		assert(env.getChildren(dbname, &filenames).ok());
		uint64_t number;
		FileType type;
		int filesRenamed = 0;
		for (size_t i = 0; i < filenames.size(); i++) {
			if (parseFileName(filenames[i], &number, &type) && type == kTableFile) {
				const std::string from = tableFileName(dbname, number);
				const std::string to = sstTableFileName(dbname, number);
				assert(env.renameFile(from, to).ok());
				filesRenamed++;
			}
		}
		return filesRenamed;
	}

	void empty() {
		do {
			assert(db != nullptr);
			assert("NOT_FOUND" == get("foo"));
		} while (changeOptions());
	}

	void emptyKey() {
		do {
			assert(put("", "v1").ok());
			assert("v1" == get(""));
			assert(put("", "v2").ok());
			assert("v2" == get(""));
		} while (changeOptions());
	}

	void emptyValue() { 
		do {
			assert(put("key", "v1").ok());
			assert("v1" == get("key"));
			assert(put("key", "").ok());
			assert("" == get("key"));
			assert(put("key", "v2").ok());
			assert("v2" == get("key"));
		} while (changeOptions());
	}
	
	void readWrite() {
		do {
			assert(put("foo", "v1").ok());
			assert("v1" == get("foo"));
			assert(put("bar", "v2").ok());
			assert(put("foo", "v3").ok());
			assert("v3" == get("foo"));
			assert("v2"== get("bar"));
		}while (changeOptions());
	}
};

void bmLogAndApply(int iters, int numbasefiles) {
	Options opts;
	opts.createIfMissing = true;
	std::string dbname = "./leveldb_test_benchmark";
	std::shared_ptr <Env> env(new	Env());
	DBImpl db(opts, dbname);
	db.destroyDB(dbname, opts);

	Status s = db.open();
	assert(s.ok());

	BytewiseComparatorImpl byteImpl;
	InternalKeyComparator cmp(&byteImpl);
	Options options;
	VersionSet vset(dbname, options, nullptr, &cmp);
	bool manifest;
	assert(vset.recover(&manifest).ok());

	VersionEdit vbase;
	uint64_t fnum = 1;
	for (int i = 0; i < numbasefiles; i++) {
		InternalKey start(makeKey(2 * fnum), 1, kTypeValue);
		InternalKey limit(makeKey(2 * fnum + 1), 1, kTypeDeletion);
		vbase.addFile(2, fnum++, 1 /* file size */, start, limit);
	}

	assert(vset.logAndApply(&vbase).ok());
	uint64_t startMicros = env->nowMicros();

	for (int i = 0; i < iters; i++) {
		VersionEdit vedit;
		vedit.deleteFile(2, fnum);
		InternalKey start(makeKey(2 * fnum), 1, kTypeValue);
		InternalKey limit(makeKey(2 * fnum + 1), 1, kTypeDeletion);
		vedit.addFile(2, fnum++, 1 /* file size */, start, limit);
		vset.logAndApply(&vedit);
	}

	uint64_t stopMicros = env->nowMicros();
	unsigned int us = stopMicros - startMicros;
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", numbasefiles);
	fprintf(stderr,
		"BM_LogAndApply/%-6s   %8d iters : %9u us (%7.0f us / iter)\n",
		buf, iters, us, ((float)us) / iters);
}

//int main(int argc, char *argv[]) {
//	DBTest test;
//	test.empty();
//	test.emptyKey();
//	test.emptyValue();
//	test.readWrite();
//	
//	bmLogAndApply(1000, 1);
//	bmLogAndApply(1000, 100);
//	bmLogAndApply(1000, 10000);
//	bmLogAndApply(100, 100000);
//	return 0;
//}

