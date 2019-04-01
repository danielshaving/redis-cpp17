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

    DBTest() : optionConfig(kDefault) {
		dbname = "/db_test";
		reopen();
    }

    ~DBTest() {
		db->destroyDB(dbname, Options());
    }

    void reopen(Options *options = nullptr) {
		assert(tryReopen(options).ok());
    }

    void destroyAndReopen(Options *options = nullptr) {
        db->destroyDB(dbname, Options());
        assert(tryReopen(options).ok());
      }

	Status tryReopen(Options *options) {
		Options opts;
		if (options != nullptr) {
		  opts = *options;
		} else {
		  opts = currentOptions();
		  opts.createIfMissing = true;
		}

		lastOptions = opts;
		db.reset(new DBImpl(opts, dbname));
		db->destroyDB(dbname, Options());
		return db->open();
	}

	// Switch to a fresh database with the next option configuration to
	// test.  Return false if there are no more configurations to test.
	bool ChangeOptions() {
		optionConfig++;
		if (optionConfig >= kEnd) {
		  return false;
		} else {
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
			options.reuseLogs = true;
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
		} else if (!s.ok()) {
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
		} else {
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

};

void bmLogAndApply(int iters, int numbasefiles) {
    Options opts;
    opts.createIfMissing = true;
    std::string dbname = "./leveldb_test_benchmark";
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
    uint64_t startMicros = options.env->nowMicros();

    for (int i = 0; i < iters; i++) {
        VersionEdit vedit;
        vedit.deleteFile(2, fnum);
        InternalKey start(makeKey(2 * fnum), 1, kTypeValue);
        InternalKey limit(makeKey(2 * fnum + 1), 1, kTypeDeletion);
        vedit.addFile(2, fnum++, 1 /* file size */, start, limit);
        vset.logAndApply(&vedit);
    }

    uint64_t stopMicros = options.env->nowMicros();
    unsigned int us = stopMicros - startMicros;
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", numbasefiles);
    fprintf(stderr,
            "BM_LogAndApply/%-6s   %8d iters : %9u us (%7.0f us / iter)\n",
            buf, iters, us, ((float) us) / iters);
}

/*
int main() {
    bmLogAndApply(1000, 1);
    bmLogAndApply(1000, 100);
    bmLogAndApply(1000, 10000);
    bmLogAndApply(100, 100000);
    return 0;
}
*/
