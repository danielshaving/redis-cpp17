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
public:
    std::string dbname;
    DBImpl db;
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

int main() {
    bmLogAndApply(1000, 1);
    bmLogAndApply(1000, 100);
    bmLogAndApply(1000, 10000);
    bmLogAndApply(100, 100000);
    return 0;
}
