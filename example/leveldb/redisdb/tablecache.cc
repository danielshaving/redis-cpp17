#include "tablecache.h"
#include "filename.h"
#include "coding.h"
#include "table.h"

struct TableAndFile {
    std::shared_ptr <RandomAccessFile> file;
    std::shared_ptr <Table> table;
};

static void deleteEntry(const std::string_view &key, std::any &value) {

}

TableCache::TableCache(const std::string &dbname, const Options &options, int entries)
        : options(options),
          dbname(dbname),
          cache(new ShardedLRUCache(entries)),
		  env(new Env()) {

}

TableCache::~TableCache() {

}

void TableCache::evict(uint64_t fileNumber) {
	char buf[sizeof(fileNumber)];
	encodeFixed64(buf, fileNumber);
	cache->erase(std::string_view(buf, sizeof(buf)));
}

Status TableCache::findTable(uint64_t fileNumber, uint64_t fileSize,
                             std::shared_ptr <LRUHandle> &handle) {
    Status s;
    char buf[sizeof(fileNumber)];
    encodeFixed64(buf, fileNumber);
    std::string_view key(buf, sizeof(buf));
    handle = cache->lookup(key);
    if (handle == nullptr) {
        std::string fname = tableFileName(dbname, fileNumber);
        std::shared_ptr <RandomAccessFile> file = nullptr;
        std::shared_ptr <Table> table = nullptr;
        s = env->newRandomAccessFile(fname, file);
        if (s.ok()) {
            s = Table::open(options, file, fileSize, table);
        }

        if (!s.ok()) {
            assert(table == nullptr);
            // We do not cache error results so that if the error is transient,
            // or somebody repairs the file, we recover automatically.
        } else {
            std::shared_ptr <TableAndFile> tf(new TableAndFile);
            tf->file = file;
            tf->table = table;
            handle = cache->insert(key, tf, 1, nullptr);
            printf("Table cache is open %s\n", fname.c_str());
        }
    }
    return s;
}

std::shared_ptr <Iterator> TableCache::newIterator(const ReadOptions &options,
                                                   uint64_t fileNumber,
                                                   uint64_t fileSize,
                                                   std::shared_ptr <Table> tableptr) {
    std::shared_ptr <LRUHandle> handle;
    Status s = findTable(fileNumber, fileSize, handle);
    if (!s.ok()) {
        return newErrorIterator(s);
    }

    std::shared_ptr <Table> table = std::any_cast <std::shared_ptr<TableAndFile>>(handle->value)->table;
    std::shared_ptr <Iterator> result = table->newIterator(options);
    result->registerCleanup(handle);
    tableptr = table;
    return result;
}

std::shared_ptr <Iterator> TableCache::getFileIterator(const ReadOptions &options, const std::string_view &fileValue) {
	if (fileValue.size() != 16) {
		return newErrorIterator(
				Status::corruption("FileReader invoked with unexpected value"));
	} else {
		return newIterator(options,
								  decodeFixed64(fileValue.data()),
								  decodeFixed64(fileValue.data() + 8));
	}
}

Status TableCache::get(const ReadOptions &options,
                       uint64_t fileNumber,
                       uint64_t fileSize,
                       const std::string_view &k,
                       const std::any &arg,
                       std::function<void(const std::any &,
                                          const std::string_view &, const std::string_view &)> &&callback) {
    std::shared_ptr <LRUHandle> handle;
    Status s = findTable(fileNumber, fileSize, handle);
    if (s.ok()) {
        //printf("Table cache get file number :%d bytes %lld\n", fileNumber, fileSize);
        std::shared_ptr <Table> table = std::any_cast <std::shared_ptr<TableAndFile>>(handle->value)->table;
        s = table->internalGet(options, k, arg, callback);
    }

    return s;
}
