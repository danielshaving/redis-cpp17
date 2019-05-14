#include "tablecache.h"
#include "filename.h"
#include "coding.h"
#include "table.h"

struct TableAndFile {
	std::shared_ptr<RandomAccessFile> file;
	std::shared_ptr<Table> table;
};

static void deleteEntry(const std::string_view& key, std::any& value) {

}

TableCache::TableCache(const std::string& dbname, const Options& options, int entries)
	: options(options),
	dbname(dbname),
	cache(new ShardedLRUCache(entries)) {

}

TableCache::~TableCache() {

}

void TableCache::evict(uint64_t fileNumber) {
	char buf[sizeof(fileNumber)];
	EncodeFixed64(buf, fileNumber);
	cache->Erase(std::string_view(buf, sizeof(buf)));
}

Status TableCache::FindTable(uint64_t fileNumber, uint64_t FileSize,
	std::shared_ptr<LRUHandle>& handle) {
	Status s;
	char buf[sizeof(fileNumber)];
	EncodeFixed64(buf, fileNumber);
	std::string_view key(buf, sizeof(buf));
	handle = cache->Lookup(key);
	if (handle == nullptr) {
		std::string fname = TableFileName(dbname, fileNumber);
		std::shared_ptr<RandomAccessFile> file = nullptr;
		std::shared_ptr<Table> table = nullptr;
		s = options.env->NewRandomAccessFile(fname, file);
		if (s.ok()) {
			s = Table::Open(options, file, FileSize, table);
		}

		if (!s.ok()) {
			assert(table == nullptr);
			// We do not cache error results so that if the error is transient,
			// or somebody repairs the file, we Recover automatically.
		}
		else {
			std::shared_ptr<TableAndFile> tf(new TableAndFile);
			tf->file = file;
			tf->table = table;
			handle = cache->Insert(key, tf, 1, nullptr);
			printf("Table cache is Open %s\n", fname.c_str());
		}
	}
	return s;
}

static void UnrefEntry(const std::any &arg1, const std::any &arg2) {
	std::shared_ptr<ShardedLRUCache> cache = std::any_cast<std::shared_ptr<ShardedLRUCache>>(arg1);
	std::shared_ptr<LRUHandle> handle = std::any_cast<std::shared_ptr<LRUHandle>>(arg2);
	cache->Release(handle);
}

std::shared_ptr<Iterator> TableCache::NewIterator(const ReadOptions& options,
	uint64_t fileNumber,
	uint64_t FileSize,
	std::shared_ptr<Table> tableptr) {
	std::shared_ptr<LRUHandle> handle;
	Status s = FindTable(fileNumber, FileSize, handle);
	if (!s.ok()) {
		return NewErrorIterator(s);
	}

	std::shared_ptr<Table> table = std::any_cast<std::shared_ptr<TableAndFile>>(handle->value)->table;
	std::shared_ptr<Iterator> result = table->NewIterator(options);
	result->RegisterCleanup(std::bind(UnrefEntry, cache, handle));
	if (tableptr != nullptr) {
		tableptr = table;
	}
	return result;
}

std::shared_ptr<Iterator> TableCache::GetFileIterator(const ReadOptions& options, const std::string_view& fileValue) {
	if (fileValue.size() != 16) {
		return NewErrorIterator(
			Status::Corruption("FileReader invoked with unexpected value"));
	}
	else {
		return NewIterator(options,
			DecodeFixed64(fileValue.data()),
			DecodeFixed64(fileValue.data() + 8));
	}
}

Status TableCache::Get(const ReadOptions& options,
	uint64_t fileNumber,
	uint64_t FileSize,
	const std::string_view & k,
	const std::any & arg,
	std::function<void(const std::any&,
		const std::string_view&, const std::string_view&)> && callback) {
	std::shared_ptr<LRUHandle> handle;
	Status s = FindTable(fileNumber, FileSize, handle);
	if (s.ok()) {
		//printf("Table cache get file number :%d bytes %lld\n", fileNumber, FileSize);
		std::shared_ptr<Table> table = std::any_cast<std::shared_ptr<TableAndFile>>(handle->value)->table;
		s = table->InternalGet(options, k, arg, callback);
	}

	return s;
}
