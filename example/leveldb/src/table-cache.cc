#include "table-cache.h"
#include "filename.h"
#include "coding.h"
#include "table.h"

struct TableAndFile 
{
	std::shared_ptr<PosixMmapReadableFile> file;
	std::shared_ptr<Table> table;
};

static void deleteEntry(const std::string_view &key, std::any  &value) 
{
	
}

TableCache::TableCache(const std::string &dbname, const Options &options, int entries)
:options(options),
 dbname(dbname),
 cache(new ShardedLRUCache(entries))
{

}

TableCache::~TableCache()
{

}

Status TableCache::findTable(uint64_t fileNumber, uint64_t fileSize,
		  std::shared_ptr<LRUHandle> &handle)
{
	Status s;
	char buf[sizeof(fileNumber)];
	encodeFixed64(buf, fileNumber);
	std::string_view key(buf, sizeof(buf));
	handle = cache->lookup(key);
	if (handle == nullptr)
	{
		std::string fname = tableFileName(dbname, fileNumber);
		std::shared_ptr<PosixMmapReadableFile> file = nullptr;
		std::shared_ptr<Table> table = nullptr;
		s = options.env->newRandomAccessFile(fname, file);
		if (s.ok()) 
		{
			s = Table::open(options, file.get(), fileSize, table);
		}

		if (!s.ok()) 
		{
			assert(table == nullptr);
			// We do not cache error results so that if the error is transient,
			// or somebody repairs the file, we recover automatically.
		}
		else 
		{
			std::shared_ptr<TableAndFile> tf(new TableAndFile);
			tf->file = file;
			tf->table = table;
			handle = cache->insert(key, tf, 1, nullptr);
		}
	}
	return s;
}

Status TableCache::get(const ReadOptions &options,
		 uint64_t fileNumber,
		 uint64_t fileSize,
		 const std::string_view &k,
		 const std::any &arg,
		 std::function<void(const std::string_view &k, const std::any &value)> &handleResult)
{
	std::shared_ptr<LRUHandle> handle;
	Status s = findTable(fileNumber, fileSize, handle);
	if (s.ok())
	{

	}
	return s;
}
