#pragma once
#include <string>
#include <stdint.h>
#include "dbformat.h"
#include "cache.h"
#include "table.h"
#include "option.h"

class TableCache
{
public:
	 TableCache(const std::string &dbname, const Options &options, int entries);
	  ~TableCache();
	  // If a seek to internal key "k" in specified file finds an entry,
	  // call (*handle_result)(arg, found_key, found_value).
	  Status get(const ReadOptions &options,
	             uint64_t fileNumber,
	             uint64_t fileSize,
	             const std::string_view &k,
	             const std::any &arg,
				 std::function<void(const std::string_view &k, const std::any &value)> &handleResult);
	  Status findTable(uint64_t fileNumber, uint64_t fileSize,
			  std::shared_ptr<LRUHandle> &handle);
private:
	TableCache(const TableCache&) = delete;
	void operator=(const TableCache&) = delete;

	std::string dbname;
	const Options options;
	std::shared_ptr<ShardedLRUCache> cache;
};
