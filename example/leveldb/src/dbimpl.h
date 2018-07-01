#pragma once

#include <deque>
#include <set>
#include <memory>
#include "option.h"
#include "status.h"
#include "writebatch.h"

class DBImpl
{
public:
	DBImpl(const Options &options,const std::string &dbname);
	~DBImpl();
	// Implementations of the DB interface
	Status put(const WriteOptions&,const std::string_view &key,const std::string_view &value);
	Status erase(const WriteOptions&,const std::string_view &key);
	Status write(const WriteOptions &options,WriteBatch *updates);
	Status get(const ReadOptions &options,const std::string_view &key,std::string *value);
	Status open(const Options& options,const std::string& dbname);
	Status makeRoomForWrite(bool force /* compact even if there is room? */);

private:
	struct Writer;
	  // No copying allowed
	DBImpl(const DBImpl&);
	void operator=(const DBImpl&);

	 // Queue of writers.
	std::deque<std::shared_ptr<Writer>> writers;
	std::shared_ptr<WriteBatch>	tmpBatch;
	std::shared_ptr<MemTable> mem;
	std::shared_ptr<MemTable> imm;
};