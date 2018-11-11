#pragma once
#include <string>
#include <string_view>
#include <memory>
#include "status.h"
#include "memtable.h"

class WriteBatchInternal;

class WriteBatch
{
public:
	WriteBatch();

	// Intentionally copyable.
	WriteBatch(const WriteBatch&) = default;
	WriteBatch& operator =(const WriteBatch&) = default;

	~WriteBatch();

	// Store the mapping "key->value" in the database.
	void put(const std::string_view &key, const std::string_view &value);

	// If the database contains a mapping for "key", erase it.  Else do nothing.
	void del(const std::string_view &key);

	// Clear all updates buffered in this batch.
	void clear();

	Status iterate(uint64_t sequence, const std::shared_ptr<MemTable> &mem) const;

	// The size of the database changes caused by this batch.
	//
	// This number is tied to implementation details, and may change across
	// releases. It is intended for LevelDB usage metrics.
	size_t approximateSize();

	std::string rep;
};

class WriteBatchInternal
{
public:
	// Return the number of entries in the batch.
	static int count(const WriteBatch *b);

	// Set the count for the number of entries in the batch.
	static void setCount(WriteBatch *b, int n);

	// Return the sequence number for the start of this batch.
	static uint64_t getSequence(const WriteBatch* b);

	// Store the specified number as the sequence number for the start of
	// this batch.
	static void setSequence(WriteBatch *b, uint64_t seq);

	static std::string_view contents(WriteBatch *b) { return std::string_view(b->rep); }

	static size_t byteSize(WriteBatch *b) { return b->rep.size(); }

	static void setContents(WriteBatch *b, const std::string_view &contents);

	static void append(WriteBatch *dst, const WriteBatch *src);

	static Status insertInto(const WriteBatch *batch, const std::shared_ptr<MemTable> &memtable);
};
