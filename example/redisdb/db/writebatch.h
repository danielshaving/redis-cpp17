#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include "status.h"
#include "memtable.h"

class WriteBatchInternal;

class WriteBatch {
public:
	WriteBatch();

	// Intentionally copyable.
	WriteBatch(const WriteBatch&) = default;

	WriteBatch& operator=(const WriteBatch&) = default;

	~WriteBatch();

	// Store the mapping "key->value" in the database.
	void Put(const std::string_view& key, const std::string_view& value);

	// If the database Contains a mapping for "key", erase it.  Else do nothing.
	void Delete(const std::string_view& key);

	// Clear all updates buffered in this batch.
	void clear();

	Status Iterate(uint64_t sequence, const std::shared_ptr<MemTable>& mem) const;

	// The size of the database changes caused by this batch.
	//
	// This number is tied to implementation details, and may change across
	// releases. It is intended for LevelDB usage metrics.
	size_t ApproximateSize();

	std::string rep;
};

class WriteBatchInternal {
public:
	// Return the number of entries in the batch.
	static int Count(const WriteBatch* b);

	// Set the Count for the number of entries in the batch.
	static void SetCount(WriteBatch* b, int n);

	// Return the sequence number for the start of this batch.
	static uint64_t GetSequence(const WriteBatch* b);

	// Store the specified number as the sequence number for the start of
	// this batch.
	static void SetSequence(WriteBatch* b, uint64_t seq);

	static std::string_view Contents(WriteBatch* b) { return std::string_view(b->rep); }

	static size_t ByteSize(WriteBatch* b) { return b->rep.size(); }

	static void SetContents(WriteBatch* b, const std::string_view& Contents);

	static void append(WriteBatch* dst, const WriteBatch* src);

	static Status InsertInto(const WriteBatch* batch, const std::shared_ptr<MemTable>& memtable);
};
