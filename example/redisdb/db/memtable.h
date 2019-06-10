#pragma once

#include <string>
#include <map>
#include <set>
#include <assert.h>
#include "status.h"
#include "dbformat.h"
#include "iterator.h"
#include "skiplist.h"

class MemTable {
public:
	MemTable(const InternalKeyComparator& comparator);

	~MemTable();

	size_t GetMemoryUsage() { return memoryusage; }

	// Return an iterator that yields the Contents of the memtable.
	//
	// The caller must ensure that the underlying MemTable remains live
	// while the returned iterator is live.  The keys returned by this
	// iterator are internal keys encoded by AppendInternalKey in the
	// db/format.{h,cc} module.
	std::shared_ptr<Iterator> NewIterator();

	// Add an entry into memtable that maps key to value at the
	// specified sequence number and with the specified type.
	// Typically value will be empty if type==kTypeDeletion.
	void Add(uint64_t seq, ValueType type, const std::string_view& key,
		const std::string_view& value);

	bool Get(const LookupKey& key, std::string* value, Status* s);

	void ClearTable();

private:
	struct KeyComparator {
		const InternalKeyComparator icmp;

		KeyComparator(const InternalKeyComparator& c)
			: icmp(c) {
		}

		int operator()(const char* a, const char* b) const;
	};

	friend class MemTableIterator;
	typedef SkipList<const char*, KeyComparator> Table;
	Table table;
	KeyComparator kcmp;
	size_t memoryusage;
public:
	Table& GetTable() { return table; }
};
