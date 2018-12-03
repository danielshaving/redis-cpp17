#pragma once
#include <string>
#include <map>
#include <set>
#include <assert.h>
#include "status.h"
#include "dbformat.h"
#include "iterator.h"

class MemTable
{
public:
	MemTable(const InternalKeyComparator &comparator);
	~MemTable();

	size_t getMemoryUsage() { return memoryUsage; }

	// Return an iterator that yields the contents of the memtable.
	//
	// The caller must ensure that the underlying MemTable remains live
	// while the returned iterator is live.  The keys returned by this
	// iterator are internal keys encoded by AppendInternalKey in the
	// db/format.{h,cc} module.
	std::shared_ptr<Iterator> newIterator();
	  
	// Add an entry into memtable that maps key to value at the
	// specified sequence number and with the specified type.
	// Typically value will be empty if type==kTypeDeletion.
	void add(uint64_t seq, ValueType type, const std::string_view &key,
		const std::string_view &value);
	bool get(const LookupKey &key, std::string *value, Status *s);
	size_t getTableSize() { return table.size(); }
	void clearTable();

private:
	struct KeyComparator
	{
	    const InternalKeyComparator comparator;
	    KeyComparator(const InternalKeyComparator &c) : comparator(c) { }
		int operator()(const char *a, const char *b) const;
	};
	
	friend class MemTableIterator;
	typedef std::set<const char *, KeyComparator> Table;
	KeyComparator kcmp;
	Table table;
	size_t memoryUsage;
	int refs;
public:
	Table &getTable() { return table; }
};
