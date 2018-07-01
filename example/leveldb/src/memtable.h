#pragma once
#include <string>
#include <map>
#include <set>
#include <assert.h>

#include "dbformat.h"

class MemTable
{
public:
	MemTable();
	~MemTable();

	size_t getMemoryUsage() { return memoryUsage; }
private:
	struct KeyComparator
	{
		const InternalKeyComparator comparator;
		int operator()(const char *a,const char *b) const;
	};

	typedef std::set<const char *,KeyComparator> Table;
	Table table;
	size_t memoryUsage;
	int refs;
};