#pragma once
#include <map>
#include <set>
#include <vector>
#include "option.h"
#include "dbformat.h"

class Version
{

};

class VersionSet
{
public:
	VersionSet(const std::string &dbname,const Options *options);
	~VersionSet();

	uint64_t getLastSequence() const { return lastSequence; }
	void setLastSequence(uint64_t s)
	{
		assert(s >= lastSequence);
		lastSequence = s;
	}

	uint64_t newFileNumber() { return nextFileNumber++; }
	Status recover(bool manifest);

private:
	uint64_t lastSequence;
	uint64_t nextFileNumber;
	uint64_t manifestFileNumber;
	uint64_t logNumber;
	uint64_t prevLogNumber;  // 0 or backing store for memtable being compacted
};
