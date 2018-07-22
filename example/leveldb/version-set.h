#pragma once
#include <map>
#include <set>
#include <vector>
#include <list>
#include "option.h"
#include "dbformat.h"
#include "version-edit.h"

class Version;
class VersionSet;
class Builder
{
	Builder(VersionSet *vset,Version *base)
	{

	}
};

class Version
{
public:
	Version(VersionSet *vset)
	{

	}
};

class VersionSet
{
public:
	VersionSet(const std::string &dbname,const Options &options);
	~VersionSet();

	uint64_t getLastSequence() const { return lastSequence; }
	void setLastSequence(uint64_t s)
	{
		assert(s >= lastSequence);
		lastSequence = s;
	}

	uint64_t newFileNumber() { return nextFileNumber++; }
	Status recover(bool *manifest);
	// Apply all of the edits in *edit to the current state.
	void apply(VersionEdit* edit);

	void markFileNumberUsed(uint64_t number);

private:
	const std::string dbname;
	const Options options;
	uint64_t lastSequence;
	uint64_t nextFileNumber;
	uint64_t manifestFileNumber;
	uint64_t logNumber;
	uint64_t prevLogNumber;  // 0 or backing store for memtable being compacted
	std::list<std::shared_ptr<Version>> versions;
};
