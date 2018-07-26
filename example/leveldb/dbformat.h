#pragma once
#include <string_view>
#include <stdio.h>
#include <string>
#include <assert.h>
#include "zmalloc.h"

static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum ValueType 
{
	kTypeDeletion = 0x0,
	kTypeValue = 0x1
};

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const uint64_t kMaxSequenceNumber = ((0x1ull << 56) - 1);

struct ParsedInternalKey 
{
	std::string_view userKey;
	uint64_t sequence;
	ValueType type;

	ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
	ParsedInternalKey(const std::string_view &u,const uint64_t &seq,ValueType t)
	  :userKey(u),
	  sequence(seq), 
	  type(t) { }

	std::string debugString() const;
};

// Return the length of the encoding of "key".
inline size_t internalKeyEncodingLength(const ParsedInternalKey &key) { return key.userKey.size() + 8; }

// Append the serialization of "key" to *result.
void appendInternalKey(std::string *result,const ParsedInternalKey &key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
bool parseInternalKey(const std::string_view &internalKey,ParsedInternalKey *result);

// Returns the user key portion of an internal key.
inline std::string_view extractUserKey(const std::string_view &internalKey) 
{
	assert(internalKey.size() >= 8);
	return std::string_view(internalKey.data(),internalKey.size() - 8);
}

class BytewiseComparatorImpl
{
public:
	BytewiseComparatorImpl() { }
	const char *name() const { return "leveldb.BytewiseComparator"; }
	int compare(const std::string_view &a,const std::string_view &b) const { return a.compare(b); }
	void findShortestSeparator(std::string *start,const std::string_view &limit) const;
	void findShortSuccessor(std::string *key) const;
};
// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.

class InternalKeyComparator
{
public:
	InternalKeyComparator() { }
	~InternalKeyComparator() { }

	const char *name() const;
	void findShortestSeparator(std::string *start,const std::string_view &limit) const;
	void findShortSuccessor(std::string *key) const;
	int compare(const std::string_view &a,const std::string_view &b) const;
	int compare(const InternalKey &a,const InternalKey &b) const;
	const BytewiseComparatorImpl *getComparator() const { return &userComparator; }

private:
	BytewiseComparatorImpl userComparator;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.

class InternalKey 
{
private:
	std::string rep;
public:
	InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
	InternalKey(const std::string_view &userKey,uint64_t s,ValueType t)
	{
		appendInternalKey(&rep,ParsedInternalKey(userKey,s,t));
	}

	void decodeFrom(const std::string_view &s) { rep.assign(s.data(),s.size()); }
	std::string_view encode() const
	{
		assert(!rep.empty());
		return rep;
	}

	std::string_view userKey() const { extractUserKey(rep); }

	void setFrom(const ParsedInternalKey &p)
	{
		rep.clear();
		appendInternalKey(&rep,p);
	}

	std::string debugString() const;
	void clear() { rep.clear(); }  
};

inline int InternalKeyComparator::compare(const InternalKey &a,const InternalKey &b) const 
{
	return compare(a.encode(),b.encode());
}

// A helper class useful for DBImpl::Get()
class LookupKey
{
public:
	// Initialize *this for looking up user_key at a snapshot with
	// the specified sequence number.
	LookupKey(const std::string_view &userKey,uint64_t sequence);

	~LookupKey();

	// Return a key suitable for lookup in a MemTable.
	std::string_view memtableKey() const { return std::string_view(start,end - start); }

	// Return an internal key (suitable for passing to an internal iterator)
	std::string_view internalKey() const { return std::string_view(kstart,end - kstart); }

	// Return the user key
	std::string_view userKey() const { return std::string_view(kstart,end - kstart - 8); }

private:
	// We construct a char array of the form:
	//    klength  varint32               <-- start_
	//    userkey  char[klength]          <-- kstart_
	//    tag      uint64
	//                                    <-- end_
	// The array is a suitable MemTable key.
	// The suffix starting with "userkey" can be used as an InternalKey.
	const char *start;
	const char *kstart;
	const char *end;
	char space[200];      // Avoid allocation for short keys

	// No copying allowed
	LookupKey(const LookupKey&);
	void operator=(const LookupKey&);
};

inline LookupKey::~LookupKey()
{
	if (start != space) { zfree((void*)start); }
}
