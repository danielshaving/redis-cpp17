#include "memtable.h"
#include "coding.h"

MemTable::MemTable()
:refs(0)
{

}

MemTable::~MemTable()
{

}

static std::string_view getLengthPrefixedSlice(const char *data) 
{
	uint32_t len;
	const char *p = data;
	p = getVarint32Ptr(p,p + 5,&len);  // +5: we assume "p" is not corrupted
	return std::string_view(p,len);
}

int MemTable::KeyComparator::operator()(const char *aptr,const char *bptr)const 
{
	// Internal keys are encoded as length-prefixed strings.
	std::string_view a = getLengthPrefixedSlice(aptr);
	std::string_view b = getLengthPrefixedSlice(bptr);
	return comparator.compare(a,b);
}

