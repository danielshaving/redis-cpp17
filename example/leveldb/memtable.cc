#include "memtable.h"
#include "coding.h"
#include "zmalloc.h"

static std::string_view getLengthPrefixedSlice(const char *data)
{
	uint32_t len;
	const char *p = data;
	p = getVarint32Ptr(p,p + 5,&len);  // +5: we assume "p" is not corrupted
	return std::string_view(p, len);
}

MemTable::MemTable()
:memoryUsage(0),
 refs(0)
{

}

MemTable::~MemTable()
{
	for (auto &it : table)
	{
		zfree((void*)it);
	}
}

bool MemTable::get(const LookupKey &key,std::string *value,Status *s)
{
	std::string_view memkey = key.memtableKey();
	auto it = table.find(memkey.data());
	if (it != table.end())
	{
		// entry format is:
		//    klength  varint32
		//    userkey  char[klength]
		//    tag      uint64
		//    vlength  varint32
		//    value    char[vlength]
		// Check that it belongs to same user key.  We do not check the
		// sequence number since the Seek() call above should have skipped
		// all entries with overly large sequence numbers.

		const char *entry = *it;
		uint32_t keyLength;
		const char *keyPtr = getVarint32Ptr(entry,entry+5,&keyLength);

		if (compar.comparator.getComparator()->compare(std::string_view(keyPtr,keyLength - 8),
	            key.userKey()) == 0)
		{
			const uint64_t tag = decodeFixed64(keyPtr + keyLength - 8);
			switch (static_cast<ValueType>(tag & 0xff))
			{
				case kTypeValue:
				{
					std::string_view v = getLengthPrefixedSlice(keyPtr + keyLength);
					value->assign(v.data(),v.size());
					return true;
				}

				case kTypeDeletion:
					*s = Status::notFound(std::string_view());
				return true;
			}
		}
	}
	return false;
}

void MemTable::add(uint64_t seq,ValueType type,const std::string_view &key,const std::string_view &value)
{
	// Format of an entry is concatenation of:
	//  key_size     : varint32 of internal_key.size()
	//  key bytes    : char[internal_key.size()]
	//  value_size   : varint32 of value.size()
	//  value bytes  : char[value.size()]
	size_t keySize = key.size();
	size_t valSize = value.size();
	size_t internalKeySize = keySize + 8;

	const size_t encodedLen =
	  varintLength(internalKeySize) + internalKeySize +
	  varintLength(valSize) + valSize;

	char *buf = (char*)zmalloc(encodedLen);
	char *p = encodeVarint32(buf,internalKeySize);
	memcpy(p,key.data(),keySize);
	p += keySize;
	encodeFixed64(p,(seq << 8) | type);
	p += 8;
	p = encodeVarint32(p,valSize);
	memcpy(p,value.data(),valSize);
	assert(p + valSize == buf + encodedLen);
	table.insert(buf);
	memoryUsage += encodedLen;
}

int MemTable::KeyComparator::operator()(const char *aptr,const char *bptr)const 
{
	// Internal keys are encoded as length-prefixed strings.
	std::string_view a = getLengthPrefixedSlice(aptr);
	std::string_view b = getLengthPrefixedSlice(bptr);
	return comparator.compare(a,b);
}

