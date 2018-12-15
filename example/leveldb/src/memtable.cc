#include "memtable.h"
#include "coding.h"

MemTable::MemTable(const InternalKeyComparator &comparator)
	:memoryUsage(0),
	refs(0),
	kcmp(comparator),
	table(comparator)
{

}

MemTable::~MemTable()
{
	clearTable();
}

bool MemTable::get(const LookupKey &key, std::string *value, Status *s)
{
	std::string_view memkey = key.memtableKey();
	auto it = table.lower_bound(memkey.data());
	if (it != table.end())
	{
		// entry format is:
		// klength  varint32
		// userkey  char[klength]
		// tag      uint64
		// vlength  varint32
		// value    char[vlength]
		// Check that it belongs to same user key.  We do not check the
		// sequence number since the Seek() call above should have skipped
		// all entries with overly large sequence numbers.

		const char *entry = *it;
		uint32_t keyLength;
		const char *keyPtr = getVarint32Ptr(entry, entry + 5, &keyLength);

		if (kcmp.icmp.getComparator()->compare(std::string_view(keyPtr, keyLength - 8),
			key.userKey()) == 0)
		{
			const uint64_t tag = decodeFixed64(keyPtr + keyLength - 8);
			switch (static_cast<ValueType>(tag & 0xff))
			{
				case kTypeValue:
				{
					std::string_view v = getLengthPrefixedSlice(keyPtr + keyLength);
					value->assign(v.data(), v.size());
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

void MemTable::add(uint64_t seq, ValueType type, const std::string_view &key,
	const std::string_view &value)
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

	char *buf = (char*)malloc(encodedLen);
	char *p = encodeVarint32(buf, internalKeySize);
	memcpy(p, key.data(), keySize);
	p += keySize;
	encodeFixed64(p, (seq << 8) | type);
	p += 8;
	p = encodeVarint32(p, valSize);
	memcpy(p, value.data(), valSize);
	assert(p + valSize == buf + encodedLen);
	table.insert(buf);
	memoryUsage += encodedLen;
}


bool MemTable::KeyComparator::operator()(const char *aptr, const char *bptr) const
{
	// Internal keys are encoded as length-prefixed strings.
	std::string_view a = getLengthPrefixedSlice(aptr);
	std::string_view b = getLengthPrefixedSlice(bptr);
	return icmp.comdpare(a, b) < 0;
}

void MemTable::clearTable()
{
	for (auto &iter : table)
	{
		free((void*)iter);
	}

	memoryUsage = 0;
	table.clear();
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char *encodeKey(std::string *scratch, const std::string_view &target) 
{
	scratch->clear();
	putVarint32(scratch, target.size());
	scratch->append(target.data(), target.size());
	return scratch->data();
}

class MemTableIterator : public Iterator 
{
public:
	explicit MemTableIterator(const MemTable::Table &table) 
	:table(table), iter(table.end()), empty(false)
	{
		
	}

	virtual bool valid() const
	{
		if(!empty) return false;
		return iter != table.end();
	}
	
	virtual void seek(const std::string_view &k) 
	{
		iter = table.lower_bound(encodeKey(&tmp, k));
		if (iter == table.end())
		{
			empty = false;
		}
		else
		{
			empty = true;
		}
	}
	
	virtual void seekToFirst() 
	{ 
		iter = table.begin(); 
		if (iter == table.end())
		{
			empty = false;
		}
		else
		{
			empty = true;
		}
	}
	
	virtual void seekToLast()
	{
		iter = table.empty() ? table.end() : std::prev(table.end());
		if (iter == table.end())
		{
			empty = false;
		}
		else
		{
			empty = true;
		}
	}
	
	virtual void next()
	{
		assert (iter != table.end());
		++iter;

		if (iter == table.end())
		{
			empty = false;
		}
	}
	
	virtual void prev() 
	{
		assert(iter != table.end());
		if (iter == table.begin()) 
		{
			iter = table.end();
		}
		else 
		{
			--iter;
		}
		
		if (iter == table.end())
		{
			empty = false;
		}
	}
	
	virtual std::string_view key() const 
	{
		assert (iter != table.end());
		return getLengthPrefixedSlice(*iter); 
	}
	
	virtual std::string_view value() const 
	{
		assert (iter != table.end());
		std::string_view view = getLengthPrefixedSlice(*iter);
		return getLengthPrefixedSlice(view.data() + view.size());
	}
	
	virtual Status status() const { return Status::OK(); }
  	
	virtual void registerCleanup(const std::any &arg) { }
private:
	MemTable::Table table;
	MemTable::Table::iterator iter;
	std::string tmp;
	bool empty;
	 // No copying allowed
	MemTableIterator(const MemTableIterator&);
	void operator=(const MemTableIterator&);
};

std::shared_ptr<Iterator> MemTable::newIterator()
{
	std::shared_ptr<Iterator> it(new MemTableIterator(table));
	return it;
}


