#include "writebatch.h"
#include "coding.h"


// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() 
{
	clear();
}

WriteBatch::~WriteBatch() { }

void WriteBatch::clear() 
{
	rep.clear();
	rep.resize(kHeader);
}

void WriteBatch::del(const std::string_view &key)
{
	WriteBatchInternal::setCount(this,WriteBatchInternal::count(this) + 1);
	rep.push_back(static_cast<char>(kTypeDeletion));
	putLengthPrefixedSlice(&rep,key);
}

void WriteBatch::put(const std::string_view &key,const std::string_view &value)
{
	WriteBatchInternal::setCount(this,WriteBatchInternal::count(this) + 1);
	rep.push_back(static_cast<char>(kTypeValue));
	putLengthPrefixedSlice(&rep,key);
	putLengthPrefixedSlice(&rep,value);
}

size_t WriteBatch::approximateSize() 
{
	return rep.size();
}

Status WriteBatch::iterate(uint64_t sequence,const std::shared_ptr<MemTable> &mem) const 
{
	std::string_view input(rep);
	if (input.size() < kHeader) 
	{
		return Status::corruption("malformed WriteBatch (too small)");
	}

	input.remove_prefix(kHeader);
	std::string_view key,value;
	int found = 0;
	while (!input.empty()) 
	{
		found++;
		char tag = input[0];
		input.remove_prefix(1);
		switch (tag) 
		{
			case kTypeValue:
			{
				if (getLengthPrefixedSlice(&input,&key) && getLengthPrefixedSlice(&input,&value)) 
				{
					mem->add(sequence++,kTypeValue,key,value);
				} 
				else 
				{
				  	return Status::corruption("bad WriteBatch Put");
				}
				break;
			}
			case kTypeDeletion:
			{
				if (getLengthPrefixedSlice(&input,&key)) 
				{
					mem->add(sequence++,kTypeDeletion,key,std::string_view());
				} 
				else 
				{
				  	return Status::corruption("bad WriteBatch Delete");
				}
				break;
			}
			default:
			return Status::corruption("unknown WriteBatch tag");
		}
	}

	if (found != WriteBatchInternal::count(this))
	{
		return Status::corruption("WriteBatch has wrong count");
	} 
	else 
	{
		Status s;
		return s;
	}
}

Status WriteBatchInternal::insertInto(const WriteBatch *batch,const std::shared_ptr<MemTable> &memtable)
{
	return batch->iterate(getSequence(batch),memtable);
}

int WriteBatchInternal::count(const WriteBatch *b)
{
  	return decodeFixed32(b->rep.data() + 8);
}

void WriteBatchInternal::setCount(WriteBatch *b,int n)
{
  	encodeFixed32(&b->rep[8],n);
}

uint64_t WriteBatchInternal::getSequence(const WriteBatch *b)
{
	return uint64_t(decodeFixed64(b->rep.data()));
}

void WriteBatchInternal::setSequence(WriteBatch *b,uint64_t seq)
{
	encodeFixed64(&b->rep[0],seq);
}

void WriteBatchInternal::setContents(WriteBatch *b,const std::string_view &contents)
{
	assert(contents.size() >= kHeader);
	b->rep.assign(contents.data(),contents.size());
}

void WriteBatchInternal::append(WriteBatch *dst,const WriteBatch *src)
{
	setCount(dst,count(dst) + count(src));
	assert(src->rep.size() >= kHeader);
	dst->rep.append(src->rep.data() + kHeader, src->rep.size() - kHeader);
}


