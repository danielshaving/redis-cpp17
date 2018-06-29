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
					//mem->put(key,value);
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
					//mem->erase(key);
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

	if (found != count(this)) 
	{
		return Status::corruption("WriteBatch has wrong count");
	} 
	else 
	{
		return Status::oK();
	}
}

int WriteBatch::count(const WriteBatch *b) 
{
  	return decodeFixed32(b->rep.data() + 8);
}

void WriteBatch::setCount(WriteBatch *b,int n) 
{
  	encodeFixed32(&b->rep[8],n);
}

uint64_t WriteBatch::getSequence(const WriteBatch *b) 
{
	return uint64_t(decodeFixed64(b->rep.data()));
}

void WriteBatch::setSequence(WriteBatch *b,uint64_t seq) 
{
	encodeFixed64(&b->rep[0],seq);
}

void WriteBatch::put(const std::string_view &key,const std::string_view &value) 
{
	setCount(this,count(this) + 1);
	rep.push_back(static_cast<char>(kTypeValue));
	putLengthPrefixedSlice(&rep,key);
	putLengthPrefixedSlice(&rep,value);
}

void WriteBatch::erase(const std::string_view &key) 
{
	setCount(this,count(this) + 1);
	rep.push_back(static_cast<char>(kTypeDeletion));
	putLengthPrefixedSlice(&rep,key);
}

void WriteBatch::setContents(WriteBatch *b,const std::string_view &contents) 
{
	assert(contents.size() >= kHeader);
	b->rep.assign(contents.data(),contents.size());
}

void WriteBatch::append(WriteBatch *dst,const WriteBatch *src) 
{
	setCount(dst,count(dst) + count(src));
	assert(src->rep.size() >= kHeader);
	dst->rep.append(src->rep.data() + kHeader, src->rep.size() - kHeader);
}


