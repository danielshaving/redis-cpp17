#pragma once
#include "all.h"
#include "xZmalloc.h"
#include "xBuffer.h"
#include "xUtil.h"
#include "xCallback.h"

class  xItem : noncopyable
{
public:
	enum UpdatePolicy
	{
		kInvalid,
		kSet,
		kAdd,
		kReplace,
		kAppend,
		kPrepend,
		kCas,
	};

	static ItemPtr makeItem(xStringPiece keyArg, uint32_t flagsArg,int exptimeArg,int valuelen,uint64_t casArg)
	{
		return std::make_shared<xItem>(keyArg, flagsArg, exptimeArg, valuelen, casArg);
	}

	xItem(xStringPiece keyArg,
		 uint32_t flagsArg,
		 int exptimeArg,
		 int valuelen,
		 uint64_t casArg);

	~xItem()
	{
		zfree(data);
	}

	int getRelExptime() const  {  return relExptime; }
	uint32_t getFlags() const {  return flags; }
	const char* value() const {  return data +keyLen; }
	size_t valueLength() const {  return valueLen; }
	uint64_t getCas() const{ return cas; }
	void setCas(uint64_t casArg) { cas = casArg;}
	size_t neededBytes() const;
	size_t getHash() const { return hash; }
	xStringPiece getKey()const { return xStringPiece(data,keyLen); }
	void append(const char *data, size_t len);
	void output(xBuffer *out, bool needCas = false) const;
	void resetKey(xStringPiece k);

	bool endsWithCRLF() const
	{
		return receivedBytes == totalLen()&& data[totalLen()-2] == '\r'&& data[totalLen()-1] == '\n';
	}

private:
	int totalLen() const { return keyLen + valueLen; }
	int keyLen;
	const uint32_t flags;
	const int relExptime;
	const int valueLen;
	int receivedBytes;
	uint64_t cas;
	size_t hash;
	char *data;
};



