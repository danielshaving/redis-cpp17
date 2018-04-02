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

	static ItemPtr makeItem(xStringPiece keyArg, uint32_t flagsArg,int32_t exptimeArg,int32_t valuelen,uint64_t casArg)
	{
		return std::make_shared<xItem>(keyArg,flagsArg,exptimeArg,valuelen, casArg);
	}

	xItem(xStringPiece keyArg,uint32_t flagsArg,int32_t exptimeArg, int32_t valuelen,uint64_t casArg);
	~xItem()
	{
		zfree(data);
	}

	int32_t getRelExptime() const  {  return relExptime; }
	uint32_t getFlags() const {  return flags; }
	const char *value() const {  return data +keyLen; }
	size_t valueLength() const {  return valueLen; }
	uint64_t getCas() const{ return cas; }
	void setCas(uint64_t casArg) { cas = casArg;}
	size_t neededBytes() const;
	size_t getHash() const { return hash; }
	xStringPiece getKey() const { return xStringPiece(data,keyLen); }
	void append(const char *data, size_t len);
	void output(xBuffer *out,bool needCas = false) const;
	void resetKey(xStringPiece k);

	bool endsWithCRLF() const
	{
		return receivedBytes == totalLen() && data[totalLen()-2] == '\r'&& data[totalLen()-1] == '\n';
	}

private:
	int32_t totalLen() const { return keyLen + valueLen; }
	int32_t keyLen;
	const uint32_t flags;
	const int32_t relExptime;
	const int32_t valueLen;
	int32_t receivedBytes;
	uint64_t cas;
	size_t hash;
	char *data;
};



