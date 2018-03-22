#include "xLog.h"
#include "xItem.h"

xItem::xItem(xStringPiece keyArg,
           uint32_t flagsArg,
           int32_t exptimeArg,
           int32_t valuelen,
           uint64_t casArg)
  : keyLen(keyArg.size()),
    flags(flagsArg),
    relExptime(exptimeArg),
    valueLen(valuelen),
    receivedBytes(0),
    cas(casArg),
    hash(dictGenHashFunction(keyArg.data(),keyArg.size())),
    data(static_cast<char*>(zmalloc(totalLen())))
{
	assert(valuelen >= 2);
	assert(receivedBytes < totalLen());
	append(keyArg.data(), keyLen);
}

size_t xItem::neededBytes() const
{
	return totalLen() - receivedBytes;
}

void xItem::append(const char *data, size_t len)
{
	assert(len <= neededBytes());
	memcpy(this->data + receivedBytes, data, len);
	receivedBytes += static_cast<int32_t>(len);
	assert(receivedBytes <= totalLen());
}

void xItem::output(xBuffer *out, bool needCas) const
{
	out->append("VALUE ");
	out->append(data, keyLen);
	xLogStream buf;
	buf << ' ' << getFlags() << ' ' << valueLen-2;
	if (needCas)
	{
		buf << ' ' << getCas();
	}

	buf << "\r\n";
	out->append(buf.getBuffer().getData(), buf.getBuffer().length());
	out->append(value(), valueLen);
}

void xItem::resetKey(xStringPiece k)
{
	assert(k.size() <= 250);
	keyLen = k.size();
	receivedBytes = 0;
	append(k.data(), k.size());
	hash = dictGenHashFunction(k.data(),k.size());
}

