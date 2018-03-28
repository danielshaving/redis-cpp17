#pragma once
#include "xBuffer.h"

class xHttpResponse : noncopyable
{
public:
	void swap(xBuffer &buf) { sendBuf.swap(buf); }
	xBuffer *intputBuffer() { return &sendBuf; }
	void appendBuffer(const char *buf,size_t len) { sendBuf.append(buf,len); }
	void reset() { sendBuf.retrieveAll(); }
private:
	xBuffer sendBuf;
};
