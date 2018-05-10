#pragma once
#include "buffer.h"

class xHttpResponse : noncopyable
{
public:
	void swap(Buffer &buf) { sendBuf.swap(buf); }
	Buffer *intputBuffer() { return &sendBuf; }
	void appendBuffer(const char *buf,size_t len) { sendBuf.append(buf,len); }
	void reset() { sendBuf.retrieveAll(); }
private:
	Buffer sendBuf;
};
