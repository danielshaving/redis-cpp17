#pragma once
#include "xBuffer.h"

class xHttpResponse
{
public:
	void swap(xBuffer &buf);

	xBuffer *intputBuffer() { return &sendBuf; }
	void appendBuffer(const char *buf,size_t len) { sendBuf.append(buf,len); }

private:
	xBuffer sendBuf;
};
