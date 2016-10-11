#pragma once
#include "all.h"
#include "xSds.h"
#include "xCrc64.h"

class xRio
{
public:
	xRio();
	~xRio();
	union
	{
		struct
		{
			sds ptr;
			off_t pos;
		}buffer;

		struct
		{
			FILE *fp;
			off_t buffered;
			off_t autosync;
		}file;

		struct
		{
			int *fds;
			int *state;
			int numfds;
			off_t pos;
			sds buf;
		}fdset;
	}io;

public:
	uint64_t cksum;
	size_t processedBytes;
	size_t maxProcessingChunk;

	std::function<size_t (xRio *,void * buf,size_t len) > readFuc;
	std::function<size_t (xRio *,const void *buf,size_t len) > writeFuc;
	std::function<off_t (xRio *) > tellFuc;
	std::function<int (xRio *) > flushFuc;
	std::function<void (xRio *,const void *buf,size_t len) > updateFuc;
};


off_t  rioTell(xRio *r);
size_t rioWrite(xRio *r,const void *buf,size_t len);
size_t rioRead(xRio *r,void *buf,size_t len);
off_t  rioFlush(xRio *r);


size_t rioFileRead(xRio*r, void *buf, size_t len);
size_t rioFileWrite(xRio *r, const void *buf, size_t len);

inline off_t rioFileTell(xRio *r);
int rioFileFlush(xRio *r);

void rioInitWithFile(xRio *r, FILE *fp);
void rioInitWithBuffer(xRio *r, sds s);

void rioGenericUpdateChecksum(xRio *r, const void *buf, size_t len);
