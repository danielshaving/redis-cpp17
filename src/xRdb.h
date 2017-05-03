#pragma once
#include "all.h"
#include "xSds.h"
#include "xCrc64.h"
#include "xObject.h"
#include "xLzf.h"
#include "xPosix.h"

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


class xRedis;
class xRdb
{
public:
	xRdb();
	~xRdb();
	int rdbSaveType(xRio *rdb, unsigned char type);
	int rdbLoadType(xRio *rdb);
	int rdbSaveLen(xRio *rdb, uint32_t len);
	uint32_t rdbLoadLen(xRio *rdb, int *isencoded);
	int rdbSaveObjectType(xRio *rdb, rObj *o);
	int rdbLoadObjectType(xRio *rdb);
	uint32_t rdbLoadUType(xRio *rdb);
	int rdbLoad(char *filename,xRedis * redis);
	int rdbSave(char *filename,xRedis * redis);
	int rdbSaveRio(xRio *rdb, int *error,xRedis * redis);
	int rdbSaveObject(xRio *rdb, rObj *o);
	int rdbSaveStringObject(xRio *rdb, rObj *obj);
	rObj *rdbLoadObject(int type, xRio *rdb);
	int rdbSaveKeyValuePair(xRio *rdb, rObj *key, rObj *val, long long now);
	rObj *rdbLoadStringObject(xRio *rdb);
	int rdbWriteRaw(xRio *rdb, void *p, size_t len);
	size_t rdbSaveRawString(xRio *rdb, const char *s, size_t len);
	int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc);
	int lll2string(char* dst, size_t dstlen, long long svalue);
	int rdbEncodeInteger(long long value, unsigned char *enc);
	uint32_t digits10(uint64_t v);
	void startLoading(FILE *fp);
	rObj *rdbGenericLoadStringObject(xRio *rdb, int encode);
	rObj *rdbLoadIntegerObject(xRio *rdb, int enctype, int encode);
	rObj *rdbLoadEncodedStringObject(xRio *rdb);
	int rdbSaveLzfStringObject(xRio *rdb, unsigned char *s, size_t len);
	int rdbSaveValue(xRio *rdb, rObj *value,long long now);
	int rdbSaveKey(xRio *rdb, rObj *value,long long now);
	rObj *rdbLoadLzfStringObject(xRio *rdb);

	int rdbLoadSet(xRio *rdb,xRedis * redis);
	int rdbLoadHset(xRio *rdb,xRedis * redis);

	int rdbSaveSet(xRio *rdb,xRedis * redis);
	int rdbSaveHset(xRio *rdb,xRedis * redis);
	
private:
	mutable MutexLock mutex;
};