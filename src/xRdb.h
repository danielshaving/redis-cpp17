#pragma once
#include "all.h"
#include "xSds.h"
#include "xCrc64.h"
#include "xObject.h"
#include "xLzf.h"
#include "xSession.h"

class xRedis;
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


class xRdb
{
public:
	void init(xRedis * redis);
	off_t  rioTell(xRio *r);
	size_t rioWrite(xRio *r,const void *buf,size_t len);
	size_t rioRead(xRio *r,void *buf,size_t len);
	size_t rioRepliRead(xRio * r,void *buf,size_t len);
	off_t  rioFlush(xRio *r);


	size_t rioFileRead(xRio*r, void *buf, size_t len);
	size_t rioFileWrite(xRio *r, const void *buf, size_t len);

	inline off_t rioFileTell(xRio *r);
	int rioFileFlush(xRio *r);

	void rioInitWithFile(xRio *r, FILE *fp);
	void rioInitWithBuffer(xRio *r, sds s);

	FILE * createFile();
	int  closeFile(FILE * fp);


	int rdbSaveType(xRio *rdb, unsigned char type);
	int rdbSaveLen(xRio *rdb, uint32_t len);
	int rdbSave(char *filename);
	int rdbSaveRio(xRio *rdb, int *error);
	int rdbSaveObject(xRio *rdb, rObj *o);
	int rdbSaveStringObject(xRio *rdb, rObj *obj);
	int rdbSaveKeyValuePair(xRio *rdb, rObj *key, rObj *val, long long now);
	size_t rdbSaveRawString(xRio *rdb, const char *s, size_t len);
	int rdbSaveLzfStringObject(xRio *rdb, unsigned char *s, size_t len);
	int rdbSaveValue(xRio *rdb, rObj *value,long long now);
	int rdbSaveKey(xRio *rdb, rObj *value,long long now);
	int rdbSaveSet(xRio *rdb);
	int rdbSaveHset(xRio *rdb);
	int rdbSaveObjectType(xRio *rdb, rObj *o);

	int rdbLoadType(xRio *rdb);
	uint32_t rdbLoadUType(xRio *rdb);
	rObj *rdbLoadIntegerObject(xRio *rdb, int enctype, int encode);
	rObj *rdbLoadEncodedStringObject(xRio *rdb);
	rObj *rdbLoadLzfStringObject(xRio *rdb);
	int rdbLoadSet(xRio *rdb);
	int rdbLoadHset(xRio *rdb);
	uint32_t rdbLoadLen(xRio *rdb, int *isencoded);
	int rdbLoad(char *filename);
	bool  rdbReplication(char *filename,xSession *session);
	rObj *rdbLoadObject(int type, xRio *rdb);
	rObj *rdbLoadStringObject(xRio *rdb);

	void rioGenericUpdateChecksum(xRio *r, const void *buf, size_t len);
	int rdbWriteRaw(xRio *rdb, void *p, size_t len);
	int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc);
	int rdbEncodeInteger(long long value, unsigned char *enc);
	rObj *rdbGenericLoadStringObject(xRio *rdb, int encode);
	int  rdbWrite(char *filename,const char *buf, size_t len);
	int rdbSyncWrite(const char *buf,FILE * fp,size_t len);
	int rdbSyncClose(char * fileName,FILE * fp);
private:
	xRedis * redis;
};

