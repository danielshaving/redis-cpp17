#pragma once
#include "all.h"
#include "xSds.h"
#include "xCrc64.h"
#include "xObject.h"
#include "xLzf.h"
#include "xSession.h"

class xRedis;
class xRio: noncopyable
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
			int32_t *fds;
			int32_t *state;
			int32_t numfds;
			off_t pos;
			sds buf;
		}fdset;
	}io;

public:
	uint64_t cksum;
	size_t processedBytes;
	size_t maxProcessingChunk;

	std::function<size_t (xRio *,void *buf,size_t len) > readFuc;
	std::function<size_t (xRio *,const void *buf,size_t len) > writeFuc;
	std::function<off_t (xRio *) > tellFuc;
	std::function<int32_t (xRio *) > flushFuc;
	std::function<void (xRio *,const void *buf,size_t len) > updateFuc;
};

class xRdb: noncopyable
{
public:
	xRdb(xRedis *redis);
	off_t  rioTell(xRio *r);
	size_t rioWrite(xRio *r,const void *buf,size_t len);
	size_t rioRead(xRio *r,void *buf,size_t len);
	size_t rioRepliRead(xRio *r,void *buf,size_t len);
	off_t  rioFlush(xRio *r);
	size_t rioFileRead(xRio*r,void *buf, size_t len);
	size_t rioFileWrite(xRio *r,const void *buf, size_t len);
	inline off_t rioFileTell(xRio *r);
	int32_t rioFileFlush(xRio *r);

	size_t rioBufferWrite(xRio *r,const void *buf, size_t len) ;
	size_t rioBufferRead(xRio *r,void *buf, size_t len) ;
	off_t rioBufferTell(xRio *r);
	int rioBufferFlush(xRio *r);

	void rioInitWithFile(xRio *r,FILE *fp);
	void rioInitWithBuffer(xRio *r,sds s);
	FILE *createFile();
	int32_t  closeFile(FILE *fp);
	
	int32_t rdbSaveBinaryDoubleValue(xRio *rdb,double val);
	int32_t rdbSaveMillisecondTime(xRio *rdb,int64_t t);
	int32_t rdbSaveType(xRio *rdb,uint8_t type);
	int32_t rdbSaveLen(xRio *rdb,uint32_t len);
	int32_t rdbSave(char *filename);
	int32_t rdbSaveRio(xRio *rdb,int32_t *error);
	int32_t rdbSaveObject(xRio *rdb,rObj *o);
	int32_t rdbSaveStringObject(xRio *rdb, rObj *obj);
	int32_t rdbSaveKeyValuePair(xRio *rdb, rObj *key, rObj *val);
	size_t rdbSaveRawString(xRio *rdb, const char *s, size_t len);
	int32_t rdbSaveLzfStringObject(xRio *rdb,uint8_t *s, size_t len);
	int32_t rdbSaveValue(xRio *rdb,rObj *value);
	int32_t rdbSaveKey(xRio *rdb,rObj *value);
	int32_t rdbSaveStruct(xRio *rdb);
	int32_t rdbSaveExpre(xRio *rdb);
	int32_t rdbSaveObjectType(xRio *rdb,rObj *o);

	int32_t rdbLoadType(xRio *rdb);
	uint32_t rdbLoadUType(xRio *rdb);
	rObj *rdbLoadIntegerObject(xRio *rdb,int32_t enctype, int32_t encode);
	rObj *rdbLoadEncodedStringObject(xRio *rdb);
	rObj *rdbLoadLzfStringObject(xRio *rdb);
	int64_t rdbLoadMillisecondTime(xRio *rdb);
	int32_t rdbLoadBinaryDoubleValue(xRio *rdb,double *val);

	int32_t rdbRestoreString(rObj *key,xRio *rdb,int32_t type);
	int32_t rdbRestoreHash(rObj *key,xRio *rdb,int32_t type);
	int32_t rdbRestoreList(rObj *key,xRio *rdb,int32_t type);
	int32_t rdbRestoreZset(rObj *key,xRio *rdb,int32_t type);
	int32_t rdbRestoreSet(rObj *key,xRio *rdb,int32_t type);
	int32_t rdbRestoreExpire(rObj *key,xRio *rdb,int32_t type);
	
	int32_t rdbLoadString(xRio *rdb,int32_t type);
	int32_t rdbLoadHash(xRio *rdb,int32_t type);
	int32_t rdbLoadList(xRio *rdb,int32_t type);
	int32_t rdbLoadZset(xRio *rdb,int32_t type);
	int32_t rdbLoadSet(xRio *rdb,int32_t type);
	int32_t rdbLoadExpire(xRio *rdb,int32_t type);
	uint32_t rdbLoadLen(xRio *rdb, int32_t *isencoded);
	
	int32_t rdbLoad(char *fileName);
	bool  rdbReplication(char *filename,const TcpConnectionPtr &conn);
	rObj *rdbLoadObject(int32_t type, xRio *rdb);
	rObj *rdbLoadStringObject(xRio *rdb);

	void rioGenericUpdateChecksum(xRio *r,const void *buf, size_t len);
	int32_t rdbWriteRaw(xRio *rdb, void *p,size_t len);
	int32_t rdbTryIntegerEncoding(char *s,size_t len,uint8_t *enc);
	int32_t rdbEncodeInteger(int64_t value,uint8_t *enc);
	rObj *rdbGenericLoadStringObject(xRio *rdb, int32_t encode);
	int32_t  rdbWrite(char *filename,const char *buf, size_t len);
	int32_t rdbSyncWrite(const char *buf,FILE * fp,size_t len);
	int32_t rdbSyncClose(char *fileName,FILE * fp);
	void setBlockEnable(bool enabled) { blockEnabled = enabled; }
	int  createDumpPayload(xRio *rdb,rObj *obj);
	int  verifyDumpPayload(xRio *rdb,rObj *obj);
	
public:
	xRedis * redis;
	bool blockEnabled;
};

