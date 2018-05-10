#pragma once
#include "all.h"
#include "sds.h"
#include "crc64.h"
#include "object.h"
#include "lzf.h"
#include "session.h"

struct Rio
{
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

	uint64_t cksum;
	size_t processedBytes;
	size_t maxProcessingChunk;

	std::function<size_t(Rio *,void *buf,size_t len)> readFuc;
	std::function<size_t(Rio *,const void *buf,size_t len)> writeFuc;
	std::function<off_t(Rio *)> tellFuc;
	std::function<int32_t(Rio *)> flushFuc;
	std::function<void(Rio *,const void *buf,size_t len)> updateFuc;
};

class Redis;
class Rdb
{
public:
	Rdb(Redis *redis)
	:redis(redis),
	 blockEnabled(true)
	{

	}

	~Rdb()
	{

	}

	int32_t rdbSaveInfoAuxFields(Rio *rdb,int32_t flags);
	ssize_t rdbSaveAuxField(Rio *rdb,char *key,size_t keylen,char *val,size_t vallen);
	ssize_t rdbSaveAuxFieldStrStr(Rio *rdb,char *key,char *val);
	ssize_t rdbSaveAuxFieldStrInt(Rio *rdb,char *key,int64_t val);
	ssize_t rdbSaveRawString(Rio *rdb,uint8_t *s,size_t len);
	ssize_t rdbSaveLongLongAsStringObject(Rio *rdb,int64_t value);

	off_t  rioTell(Rio *r);
	size_t rioWrite(Rio *r,const void *buf,size_t len);
	size_t rioRead(Rio *r,void *buf,size_t len);
	size_t rioRepliRead(Rio *r,void *buf,size_t len);
	off_t  rioFlush(Rio *r);
	size_t rioFileRead(Rio*r,void *buf, size_t len);
	size_t rioFileWrite(Rio *r,const void *buf,size_t len);
	inline off_t rioFileTell(Rio *r);
	int32_t rioFileFlush(Rio *r);

	void rdbLoadRaw(Rio *rdb,int32_t *buf,uint64_t len);
	time_t rdbLoadTime(Rio *rdb);

	size_t rioBufferWrite(Rio *r,const void *buf,size_t len) ;
	size_t rioBufferRead(Rio *r,void *buf,size_t len) ;
	off_t rioBufferTell(Rio *r);
	int32_t rioBufferFlush(Rio *r);

	void rioInitWithFile(Rio *r,FILE *fp);
	void rioInitWithBuffer(Rio *r,sds s);

	int32_t rdbLoadRio(Rio *rdb);
	int32_t startLoading(FILE *fp);
	
	int32_t rdbSaveBinaryDoubleValue(Rio *rdb,double val);
	int32_t rdbSaveMillisecondTime(Rio *rdb,int64_t t);
	int32_t rdbSaveType(Rio *rdb,uint8_t type);
	int32_t rdbSaveLen(Rio *rdb,uint32_t len);
	int32_t rdbSave(char *filename);
	int32_t rdbSaveRio(Rio *rdb,int32_t *error,int32_t flags);
	int32_t rdbSaveObject(Rio *rdb,rObj *o);
	int32_t rdbSaveStringObject(Rio *rdb,rObj *obj);
	int32_t rdbSaveKeyValuePair(Rio *rdb,rObj *key,rObj *val,int64_t expiretime,int64_t now);
	size_t rdbSaveRawString(Rio *rdb,const char *s,size_t len);
	int32_t rdbSaveLzfStringObject(Rio *rdb,uint8_t *s,size_t len);
	int32_t rdbSaveValue(Rio *rdb,rObj *value);
	int32_t rdbSaveKey(Rio *rdb,rObj *value);
	int32_t rdbSaveStruct(Rio *rdb);
	int32_t rdbSaveObjectType(Rio *rdb,rObj *o);

	int32_t rdbLoadType(Rio *rdb);
	uint32_t rdbLoadUType(Rio *rdb);

	int64_t rdbLoadMillisecondTime(Rio *rdb);
	int32_t rdbLoadBinaryDoubleValue(Rio *rdb,double *val);

	int32_t rdbRestoreString(rObj *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreHash(rObj *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreList(rObj *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreZset(rObj *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreSet(rObj *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreExpire(rObj *key,Rio *rdb,int32_t type);
	
	int32_t rdbLoadString(Rio *rdb,int32_t type,int64_t expiretime,int64_t now);
	int32_t rdbLoadHash(Rio *rdb,int32_t type);
	int32_t rdbLoadList(Rio *rdb,int32_t type);
	int32_t rdbLoadZset(Rio *rdb,int32_t type);
	int32_t rdbLoadSet(Rio *rdb,int32_t type);
	uint32_t rdbLoadLen(Rio *rdb,int32_t *isencoded);
	
	int32_t rdbLoad(char *fileName);
	bool rdbReplication(char *filename,const TcpConnectionPtr &conn);

	rObj *rdbLoadObject(int32_t type,Rio *rdb);
	rObj *rdbLoadStringObject(Rio *rdb);
	rObj *rdbLoadIntegerObject(Rio *rdb,int32_t enctype,int32_t encode);
	rObj *rdbLoadEncodedStringObject(Rio *rdb);
	rObj *rdbLoadLzfStringObject(Rio *rdb);
	rObj *rdbGenericLoadStringObject(Rio *rdb,int32_t encode);

	void rioGenericUpdateChecksum(Rio *r,const void *buf,size_t len);
	int32_t rdbWriteRaw(Rio *rdb, void *p,size_t len);
	int32_t rdbTryIntegerEncoding(char *s,size_t len,uint8_t *enc);
	int32_t rdbEncodeInteger(int64_t value,uint8_t *enc);
	int32_t rdbWrite(char *filename,const char *buf,size_t len);
	int32_t rdbSyncWrite(const char *buf,FILE *fp,size_t len);
	int32_t rdbSyncClose(char *fileName,FILE *fp);
	void setBlockEnable(bool enabled) { blockEnabled = enabled; }
	int32_t createDumpPayload(Rio *rdb,rObj *obj);
	int32_t verifyDumpPayload(Rio *rdb,rObj *obj);
	
private:
	Rdb(const Rdb&);
	void operator=(const Rdb&);

	Redis *redis;
	bool blockEnabled;
};

