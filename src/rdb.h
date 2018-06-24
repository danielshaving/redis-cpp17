//
// Created by zhanghao on 2018/6/17.
//

#pragma once
#include "all.h"
#include "sds.h"
#include "crc64.h"
#include "object.h"
#include "lzf.h"
#include "session.h"

/* At every loading step try to remember what we were about to do, so that
 * we can log this information when an error is encountered. */
#define RDB_CHECK_DOING_START 0
#define RDB_CHECK_DOING_READ_TYPE 1
#define RDB_CHECK_DOING_READ_EXPIRE 2
#define RDB_CHECK_DOING_READ_KEY 3
#define RDB_CHECK_DOING_READ_OBJECT_VALUE 4
#define RDB_CHECK_DOING_CHECK_SUM 5
#define RDB_CHECK_DOING_READ_LEN 6
#define RDB_CHECK_DOING_READ_AUX 7

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

struct RdbState
{
    Rio *rio;
    RedisObject *key;           /* Current key we are reading. */
    int32_t keyType;           /* Current key type if != -1. */
    uint32_t keys;             /* Number of keys processed. */
    uint32_t expires;          /* Number of keys with an expire. */
    uint32_t alreadyExpired;  /* Number of keys already expired. */
    int32_t doing;            /* The state while reading the RDB. */
    int32_t errorSet;         /* True if error is populated. */
    char error[1024];
};

class Redis;
class Rdb
{
public:
	Rdb(Redis *redis)
	:redis(redis),
	 blockEnabled(true) { }
	~Rdb() { }

	void checkRdb(int32_t argc,char **argv,FILE *fp);
    void rdbCheckInfo(const char *fmt, ...);
    void rdbCheckSetupSignals(void);
    void rdbCheckHandleCrash(int32_t sig,siginfo_t *info,void *secret);
    void rdbCheckSetError(const char *fmt, ...);

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
	int32_t rdbSaveObject(Rio *rdb,RedisObject *o);
	int32_t rdbSaveStringObject(Rio *rdb,RedisObject *obj);
	int32_t rdbSaveKeyValuePair(Rio *rdb,RedisObject *key,RedisObject *val,int64_t expiretime,int64_t now);
	size_t rdbSaveRawString(Rio *rdb,const char *s,size_t len);
	int32_t rdbSaveLzfStringObject(Rio *rdb,uint8_t *s,size_t len);
	int32_t rdbSaveValue(Rio *rdb,RedisObject *value);
	int32_t rdbSaveKey(Rio *rdb,RedisObject *value);
	int32_t rdbSaveStruct(Rio *rdb);
	int32_t rdbSaveObjectType(Rio *rdb,RedisObject *o);

	int32_t rdbLoadType(Rio *rdb);
	uint32_t rdbLoadUType(Rio *rdb);

	int64_t rdbLoadMillisecondTime(Rio *rdb);
	int32_t rdbLoadBinaryDoubleValue(Rio *rdb,double *val);

	int32_t rdbRestoreString(RedisObject *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreHash(RedisObject *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreList(RedisObject *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreZset(RedisObject *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreSet(RedisObject *key,Rio *rdb,int32_t type);
	int32_t rdbRestoreExpire(RedisObject *key,Rio *rdb,int32_t type);
	
	int32_t rdbLoadString(Rio *rdb,int32_t type,int64_t expiretime,int64_t now);
	int32_t rdbLoadHash(Rio *rdb,int32_t type);
	int32_t rdbLoadList(Rio *rdb,int32_t type);
	int32_t rdbLoadZset(Rio *rdb,int32_t type);
	int32_t rdbLoadSet(Rio *rdb,int32_t type);
	uint32_t rdbLoadLen(Rio *rdb,int32_t *isencoded);
	
	int32_t rdbLoad(char *fileName);
	bool rdbReplication(char *filename,const TcpConnectionPtr &conn);

	RedisObject *rdbLoadObject(int32_t type,Rio *rdb);
	RedisObject *rdbLoadStringObject(Rio *rdb);
	RedisObject *rdbLoadIntegerObject(Rio *rdb,int32_t enctype,int32_t encode);
	RedisObject *rdbLoadEncodedStringObject(Rio *rdb);
	RedisObject *rdbLoadLzfStringObject(Rio *rdb);
	RedisObject *rdbGenericLoadStringObject(Rio *rdb,int32_t encode);

	void rioGenericUpdateChecksum(Rio *r,const void *buf,size_t len);
	int32_t rdbWriteRaw(Rio *rdb, void *p,size_t len);
	int32_t rdbTryIntegerEncoding(char *s,size_t len,uint8_t *enc);
	int32_t rdbEncodeInteger(int64_t value,uint8_t *enc);
	int32_t rdbWrite(char *filename,const char *buf,size_t len);
	int32_t rdbSyncWrite(const char *buf,FILE *fp,size_t len);
	int32_t rdbSyncClose(char *fileName,FILE *fp);
	void setBlockEnable(bool enabled) { blockEnabled = enabled; }
	int32_t createDumpPayload(Rio *rdb,RedisObject *obj);
	int32_t verifyDumpPayload(Rio *rdb,RedisObject *obj);
	
private:
	Rdb(const Rdb&);
	void operator=(const Rdb&);

	Redis *redis;
    RdbState rdbState;
	bool blockEnabled;
    int32_t rdbCheckMode;
};


