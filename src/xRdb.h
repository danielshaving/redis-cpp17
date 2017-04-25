#pragma once

#include "xRio.h"
#include "xObject.h"
#include "xLzf.h"
#include "xPosix.h"
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
