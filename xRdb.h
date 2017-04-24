#pragma once

#include "xRio.h"
#include "xObject.h"
#include "xLzf.h"
class xRdb
{
public:
	xRdb();
	~xRdb();
	int rdbSaveType(xRio *rdb, unsigned char type);
	int rdbLoadType(xRio *rdb);
	int rdbSaveTime(xRio *rdb, time_t t);
	time_t rdbLoadTime(xRio *rdb);
	int rdbSaveLen(xRio *rdb, uint32_t len);
	uint32_t rdbLoadLen(xRio *rdb, int *isencoded);
	int rdbSaveObjectType(xRio *rdb, rObj *o);
	int rdbLoadObjectType(xRio *rdb);
	uint32_t rdbLoadUType(xRio *rdb);
	int rdbLoad(char *filename);
	int rdbSaveBackground(char *filename);
	int rdbSaveToSlavesSockets(void);
	void rdbRemoveTempFile(pid_t childpid);
	int rdbSave(char *filename);
	int rdbSaveRio(xRio *rdb, int *error);
	int rdbSaveObject(xRio *rdb, rObj *o);
	int rdbSaveStringObject(xRio *rdb, rObj *obj);
	off_t rdbSavedObjectLen(xRio *o);
	off_t rdbSavedObjectPages(xRio *o);
	rObj *rdbLoadObject(int type, xRio *rdb);
	void backgroundSaveDoneHandler(int exitcode, int bysignal);
	int rdbSaveKeyValuePair(xRio *rdb, rObj *key, rObj *val, long long now);
	rObj *rdbLoadStringObject(xRio *rdb);
	int rdbWriteRaw(xRio *rdb, void *p, size_t len);
	int rdbSaveLongLongAsStringObject(xRio*rdb, long long value);
	int rdbSaveRawString(xRio *rdb, const char *s, size_t len);
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
};
