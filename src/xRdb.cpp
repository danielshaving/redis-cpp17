#include "xRdb.h"
#include "xRedis.h"

/* Returns 1 or 0 for success/failure. */
size_t xRdb::rioBufferWrite(xRio *r,const void *buf,size_t len)
{
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns 1 or 0 for success/failure. */
size_t xRdb::rioBufferRead(xRio *r,void *buf,size_t len)
{
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns read/write position in buffer. */
off_t xRdb::rioBufferTell(xRio *r) 
{
    return r->io.buffer.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
int xRdb::rioBufferFlush(xRio *r)
{
    UNUSED(r);
    return 1; /* Nothing to do, our write just appends to the buffer. */
}


off_t xRdb::rioTell(xRio *r)
{
	return r->tellFuc(r);
}

off_t xRdb::rioFlush(xRio *r)
{
	return r->flushFuc(r);
}

size_t xRdb::rioWrite(xRio *r,const void *buf,size_t len)
{
	while(len)
	{
		size_t bytesToWrite = (r->maxProcessingChunk && r->maxProcessingChunk < len) ? r->maxProcessingChunk:len;
		if (r->updateFuc)
		{
			r->updateFuc(r,buf,bytesToWrite);
		}

		if (r->writeFuc(r,buf,bytesToWrite) == 0)
		{
			return 0;
		}

		buf = (char*)buf + bytesToWrite;
		len -= bytesToWrite;
		r->processedBytes += bytesToWrite;
	}

	return 1;
}


size_t xRdb::rioRepliRead(xRio *r,void *buf,size_t len)
{
	fseek(r->io.file.fp,r->processedBytes ,SEEK_SET);
	size_t readBytes = ::fread(buf,1,len,r->io.file.fp);
	if (readBytes == 0)
	{
		return 0;
	}

	if (r->updateFuc)
	{
		r->updateFuc(r,buf,readBytes);
	}

	r->processedBytes += readBytes;
	return readBytes;
}

size_t xRdb::rioRead(xRio *r,void *buf,size_t len)
{
	while(len)
	{
		size_t bytesToRead = (r->maxProcessingChunk && r->maxProcessingChunk <len)? r->maxProcessingChunk:len;
		if (r->readFuc(r,buf,bytesToRead) == 0)
		{
			return 0;
		}

		if (r->updateFuc)
		{
			r->updateFuc(r,buf,bytesToRead);
		}

		buf = (char*)buf + bytesToRead;
		len -= bytesToRead;
		r->processedBytes += bytesToRead;
	}

	return 1;
}

size_t xRdb::rioFileRead(xRio *r,void *buf,size_t len)
{
	return fread(buf,len,1,r->io.file.fp);
}

size_t xRdb::rioFileWrite(xRio *r,const void *buf,size_t len)
{
	 size_t retval;
	 retval = fwrite(buf,len,1,r->io.file.fp);
	 r->io.file.buffered += len;

	 if (r->io.file.autosync && r->io.file.buffered >= r->io.file.autosync)
	 {
	 	fflush(r->io.file.fp);
	 }

	 return retval;
}

off_t xRdb::rioFileTell(xRio *r)
{
	return ftello(r->io.file.fp);
}

int32_t xRdb::rioFileFlush(xRio *r)
{
	return (fflush(r->io.file.fp) == 0) ? 1:0;
}

void xRdb::rioGenericUpdateChecksum(xRio *r,const void *buf,size_t len)
{
	r->cksum = crc64(r->cksum,(const unsigned char*)buf,len);
}

void xRdb::rioInitWithBuffer(xRio *r,sds s)
{
	r->readFuc = std::bind(&xRdb::rioBufferRead,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->writeFuc = std::bind(&xRdb::rioBufferWrite,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->tellFuc = std::bind(&xRdb::rioBufferTell,this,std::placeholders::_1);
	r->flushFuc = std::bind(&xRdb::rioBufferFlush,this,std::placeholders::_1);
	r->io.buffer.ptr = s;
	r->io.buffer.pos = 0;
}

void xRdb::rioInitWithFile(xRio *r,FILE *fp)
{
	r->readFuc = std::bind(&xRdb::rioFileRead,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->writeFuc = std::bind(&xRdb::rioFileWrite,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->tellFuc = std::bind(&xRdb::rioFileTell,this,std::placeholders::_1);
	r->flushFuc = std::bind(&xRdb::rioFileFlush,this,std::placeholders::_1);
	r->updateFuc = std::bind(&xRdb::rioGenericUpdateChecksum,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->cksum = 0;
	r->processedBytes = 0;
	r->maxProcessingChunk = 1024 * 64;
	r->io.file.fp = fp;
	r->io.file.buffered = 0;
	r->io.file.autosync = 0;
}

int32_t xRdb::rdbEncodeInteger(int64_t value,uint8_t *enc)
{
	if (value >= -(1<<7) && value <= (1<<7)-1)
	{
		enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
		enc[1] = value&0xFF;
		return 2;
	} 
	else if (value >= -(1<<15) && value <= (1<<15)-1)
	{
		enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
		enc[1] = value&0xFF;
		enc[2] = (value>>8)&0xFF;
		return 3;
	}
	else if (value >= -((int64_t)1<<31) && value <= ((int64_t)1<<31)-1)
	{
		enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
		enc[1] = value&0xFF;
		enc[2] = (value>>8)&0xFF;
		enc[3] = (value>>16)&0xFF;
		enc[4] = (value>>24)&0xFF;
		return 5;
	} 
	else 
	{
		return 0;
   	}
}

int32_t xRdb::rdbTryIntegerEncoding(char *s,size_t len,uint8_t *enc)
{
	int64_t value;
	char *endptr,buf[32];

	value = strtoll(s,&endptr,10);
	if (endptr[0] != '\0')
	{
		return 0;
	}
	
	ll2string(buf,32,value);
	if (strlen(buf) != len || memcmp(buf,s,len)) 
	{
		return 0;	
	}
  	return rdbEncodeInteger(value,enc);
}

uint32_t xRdb::rdbLoadLen(xRio *rdb, int32_t *isencoded)
{
	unsigned char buf[2];
	uint32_t len;
	int32_t type;
	if (isencoded)
	{
		*isencoded = 0;
	}

	if (rioRead(rdb,buf,1) == 0)
	{
		return REDIS_RDB_LENERR;
	}

	type = (buf[0]&0xC0)>>6;

	if (type == REDIS_RDB_ENCVAL)
	{
		if (isencoded)
		{
			*isencoded = 1;
		}
		return buf[0]&0x3F;
	}
	else if (type == REDIS_RDB_6BITLEN)
	{
		return buf[0]&0x3F;
	}
	else if (type == REDIS_RDB_14BITLEN)
	{
		if (rioRead(rdb,buf+1,1) == 0)
		{
			return REDIS_RDB_LENERR;	
		}
        return ((buf[0]&0x3F)<<8)|buf[1];
	}
	else
	{
		if (rioRead(rdb,&len,4) == 0)
		{
			return REDIS_RDB_LENERR;
		}
        return ntohl(len);
	}
	
}

rObj *xRdb::rdbLoadIntegerObject(xRio *rdb,int32_t enctype,int32_t encode)
{
	unsigned char enc[4];
	int64_t val;

	if (enctype == REDIS_RDB_ENC_INT8)
	{
		if (rioRead(rdb,enc,1) == 0)
		{
			return nullptr;
		}
		
		val = (signed char)enc[0];
	}
	else if (enctype == REDIS_RDB_ENC_INT16)
	{
		uint16_t v;
		if (rioRead(rdb,enc,2) == 0) return nullptr;
		v = enc[0]|(enc[1]<<8);
		val = (int16_t)v;
	}
	else if (enctype == REDIS_RDB_ENC_INT32)
	{
		uint32_t v;
		if (rioRead(rdb,enc,4) == 0) return nullptr;
		v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
		val = (int32_t)v;
	} 
	else 
	{
		val = 0;
		LOG_ERROR<<"Unknown RDB integer encoding type";
	}

	if (encode)
        return redis->getObject()->createStringObjectFromLongLong(val);
    else
        return redis->getObject()->createObject(REDIS_STRING,sdsfromlonglong(val));
}

rObj *xRdb::rdbLoadEncodedStringObject(xRio *rdb)
{
	return rdbGenericLoadStringObject(rdb,1);
}

rObj *xRdb::rdbLoadLzfStringObject(xRio *rdb)
{
    rObj *obj = nullptr;
	uint32_t len,clen;
	unsigned char *c = nullptr;
	sds val = nullptr;

	if ((clen = rdbLoadLen(rdb, nullptr)) == REDIS_RDB_LENERR) return nullptr;
	if ((len = rdbLoadLen(rdb, nullptr)) == REDIS_RDB_LENERR) return nullptr;
	if ((c = (unsigned char *)zmalloc(clen)) == nullptr) goto err;
	if ((val = sdsnewlen(nullptr, len)) == nullptr) goto err;
	if (rioRead(rdb, c, clen) == 0) goto err;
	if (lzf_decompress(c, clen, val, len) == 0) goto err;

	obj = redis->getObject()->createStringObject(val,len);
	sdsfree(val);
	zfree(c);
	return obj;
	err:
	zfree(c);
	sdsfree(val);
	return nullptr;
}


rObj *xRdb::rdbGenericLoadStringObject(xRio *rdb,int32_t encode)
{
	int32_t isencoded;
	uint32_t len;
	rObj *o;

	len = rdbLoadLen(rdb,&isencoded);
	if (isencoded)
	{
		switch (len)
		{
		case REDIS_RDB_ENC_INT8:
		case REDIS_RDB_ENC_INT16:
		case REDIS_RDB_ENC_INT32:
			return rdbLoadIntegerObject(rdb, len, encode);
		case REDIS_RDB_ENC_LZF:
			return rdbLoadLzfStringObject(rdb);
		default:
			LOG_WARN << "Unknown RDB encoding type";
			break;
		}
	}

	if (len == REDIS_RDB_LENERR)
	{
		return nullptr;
	}

	o = redis->getObject()->createStringObject(nullptr,len);
	if (len && rioRead(rdb,(void*)o->ptr,len) == 0)
	{
		return nullptr;
	}

	return o;
}

rObj *xRdb::rdbLoadStringObject(xRio *rdb)
{
	return rdbGenericLoadStringObject(rdb, 0);
}


int32_t xRdb::rdbSaveBinaryDoubleValue(xRio *rdb, double val)
{
	 memrev64ifbe(&val);
	 return rdbWriteRaw(rdb,&val,sizeof(val));
}

int32_t xRdb::rdbSaveStruct(xRio *rdb)
{
	int64_t now = mstime();

	size_t n = 0;
	auto &redisShards = redis->getRedisShards();
	for (auto &it : redisShards)
	{
		auto &map = it.redis;
		auto &mu = it.mtx;
		auto &stringMap = it.stringMap;
		auto &hashMap = it.hashMap;
		auto &listMap = it.listMap;
		auto &zsetMap = it.zsetMap;
		auto &setMap =it.setMap;
		
		if (blockEnabled) mu.lock();

		for (auto &iter : map)
		{
			int64_t expire = redis->getExpire(iter);
			if (iter->type == OBJ_STRING)
			{
				auto iterr = stringMap.find(iter);
				assert(iterr != stringMap.end());
				assert(iterr->first->type == OBJ_STRING);

				if (rdbSaveKeyValuePair(rdb,iterr->first,iterr->second,expire,now) == -1) return -1;
			}
			else if (iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);

				if (rdbSaveKey(rdb,iterr->first) == -1) return -1;
				if (rdbSaveLen(rdb,iterr->second.size()) == -1) return -1;

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb,iterrr) == -1) return -1;
				}

			}
			else if (iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
				assert(iterr != hashMap.end());

				assert(iterr->first->type == OBJ_HASH);
				if (rdbSaveKey(rdb,iterr->first) == -1) return -1;
				if (rdbSaveLen(rdb,iterr->second.size()) == -1) return -1;

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveKeyValuePair(rdb,iterrr.first,iterrr.second,expire,now) == -1) return -1;
				}
			}
			else if (iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.keyMap.size() == iterr->second.sortMap.size());
				if (rdbSaveKey(rdb,iterr->first) == -1) return -1;
				if (rdbSaveLen(rdb,iterr->second.keyMap.size()) == -1) return -1;
			
				for (auto &iterrr : iterr->second.keyMap)
				{
					if (rdbSaveBinaryDoubleValue(rdb,iterrr.second) == -1) return -1;
					if (rdbSaveValue(rdb, iterrr.first) == -1) return -1;
				}
						
			}
			else if (iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);

				if (rdbSaveKey(rdb,iterr->first) == -1) return -1;
				if (rdbSaveLen(rdb,iterr->second.size()) == -1) return -1;
	
				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == -1) return -1;
				}
				
			}
			else
			{
				LOG_ERROR<<"unkown key type "<<iter->type;
				assert(false);
			}
		}
		
		if (blockEnabled) mu.unlock();
	}

	return 1;
}

int32_t xRdb::rdbLoadExpire(xRio *rdb,int32_t type)
{
	rObj *key;
	int64_t expire;
	if ((key = rdbLoadStringObject(rdb)) == nullptr) return -1;
	if ((expire = rdbLoadMillisecondTime(rdb)) == -1) return -1;

	int64_t curExpire = xTimeStamp::now().getMicroSecondsSinceEpoch();
	if (curExpire > expire)
	{
		key->calHash();
		key->type = OBJ_EXPIRE;
		std::unique_lock <std::mutex> lck(redis->getExpireMutex());
		auto timer = redis->getEventLoop()->runAfter((expire - curExpire) / 1000000,false,
			std::bind(&xRedis::setExpireTimeOut,redis,key));
		redis->getExpireTimer().insert(std::make_pair(key,timer));
	}
	else
	{
		redis->removeCommand(key);
		zfree(key);
	}
	
	return 1;
}

int32_t xRdb::rdbLoadSet(xRio *rdb,int32_t type)
{
	std::unordered_set<rObj*,Hash,Equal> set;
	rObj *key;
	int32_t len;

	if ((key = rdbLoadStringObject(rdb)) == nullptr) return -1;

	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb, nullptr)) == -1) return -1;

	set.reserve(len);
	
	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((val = rdbLoadObject(type, rdb)) == nullptr) return -1;

		val->type = OBJ_ZSET;
		val->calHash();

		auto it = set.find(val);
		assert(it != set.end());
		set.insert(val);
	}

	assert(!set.empty());

	auto &redisShards = redis->getRedisShards();

	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = setMap.find(key);
		assert(it == setMap.end());
		setMap.insert(std::make_pair(key,std::move(set)));
	}

	return 1;
}

int32_t xRdb::rdbLoadZset(xRio *rdb,int32_t type)
{
	xRedis::SortSet sortSet;
	
	rObj *key;
	int32_t rdbver;
	int32_t len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr) return -1;

	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb,nullptr)) == -1) return -1;

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		double socre;
		if (rdbLoadBinaryDoubleValue(rdb,&socre) == -1) return -1;
		if ((val = rdbLoadObject(type,rdb)) == nullptr) return -1;

		val->type = OBJ_ZSET;
		val->calHash();
		sortSet.sortMap.insert(std::make_pair(socre,val));
		sortSet.keyMap.insert(std::make_pair(val,socre));
	}
	
	assert(!sortSet.sortMap.empty());
	assert(!sortSet.keyMap.empty());

	auto &redisShards = redis->getRedisShards();

	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &zsetMap = redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = zsetMap.find(key);
		assert(it == zsetMap.end());

		auto iter = map.find(key);
		assert(iter == map.end());

		zsetMap.insert(std::make_pair(key,std::move(sortSet)));
		map.insert(key);
	}
		
	return 1;
}

int32_t xRdb::rdbLoadList(xRio *rdb, int32_t type)
{
	std::deque<rObj*> list;
	rObj *key;
	int32_t rdbver;
	int32_t len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr) return -1;

	key->type = OBJ_LIST;
	key->calHash();
	
	if ((len = rdbLoadLen(rdb, nullptr)) == -1) return -1;

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((rdbver = rdbLoadType(rdb)) == -1) return -1;
		if (rdbver != REDIS_RDB_TYPE_STRING) return -1;
		if ((val = rdbLoadObject(rdbver, rdb)) == nullptr) return -1;

		val->type = OBJ_LIST;
		val->calHash();
		list.push_back(val);
	}

	assert(!list.empty());

	auto &redisShards = redis->getRedisShards();

	size_t index = key->hash% redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map =redisShards[index].redis;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);

		auto it = listMap.find(key);
		assert(it == listMap.end());

		auto iter = map.find(key);
		assert(iter == map.end());

		map.insert(key);
		listMap.insert(std::make_pair(key, std::move(list)));
	}

	return 1;
}

int32_t xRdb::rdbLoadHash(xRio *rdb,int32_t type)
{
	std::unordered_map<rObj*,rObj*,Hash,Equal> rhash;
	rObj *key,*kkey,*val;
	int32_t len,rdbver;
	if ((key = rdbLoadStringObject(rdb)) == nullptr) return -1;
	key->calHash();
	key->type = OBJ_HASH;
	
	if ((len = rdbLoadLen(rdb,nullptr)) == -1) return -1;
	for (int32_t i = 0 ; i < len; i ++)
	{
		if ((rdbver = rdbLoadType(rdb)) == -1) return -1;
		if (rdbver != REDIS_RDB_TYPE_HASH) return -1;

		if ((kkey = rdbLoadStringObject(rdb)) == nullptr)
		{
			return REDIS_ERR;
		}
		
		kkey->calHash();
		kkey->type = OBJ_HASH;
		
		if ((val = rdbLoadObject(rdbver,rdb)) == nullptr) return -1;
		
		val->calHash();
		val->type = OBJ_HASH;

		rhash.insert(std::make_pair(kkey,val));
	}

	assert(!rhash.empty());
	auto &redisShards = redis->getRedisShards();

	size_t index  = key->hash% redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);

		auto it = hashMap.find(key);
		assert(it == hashMap.end());

		auto iter = map.find(key);
		if (iter == map.end());

		hashMap.insert(std::make_pair(key,std::move(rhash)));
		map.insert(key);
	}
	
	return 1;
}


int32_t xRdb::rdbRestoreString(rObj *key,xRio *rdb,int32_t type)
{
	rObj *val;
	if ((val = rdbLoadObject(type,rdb)) == nullptr) return -1;

	key->calHash();
	key->type = OBJ_STRING;
	val->type = OBJ_STRING;

	auto &redisShards = redis->getRedisShards();

	size_t index  = key->hash% redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &stringMap = redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(key);
		assert(it == map.end());

		auto iter = stringMap.find(key);
		assert(iter == stringMap.end());

		map.insert(key);
		stringMap.insert(std::make_pair(key,val));
	}

	return 1;
}

int32_t xRdb::rdbRestoreHash(rObj *key,xRio *rdb,int32_t type)
{
	std::unordered_map<rObj*,rObj*,Hash,Equal> rhash;
	rObj *kkey,*val;
	int32_t len,rdbver;
	key->calHash();
	key->type = OBJ_HASH;
	
	if ((len = rdbLoadLen(rdb,nullptr)) == -1) return -1;
	for (int32_t i = 0 ; i < len; i ++)
	{
		if ((rdbver = rdbLoadType(rdb)) == -1) return -1;
		if (rdbver != REDIS_RDB_TYPE_HASH) return -1;
		if ((kkey = rdbLoadStringObject(rdb)) == nullptr) return -1;
		
		kkey->calHash();
		kkey->type = OBJ_HASH;
		
		if ((val = rdbLoadObject(rdbver,rdb)) == nullptr) return -1;
		
		val->calHash();
		val->type = OBJ_HASH;

		rhash.insert(std::make_pair(kkey,val));
	}
	assert(!rhash.empty());

	auto &redisShards = redis->getRedisShards();
	size_t index  = key->hash% redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
	
		auto it = hashMap.find(key);
		assert(it == hashMap.end());

		auto iter = map.find(key);
		if (iter == map.end());
		hashMap.insert(std::make_pair(key,std::move(rhash)));
		map.insert(key);
	}

	return 1;
}

int32_t xRdb::rdbRestoreList(rObj *key,xRio *rdb,int32_t type)
{
	std::deque<rObj*> list;
	int32_t  rdbver;
	int32_t  len;
	key->type = OBJ_LIST;
	key->calHash();
	
	if ((len = rdbLoadLen(rdb, nullptr)) == -1) return -1;

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((rdbver = rdbLoadType(rdb)) == -1) return -1;
		if (rdbver != REDIS_RDB_TYPE_STRING) return -1;
		if ((val = rdbLoadObject(rdbver, rdb)) == nullptr) return -1;

		val->type = OBJ_LIST;
		val->calHash();
		list.push_back(val);
	}

	assert(!list.empty());
	auto &redisShards = redis->getRedisShards();

	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &listMap = redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(key);
		assert(it == listMap.end());
		auto iter = map.find(key);
		assert(iter == map.end());
		map.insert(key);
		listMap.insert(std::make_pair(key,std::move(list)));
	}

	return 1;
}

int32_t xRdb::rdbRestoreZset(rObj *key,xRio *rdb,int32_t type)
{
	xRedis::SortSet sortSet;
	int32_t rdbver;
	int32_t len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr) return -1;

	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb, nullptr)) == -1) return -1;

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		double socre;
		if (rdbLoadBinaryDoubleValue(rdb,&socre) == -1) return -1;	
		if ((val = rdbLoadObject(type, rdb)) == nullptr) return -1;

		val->type = OBJ_ZSET;
		val->calHash();
		sortSet.sortMap.insert(std::make_pair(socre,val));
		sortSet.keyMap.insert(std::make_pair(val,socre));
	}
	
	assert(!sortSet.sortMap.empty());
	assert(!sortSet.keyMap.empty());

	auto &redisShards = redis->getRedisShards();

	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &zsetMap = redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = zsetMap.find(key);
		assert(it == zsetMap.end());
		auto iter = map.find(key);
		assert(iter == map.end());
		zsetMap.insert(std::make_pair(key,std::move(sortSet)));
		map.insert(key);
	}
		
	return 1;
}

int32_t xRdb::rdbRestoreSet(rObj *key,xRio *rdb,int32_t type)
{
	std::unordered_set<rObj*,Hash,Equal> set;
	int32_t  len;
	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb, nullptr)) == -1) return -1;

	set.reserve(len);
	
	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((val = rdbLoadObject(type, rdb)) == nullptr) return -1;

		val->type = OBJ_ZSET;
		val->calHash();

		auto it = set.find(val);
		assert(it != set.end());
		set.insert(val);
	}

	assert(!set.empty());

	auto &redisShards = redis->getRedisShards();

	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = setMap.find(key);
		assert(it == setMap.end());
		setMap.insert(std::make_pair(key,std::move(set)));
	}
	
	return 1;
}

int32_t xRdb::rdbRestoreExpire(rObj *key,xRio *rdb,int32_t type)
{
	int64_t expire;
	if ((expire = rdbLoadMillisecondTime(rdb)) == -1) return -1;

	int64_t curExpire = xTimeStamp::now().getMicroSecondsSinceEpoch();
	if (curExpire > expire)
	{
		key->calHash();
		key->type = OBJ_EXPIRE;
		std::unique_lock <std::mutex> lck(redis->getExpireMutex());
		auto timer = redis->getEventLoop()->runAfter((expire - curExpire) / 1000000,
			false,std::bind(&xRedis::setExpireTimeOut,redis,key));
		redis->getExpireTimer().insert(std::make_pair(key,timer));
	}
	else
	{
		redis->removeCommand(key);
		zfree(key);
	}
	
	return 1;
}

int32_t xRdb::rdbLoadString(xRio *rdb,int32_t type)
{
	rObj *key,*val;
	if ((key = rdbLoadStringObject(rdb)) == nullptr) return -1;
	if ((val = rdbLoadObject(type,rdb)) == nullptr) return -1;

	key->calHash();
	key->type = OBJ_STRING;
	val->type = OBJ_STRING;
	auto &redisShards = redis->getRedisShards();
	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &stringMap = redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(key);
		assert(it == map.end());

		auto iter = stringMap.find(key);
		assert(iter == stringMap.end());
		map.insert(key);
		stringMap.insert(std::make_pair(key,val));
	}
	return 1;
}

bool xRdb::rdbReplication(char *filename,const TcpConnectionPtr &conn)
{
	xRio rdb;
	FILE *fp ;
	if ((fp = fopen(filename,"r")) == nullptr) return false;
	
	rioInitWithFile(&rdb,fp);
	struct stat sb;
	if (fstat(fileno(fp),&sb) == -1) return false;

	LOG_INFO<<"load dump.rdb size :"<<sb.st_size;
	

	int32_t fd;
	fd = fileno(fp);
	if (-1 == fd)
	{
		LOG_WARN<<"fp to fd error";
		return false;
	}

	size_t len = REDIS_SLAVE_SYNC_SIZE;
	char str[4];
	int32_t * sendLen = (int32_t*)str;
	*sendLen = sb.st_size;
	int32_t sendBytes  = * sendLen ;
	ssize_t n = ::write(conn->getSockfd(), str, 4);
	if (n < 0) return false;

	size_t sendSize;
	off_t offset = 0;
	ssize_t nwrote = 0;
	while(sendBytes)
	{
			if (sendBytes <  len) { len = sendBytes; }
#ifdef __linux__
			nwrote = ::sendfile(conn->getSockfd(),fd,&offset,len);
#endif

			if (nwrote >=0) { sendBytes -=nwrote; }
	}

	fclose(fp);
	return true;
}

int32_t xRdb::closeFile(FILE *fp)
{
	if (fclose(fp) == EOF) return -1;
	return 1;
}

FILE *xRdb::createFile()
{
	FILE *fp ;
	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)getpid());
	fp = fopen(tmpfile,"w");
	if (!fp)
	{
		LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		return nullptr;
	}
	
	return fp;	
}

int32_t xRdb::rdbSyncWrite(const char *buf,FILE *fp,size_t len)
{
	xRio rdb;
	rioInitWithFile(&rdb,fp);
	if (rioWrite(&rdb,buf,len) == 0) return REDIS_ERR;
}


int32_t xRdb::createDumpPayload(xRio *rdb,rObj *obj)
{
	auto &redisShards = redis->getRedisShards();
	size_t index = obj->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redis;
	auto &stringMap = redisShards[index].stringMap;
	auto &hashMap = redisShards[index].hashMap;
	auto &listMap = redisShards[index].listMap;
	auto &zsetMap = redisShards[index].zsetMap;
	auto &setMap = redisShards[index].setMap;
	
	{
		std::unique_lock <std::mutex> lck(mu);
		auto iter = map.find(obj);
		if (iter != map.end())
		{
			if ((*iter)->type == OBJ_STRING)
			{
				auto iterr = stringMap.find(obj);
				assert(iterr != stringMap.end());
				assert(iterr->first->type == OBJ_STRING);
				if (rdbSaveValue(rdb,iterr->second) == -1) return -1;

			}
			else if ((*iter)->type == OBJ_LIST)
			{
				auto iterr = listMap.find(obj);
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);
				if (rdbSaveLen(rdb, iterr->second.size()) == -1) return -1;
		

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == -1) return -1;
		
				}
			}
			else if ((*iter)->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(obj);
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);
				if (rdbSaveLen(rdb,iterr->second.size()) == -1) return -1;
		

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveKeyValuePair(rdb,iterrr.first,iterrr.second,-1,-1) == -1) return -1;
				}
			}
			else if ((*iter)->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(obj);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.keyMap.size() == iterr->second.sortMap.size());
				if (rdbSaveLen(rdb,iterr->second.keyMap.size()) == -1) return -1;			
				for (auto &iterrr : iterr->second.keyMap)
				{
					if (rdbSaveBinaryDoubleValue(rdb,iterrr.second) == -1) return -1;
					if (rdbSaveValue(rdb, iterrr.first) == -1) return -1;
					
				}
			}
			else if ((*iter)->type == OBJ_SET)
			{
				auto iterr = setMap.find(obj);
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);
				if (rdbSaveLen(rdb,iterr->second.size()) == -1) return -1;

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == -1) return -1;
				}
			}
			else
			{
				LOG_ERROR<<"unkown key type "<<(*iter)->type;
				assert(false);
			}
		}
	}

	if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) return -1;

	return 1;
}


int32_t xRdb::verifyDumpPayload(xRio *rdb,rObj *obj)
{	
	int type;
	while(1)
	{
		if ((type = rdbLoadType(rdb)) == -1) return  REDIS_ERR;
		if (type == RDB_OPCODE_EOF) break;

		switch(type)
		{
			case REDIS_STRING:
			{
				if (rdbRestoreString(obj,rdb,type) != REDIS_OK)
				{
					LOG_WARN<<"rdbRestoreString error";
					return REDIS_ERR;
				}

				break;
			}
			case REDIS_EXPIRE:
			{
				if (rdbRestoreExpire(obj,rdb,type) != REDIS_OK)
				{
					LOG_WARN<<"rdbRestoreExpire error";
					return REDIS_ERR;
				}

				break;
			}
			case REDIS_HASH:
			{
				if (rdbRestoreHash(obj,rdb,type) != REDIS_OK)
				{
					LOG_WARN<<"rdbRestoreHash error";
					return REDIS_ERR;
				}
				break;
			}
			case REDIS_LIST:
			{
				if (rdbRestoreList(obj,rdb, type) != REDIS_OK)
				{
					LOG_WARN << "rdbRestoreList error";
					return REDIS_ERR;
				}
				break;
			}
			case REDIS_SET:
			{
				if (rdbRestoreSet(obj,rdb, type) != REDIS_OK)
				{
					LOG_WARN << "rdbRestoreSet error";
					return REDIS_ERR;
				}
				break;
			}
			default:
			{
				assert(false);
				break;
			}
		}
	}
	
	return REDIS_OK;
}

int32_t xRdb::rdbSyncClose(char *fileName,FILE *fp)
{
	if (::fflush(fp) == EOF) return REDIS_ERR;
	if (::fsync(fileno(fp)) == -1) return REDIS_ERR;
	if (::fclose(fp) == EOF) return REDIS_ERR;

	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)getpid());
	
	if (::rename(tmpfile,fileName) == -1) return REDIS_ERR;
	return REDIS_OK;
}

int32_t xRdb::rdbWrite(char *filename,const char *buf,size_t len)
{
	FILE *fp;
	xRio rdb;
	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)getpid());
	fp = fopen(tmpfile,"w");
	if (!fp)
	{
		LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		return REDIS_ERR;
	}

	rioInitWithFile(&rdb,fp);
	if (rioWrite(&rdb,buf,len) == 0)
	{
		return REDIS_ERR;
	}

	if (::fflush(fp) == EOF) return REDIS_ERR;
	if (::fsync(fileno(fp)) == -1) return REDIS_ERR;
	if (::fclose(fp) == EOF) return REDIS_ERR;
	if (::rename(tmpfile,filename) == -1) return REDIS_ERR;
	
	return REDIS_OK;
}

void xRdb::startLoading(FILE *fp)
{
	struct stat sb;
	if (fstat(fileno(fp),&sb) == -1) assert(false);

	LOG_INFO<<"dump.rdb file size "<<sb.st_size;
}


/* This is just a wrapper for the low level function rioRead() that will
 * automatically abort if it is not possible to read the specified amount
 * of bytes. */
void xRdb::rdbLoadRaw(xRio *rdb,int32_t *buf,uint64_t len)
{
    if (rioRead(rdb,buf,len) == 0)
    {
        return ; /* Not reached. */
    }
}

time_t xRdb::rdbLoadTime(xRio *rdb)
{
    int32_t t32;
    rdbLoadRaw(rdb,&t32,4);
    return (time_t)t32;
}

int32_t xRdb::rdbLoadRio(xRio *rdb)
{
	uint32_t dbid;
	int32_t type,rdbver;
	char buf[1024];
	
	if (rioRead(rdb,buf,9) == 0) return REDIS_ERR;
	buf[9] = '\0';

	if (memcmp(buf,"REDIS",9) !=0)
	{
		LOG_ERROR<<"Wrong signature trying to load DB from file";
		errno = EINVAL;
		return REDIS_ERR;
	}

	rdbver = atoi(buf+5);
	if (rdbver < 1 || rdbver > REDIS_RDB_VERSION)
	{
		LOG_WARN<<"Can't handle RDB format version "<<rdbver;
		errno = EINVAL;
		return REDIS_OK;
	}

	int64_t expiretime = -1,noew = mstime();

	while(1)
	{
		if ((type = rdbLoadType(rdb)) == -1) return REDIS_ERR;
		if (type == RDB_OPCODE_EXPIRETIME)
		{
			 /* EXPIRETIME: load an expire associated with the next key
			 * to load. Note that after loading an expire we need to
			 * load the actual type, and continue. */
			expiretime = rdbLoadTime(rdb);
			expiretime *= 1000;
			continue;
		}
		else if (type == RDB_OPCODE_EXPIRETIME_MS)
		{
			/* EXPIRETIME_MS: milliseconds precision expire times introduced
			 * with RDB v3. Like EXPIRETIME but no with more precision. */
			expiretime = rdbLoadMillisecondTime(rdb);
			continue; /* Read next opcode. */
		}
		else if (type == RDB_OPCODE_EOF)
		{
			/* EOF: End of file, exit the main loop. */
			break;
		}
		else if (type == RDB_OPCODE_SELECTDB)
		{
			/* SELECTDB: Select the specified database. */
			if ((dbid = rdbLoadLen(rdb,nullptr)) == RDB_LENERR) return REDIS_ERR;
			if (dbid >= (unsigned)redis->dbnum)
			{
				LOG_WARN<<"FATAL: Data file was created with a Redis "
					"server configured to handle more than "
					"databases. Exiting "<<redis->dbnum;
				exit(1);
			}

			continue; /* Read next opcode. */
		}
		else if (type == RDB_OPCODE_RESIZEDB)
		{
			uint64_t db_size, expires_size;
			if ((db_size = rdbLoadLen(rdb,nullptr)) == RDB_LENERR) return REDIS_ERR;

			if ((expires_size = rdbLoadLen(rdb,nullptr)) == RDB_LENERR) return REDIS_ERR;

			continue;
		}
		else if (type == RDB_OPCODE_AUX)
		{
			/* AUX: generic string-string fields. Use to add state to RDB
			 * which is backward compatible. Implementations of RDB loading
			 * are requierd to skip AUX fields they don't understand.
			 *
			 * An AUX field is composed of two strings: key and value. */
			rObj *auxkey,*auxval;
			if ((auxkey = rdbLoadStringObject(rdb)) == nullptr) return REDIS_ERR;
			if ((auxval = rdbLoadStringObject(rdb)) == nullptr) return REDIS_ERR;

			if (((char*)auxkey->ptr)[0] == '%')
			{
				/* All the fields with a name staring with '%' are considered
				 * information fields and are logged at startup with a log
				 * level of NOTICE. */
				LOG_WARN<<"RDB "<<(char*)auxkey->ptr<<" " <<(char*)auxval->ptr;
			}

			zfree(auxkey);
			zfree(auxval);
			continue; /* Read type again. */
		}
		else if (type == REDIS_STRING)
		{
			if (rdbLoadString(rdb,type) != REDIS_OK ) return REDIS_ERR;
		}
		else if ( type == REDIS_EXPIRE)
		{
			if (rdbLoadExpire(rdb,type) != REDIS_OK) return REDIS_ERR;
		}
		else if (type == REDIS_HASH)
		{
			if (rdbLoadHash(rdb,type) != REDIS_OK) return REDIS_ERR;
		}
		else if (type == REDIS_LIST)
		{
			if (rdbLoadList(rdb,type) != REDIS_OK) return REDIS_ERR;
		}
		else if (type == REDIS_SET)
		{
			if (rdbLoadSet(rdb,type) != REDIS_OK) return REDIS_ERR;
		}
		expiretime = -1;
	}

	uint64_t cksum;
	uint64_t expected = rdb->cksum;
	if (rioRead(rdb,&cksum,8) == 0) return REDIS_ERR;

	memrev64ifbe(&cksum);
	if (cksum == 0)
	{
		LOG_WARN<<"RDB file was saved with checksum disabled: no check performed";
		return REDIS_ERR;
	}
	else if (cksum != expected)
	{
		LOG_WARN<<"Wrong RDB checksum. Aborting now";
		return REDIS_ERR;
	}

	return REDIS_OK;
}

int32_t xRdb::rdbLoad(char *filename)
{
	FILE *fp;
	xRio rdb;

	if ((fp = ::fopen(filename,"r")) == nullptr) return REDIS_ERR;

	startLoading(fp);
	rioInitWithFile(&rdb,fp);
	fclose(fp);
	return REDIS_OK;
}

int32_t xRdb::rdbLoadType(xRio *rdb)
{
    unsigned char type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}

uint32_t xRdb::rdbLoadUType(xRio *rdb) 
{
    uint32_t  type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}

int32_t xRdb::rdbSaveLen(xRio *rdb, uint32_t len)
{
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1<<6))
    {
        buf[0] = (len&0xFF)|(REDIS_RDB_6BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;
    }
    else if (len < (1<<14))
    {
        buf[0] = ((len>>8)&0xFF)|(REDIS_RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    }
    else
    {
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonl(len);
        if (rdbWriteRaw(rdb,&len,4) == -1) return -1;
        nwritten = 1+4;
    }
    return nwritten;
}

int32_t xRdb::rdbSaveLzfStringObject(xRio *rdb,uint8_t *s, size_t len)
{
	size_t comprlen,outlen;
	unsigned char byte;
	int32_t n,nwritten = 0;
	void *out;

	if (len <=4 ) return 0;
	outlen = len- 4;
	if ((out = zmalloc(outlen + 1)) == nullptr) return 0;
	comprlen = lzf_compress(s,len,out,outlen);
	if (comprlen == 0)
	{
		zfree(out);
		return 0;
	}

    byte = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) return REDIS_ERR;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,comprlen)) == -1) return REDIS_ERR;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,len)) == -1) return REDIS_ERR;
    nwritten += n;

    if ((n = rdbWriteRaw(rdb,out,comprlen)) == -1) return REDIS_ERR;
    nwritten += n;

    zfree(out);
    return nwritten;
	
}

size_t xRdb::rdbSaveRawString(xRio *rdb, const  char *s, size_t len)
{
	int32_t enclen;
	int32_t n,nwritten = 0;

	if ( len > 20)
	{
		n = rdbSaveLzfStringObject(rdb, (unsigned char*)s,len);
		if (n == -1) return -1;
      	if (n > 0) return n;
	}
	
	if ((n = rdbSaveLen(rdb,len) == -1))
	{
		return -1;
	}

	nwritten += n;
	if (len > 0)
	{
		if (rdbWriteRaw(rdb,(void*)s,len) == -1) 
		{
			return -1;	
		}
       	nwritten += len;
	}
	return nwritten;
}

int32_t xRdb::rdbLoadBinaryDoubleValue(xRio *rdb,double *val)
{
	if (rioRead(rdb,val,sizeof(*val)) == 0) return -1;
   	 memrev64ifbe(val);
}

int64_t xRdb::rdbLoadMillisecondTime(xRio *rdb)
{
	int64_t t64;
	if (rioRead(rdb,&t64,8) == 0) return -1;
	return t64;
}

rObj * xRdb::rdbLoadObject(int32_t rdbtype,xRio *rdb)
{
	rObj *o = nullptr;
	size_t len;
	uint32_t i;
	if (rdbtype == REDIS_RDB_TYPE_STRING)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr) return nullptr;
	}
	else if (rdbtype == REDIS_RDB_TYPE_LIST)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr) return nullptr;
	}
	else if (rdbtype == REDIS_RDB_TYPE_SET)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr) return nullptr;
	}
	else if (rdbtype == REDIS_RDB_TYPE_ZSET)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr) return nullptr;
	}
	else if (rdbtype == REDIS_RDB_TYPE_HASH)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr) return nullptr;
	}
	else if (rdbtype == REDIS_RDB_TYPE_EXPIRE)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr) return nullptr;
	}
	else
	{
		LOG_WARN<<"rdbLoadObject type error";
	}
	return o;
}

ssize_t xRdb::rdbSaveLongLongAsStringObject(xRio *rdb,int64_t value)
{
	unsigned char buf[32];
    ssize_t n,nwritten = 0;
    int enclen = rdbEncodeInteger(value,buf);
    if (enclen > 0)
    {
        return rdbWriteRaw(rdb,buf,enclen);
    } 
    else 
    {
        /* Encode as string */
        enclen = ll2string((char*)buf,32,value);
        if ((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n;
        if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

int32_t xRdb::rdbSaveStringObject(xRio *rdb,rObj *obj)
{
	if (obj->encoding == OBJ_ENCODING_INT) 
	{
       return rdbSaveLongLongAsStringObject(rdb,*(int32_t*)obj->ptr);
    }
    else 
    {
       return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
    }
}

int32_t xRdb::rdbSaveType(xRio *rdb,uint8_t type)
{
    return rdbWriteRaw(rdb,&type,1);
}

int32_t xRdb::rdbSaveObjectType(xRio *rdb,rObj *o)
{
	switch(o->type)
	{
		case REDIS_STRING:
		{
			return rdbSaveType(rdb,REDIS_RDB_TYPE_STRING);
		}
		case REDIS_LIST:
		{
			return rdbSaveType(rdb,REDIS_RDB_TYPE_LIST);
		}
		
		case REDIS_SET:
		{
			return rdbSaveType(rdb,REDIS_RDB_TYPE_SET);
		}
		
		case REDIS_ZSET:
		{
			return rdbSaveType(rdb,REDIS_RDB_TYPE_ZSET);
		}
		
		case REDIS_HASH:
		{
			return rdbSaveType(rdb,REDIS_RDB_TYPE_HASH);
		}
		case REDIS_EXPIRE:
		{
			return rdbSaveType(rdb,REDIS_RDB_TYPE_EXPIRE);
		}
		default:
        {
			LOG_WARN<<"Unknown object type";
		}
		
	}
	
	return  -1;
}	

int32_t xRdb::rdbSaveObject(xRio *rdb,rObj *o)
{
	int32_t n, nwritten = 0;

	if (o->type == OBJ_STRING)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
	} 
	else if (o->type == OBJ_LIST)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
	}
	else if (o->type == OBJ_ZSET)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
	}
	else if (o->type == OBJ_HASH)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
	}
	else if (o->type == OBJ_SET)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
	}
	else
	{
		LOG_ERROR<<"unknown type "<<o->type;
		return -1;
	}

    return nwritten;
}

int32_t xRdb::rdbSaveKeyValuePair(xRio *rdb,rObj *key,rObj *val,int64_t expiretime,int64_t now)
{
	 if (expiretime != -1) 
	 {
        /* If this key is already expired skip it */
        if (expiretime < now) return 0;
        if (rdbSaveType(rdb,RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if (rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
    }

	if (rdbSaveObjectType(rdb,key) == -1) return -1;
	if (rdbSaveStringObject(rdb,key) == -1) return -1;
	if (rdbSaveObject(rdb,val) == -1) return -1;
	
	return 1;
}

int32_t xRdb::rdbSaveValue(xRio *rdb,rObj *value)
{
	if (rdbSaveObjectType(rdb,value) == -1) return -1;
	if (rdbSaveStringObject(rdb,value) == -1) return -1;

	return 1;
}

int32_t xRdb::rdbSaveMillisecondTime(xRio *rdb,int64_t t)
{
	int64_t t64 = (int64_t)t;
	return rdbWriteRaw(rdb, &t64, 8);
}

int32_t xRdb::rdbSaveKey(xRio *rdb,rObj *key)
{
	if (rdbSaveObjectType(rdb,key) == -1) return -1;
	if (rdbSaveStringObject(rdb,key) == -1) return -1;
	
	return 1;
}

int32_t xRdb::rdbWriteRaw(xRio *rdb,void *p,size_t len)
{
	if (rdb && rioWrite(rdb,p,len) == 0) return -1;
	return len;
}


/* Produces a dump of the database in RDB format sending it to the specified
 * Redis I/O channel. On success C_OK is returned, otherwise C_ERR
 * is returned and part of the output, or all the output, can be
 * missing because of I/O errors.
 *
 * When the function returns C_ERR and if 'error' is not NULL, the
 * integer pointed by 'error' is set to the value of errno just after the I/O
 * error. */

int32_t xRdb::rdbSaveRio(xRio *rdb,int32_t *error,int32_t flags)
{
	char magic[10];
	int64_t now = time(0);
	uint64_t cksum;
	snprintf(magic,sizeof(magic),"REDIS%04d",REDIS_RDB_VERSION);

	if (rdbWriteRaw(rdb,magic,9) == -1) goto werr;
	if (rdbSaveInfoAuxFields(rdb,flags) == -1) goto werr;

	for (int i = 0; i < redis->dbnum; i++)
	{
		if (rdbSaveType(rdb,RDB_OPCODE_SELECTDB) == -1) goto werr;
		if (rdbSaveLen(rdb,i) == -1) goto werr;

		uint32_t dbSize,expireSize;
		dbSize = redis->getDbsize();
		expireSize = redis->getExpireSize();

		if (rdbSaveType(rdb,RDB_OPCODE_RESIZEDB) == -1) goto werr;
        if (rdbSaveLen(rdb,dbSize) == -1) goto werr;
        if (rdbSaveLen(rdb,expireSize) == -1) goto werr;
        if (rdbSaveStruct(rdb) == REDIS_ERR) goto werr;
	}

	if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) goto werr;
	
	cksum = rdb->cksum;
	memrev64ifbe(&cksum);

	if (rioWrite(rdb,&cksum,8) == 0) goto werr;
	return REDIS_OK;
werr:
	if (error) *error = errno;
	return REDIS_ERR;
}

int32_t xRdb::rdbSave(char *filename)
{
	char tmpfile[256];
	FILE *fp;
	xRio rdb;
	int32_t error;

	snprintf(tmpfile,256,"temp-%d.rdb",getpid());
	fp = ::fopen(tmpfile,"w");
	if (!fp)
	{
		LOG_TRACE<<"Failed opening rdb for saving:"<<strerror(errno);
		return REDIS_ERR;
	}

	rioInitWithFile(&rdb,fp);
	if (rdbSaveRio(&rdb,&error,RDB_SAVE_NONE) == REDIS_ERR) goto werr;
	

	if (::fflush(fp) == EOF) goto werr;
	if (::fsync(fileno(fp)) == -1) goto werr;
	if (::fclose(fp) == EOF) goto werr;

	if (::rename(tmpfile,filename) == -1)
	{
		LOG_TRACE<<"Error moving temp DB file  on the final:"<<strerror(errno);
		unlink(tmpfile);
		return REDIS_ERR;
	}

	return REDIS_OK;
	
werr:
	LOG_WARN<<"Write error saving DB on disk:" <<strerror(errno);
	fclose(fp);
	unlink(tmpfile);
	return REDIS_ERR;
}


/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
ssize_t xRdb::rdbSaveRawString(xRio *rdb,uint8_t *s,size_t len) 
{
    int32_t enclen;
    ssize_t n,nwritten = 0;

    /* Try integer encoding */
    if (len <= 11)
    {
        uint8_t buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) 
        {
            if (rdbWriteRaw(rdb,buf,enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (len > 20)
    {
        n = rdbSaveLzfStringObject(rdb,s,len);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    if ((n = rdbSaveLen(rdb,len)) == -1) return -1;
    	nwritten += n;
    if (len > 0) 
    {
        if (rdbWriteRaw(rdb,s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* Save an AUX field. */
ssize_t xRdb::rdbSaveAuxField(xRio *rdb,char *key,size_t keylen,char *val,size_t vallen)
{
    ssize_t ret,len = 0;
    if ((ret = rdbSaveType(rdb,RDB_OPCODE_AUX)) == -1) return -1;
    len += ret;
    if ((ret = rdbSaveRawString(rdb,key,keylen)) == -1) return -1;
    len += ret;
    if ((ret = rdbSaveRawString(rdb,val,vallen)) == -1) return -1;
    len += ret;
    return len;
}

/* Wrapper for rdbSaveAuxField() used when key/val length can be obtained
 * with strlen(). */
ssize_t xRdb::rdbSaveAuxFieldStrStr(xRio *rdb,char *key,char *val)
{
    return rdbSaveAuxField(rdb,key,strlen(key),val,strlen(val));
}

/* Wrapper for strlen(key) + integer type (up to long long range). */
ssize_t xRdb::rdbSaveAuxFieldStrInt(xRio *rdb,char *key,int64_t val) 
{
    char buf[LONG_STR_SIZE];
    int32_t vlen = ll2string(buf,sizeof(buf),val);
    return rdbSaveAuxField(rdb,key,strlen(key),buf,vlen);
}

int32_t xRdb::rdbSaveInfoAuxFields(xRio *rdb,int32_t flags)
{
	int32_t redisBits = (sizeof(void*) == 8) ? 64 : 32;
    int32_t aofPreamble = (flags & RDB_SAVE_AOF_PREAMBLE) != 0;

    /* Add a few fields about the state when the RDB was created. */
    if (rdbSaveAuxFieldStrStr(rdb,"redis-ver",(char*)REDIS_RDB_VERSION) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"redis-bits",redisBits) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"ctime",time(0)) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"used-mem",zmalloc_used_memory()) == -1) return -1;

    if (rdbSaveAuxFieldStrInt(rdb,"aof-preamble",aofPreamble) == -1) return -1;
    return 1;
}




