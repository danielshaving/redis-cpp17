#include "xRdb.h"
#include "xRedis.h"

xRio::xRio()
{

}

xRio::~xRio()
{

}

xRdb::xRdb(xRedis *redis)
:redis(redis)
{

}

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
		if(r->updateFuc)
		{
			r->updateFuc(r,buf,bytesToWrite);
		}

		if(r->writeFuc(r,buf,bytesToWrite) == 0)
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
	if(readBytes == 0)
	{
		return 0;
	}

	if(r->updateFuc)
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
		if(r->readFuc(r,buf,bytesToRead) == 0)
		{
			return 0;
		}

		if(r->updateFuc)
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

	 if(r->io.file.autosync && r->io.file.buffered >= r->io.file.autosync)
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

void xRdb::rioInitWithBuffer(xRio *r, sds s)
{
	r->readFuc = std::bind(&xRdb::rioBufferRead,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->writeFuc = std::bind(&xRdb::rioBufferWrite,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->tellFuc = std::bind(&xRdb::rioBufferTell,this,std::placeholders::_1);
	r->flushFuc = std::bind(&xRdb::rioBufferFlush,this,std::placeholders::_1);
	r->io.buffer.ptr = s;
	r->io.buffer.pos = 0;
}

void xRdb::rioInitWithFile(xRio *r, FILE *fp)
{
	r->readFuc = std::bind(&xRdb::rioFileRead,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->writeFuc = std::bind(&xRdb::rioFileWrite,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->tellFuc = std::bind(&xRdb::rioFileTell,this,std::placeholders::_1);
	r->flushFuc = std::bind(&xRdb::rioFileFlush,this,std::placeholders::_1);
	r->updateFuc = std::bind(&xRdb::rioGenericUpdateChecksum,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3);
	r->cksum = 0;
	r->processedBytes = 0;
	r->maxProcessingChunk = 1024 *  64;
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

int32_t xRdb::rdbTryIntegerEncoding(char *s, size_t len,uint8_t *enc)
{
	int64_t value;
	char *endptr,buf[32];

	value = strtoll(s,&endptr,10);
	if(endptr[0] != '\0')
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
	if(isencoded)
	{
		*isencoded = 0;
	}

	if(rioRead(rdb,buf,1) == 0)
	{
		return REDIS_RDB_LENERR;
	}

	type = (buf[0]&0xC0)>>6;

	if(type == REDIS_RDB_ENCVAL)
	{
		if(isencoded)
		{
			*isencoded = 1;
		}
		return buf[0]&0x3F;
	}
	else if(type == REDIS_RDB_6BITLEN)
	{
		return buf[0]&0x3F;
	}
	else if(type == REDIS_RDB_14BITLEN)
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

rObj * xRdb::rdbLoadIntegerObject(xRio *rdb, int32_t enctype, int32_t encode)
{
	unsigned char enc[4];
	int64_t val;

	if(enctype == REDIS_RDB_ENC_INT8)
	{
		if(rioRead(rdb,enc,1) == 0)
		{
			return nullptr;
		}
		
		val = (signed char)enc[0];
	}
	else if(enctype == REDIS_RDB_ENC_INT16)
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
        return redis->object.createStringObjectFromLongLong(val);
    else
        return redis->object.createObject(REDIS_STRING,sdsfromlonglong(val));
}

rObj * xRdb::rdbLoadEncodedStringObject(xRio *rdb)
{
	return rdbGenericLoadStringObject(rdb,1);
}

rObj * xRdb::rdbLoadLzfStringObject(xRio *rdb)
{
    rObj * obj = nullptr;
	uint32_t len, clen;
	unsigned char *c = nullptr;
	sds val = nullptr;

	if ((clen = rdbLoadLen(rdb, nullptr)) == REDIS_RDB_LENERR) return nullptr;
	if ((len = rdbLoadLen(rdb, nullptr)) == REDIS_RDB_LENERR) return nullptr;
	if ((c = (unsigned char *)zmalloc(clen)) == nullptr) goto err;
	if ((val = sdsnewlen(nullptr, len)) == nullptr) goto err;
	if (rioRead(rdb, c, clen) == 0) goto err;
	if (lzf_decompress(c, clen, val, len) == 0) goto err;

	obj = redis->object.createStringObject(val, len);
	sdsfree(val);
	zfree(c);
	return obj;
	err:
	zfree(c);
	sdsfree(val);
	return nullptr;
}


rObj * xRdb::rdbGenericLoadStringObject(xRio *rdb, int32_t encode)
{
	int32_t isencoded;
	uint32_t len;
	rObj *o;

	len = rdbLoadLen(rdb, &isencoded);
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

	o = redis->object.createStringObject(nullptr, len);
	if (len && rioRead(rdb, (void*)o->ptr, len) == 0)
	{
		return nullptr;
	}

	return o;
}

rObj * xRdb::rdbLoadStringObject(xRio *rdb)
{
	return rdbGenericLoadStringObject(rdb, 0);
}

int32_t xRdb::rdbSaveExpre(xRio *rdb)
{
	if(blockEnabled)
	{
		redis->expireMutex.lock();
	}

	for (auto &it : redis->expireTimers)
	{
		if (rdbSaveKey(rdb, it.first) == -1)
		{
			return REDIS_ERR;
		}

		if (rdbSaveMillisecondTime(rdb, it.second->getExpiration().getMicroSecondsSinceEpoch()) == -1)
		{
			return REDIS_ERR;
		}
	}

	if(blockEnabled)
	{
		redis->expireMutex.unlock();
	}

	return REDIS_OK;
}

int32_t xRdb::rdbSaveBinaryDoubleValue(xRio *rdb, double val)
{
	 memrev64ifbe(&val);
	 return rdbWriteRaw(rdb,&val,sizeof(val));
}

int32_t xRdb::rdbSaveStruct(xRio *rdb)
{
	size_t n = 0;
	for (auto &it : redis->redisShards)
	{
		auto &map = it.redis;
		auto &mu = it.mtx;
		auto &stringMap = it.stringMap;
		auto &hashMap = it.hashMap;
		auto &listMap = it.listMap;
		auto &zsetMap = it.zsetMap;
		auto &setMap =it.setMap;
		
		if(blockEnabled)
		{
			mu.lock();
		}
	
		for (auto &iter : map)
		{
			if(iter->type == OBJ_STRING)
			{
				auto iterr = stringMap.find(iter);
#ifdef __DEBUG__
				assert(iterr != stringMap.end());
				assert(iterr->first->type == OBJ_STRING);
#endif

				if (rdbSaveKeyValuePair(rdb, iterr->first, iterr->second) == -1)
				{
					return REDIS_ERR;
				}
			}
			else if(iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
#ifdef __DEBUG__
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);
#endif
				
				if (rdbSaveKey(rdb, iterr->first) == -1)
				{
					return REDIS_ERR;
				}

				if (rdbSaveLen(rdb, iterr->second.size()) == -1)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == -1)
					{
						return REDIS_ERR;
					}
				}

			}
			else if(iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
#ifdef __DEBUG__
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);
#endif

				if (rdbSaveKey(rdb,iterr->first) == -1)
				{
				 	return REDIS_ERR;
				}
				
				if(rdbSaveLen(rdb,iterr->second.size()) == -1)
				{
					return REDIS_ERR;
				}

				for(auto &iterrr : iterr->second)
				{
					if (rdbSaveKeyValuePair(rdb,iterrr.first,iterrr.second) == -1)
					{
						return REDIS_ERR;
					}
				}
			}
			else if(iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
#ifdef __DEBUG__
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.keyMap.size() == iterr->second.sortMap.size());
#endif
				
				if (rdbSaveKey(rdb,iterr->first) == -1)
				{
				 	return REDIS_ERR;
				}
				
				if(rdbSaveLen(rdb,iterr->second.keyMap.size()) == -1)
				{
					return REDIS_ERR;
				}

				for(auto &iterrr : iterr->second.keyMap)
				{
					if (rdbSaveBinaryDoubleValue(rdb,iterrr.second) == -1)
					{
						return  REDIS_ERR;
					}

					if (rdbSaveValue(rdb, iterrr.first) == -1)
					{
						return REDIS_ERR;
					}
				}
						
			}
			else if(iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
#ifdef __DEBUG__
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);
#endif	
				if (rdbSaveKey(rdb,iterr->first) == -1)
				{
				 	return REDIS_ERR;
				}
				
				if(rdbSaveLen(rdb,iterr->second.size()) == -1)
				{
					return REDIS_ERR;
				}

				for(auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == -1)
					{
						return REDIS_ERR;
					}
				}
				
			}
			else
			{
#ifdef __DEBUG__
				LOG_ERROR<<"unkown key type "<<iter->type;
				assert(false);
#endif
			}
		}
		
		if(blockEnabled)
		{
			mu.unlock();
		}
	}

	return REDIS_OK;
}

int32_t xRdb::rdbLoadExpire(xRio *rdb,int32_t type)
{
	rObj *key;
	int64_t expire;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return  REDIS_ERR;
	}

	if ((expire = rdbLoadMillisecondTime(rdb)) == -1)
	{
		return REDIS_ERR;
	}

	int64_t curExpire = xTimeStamp::now().getMicroSecondsSinceEpoch();
	if (curExpire > expire)
	{
		key->calHash();
		key->type = OBJ_EXPIRE;
		std::unique_lock <std::mutex> lck(redis->expireMutex);
		xTimer *timer = redis->loop.runAfter((expire - curExpire) / 1000000, key, false, std::bind(&xRedis::setExpireTimeOut, redis, std::placeholders::_1));
		redis->expireTimers.insert(std::make_pair(key, timer));
	}
	else
	{
		redis->removeCommand(key);
		zfree(key);
	}
	
	return REDIS_OK;
}

int32_t xRdb::rdbLoadSet(xRio *rdb,int32_t type)
{
	std::unordered_set<rObj*,Hash,Equal> set;
	rObj *key;
	int32_t  len;

	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb, nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	set.reserve(len);
	
	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((val = rdbLoadObject(type, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_ZSET;
		val->calHash();
		
#ifdef __DEBUG__
		auto it = set.find(val);
		assert(it != set.end());
#endif
		set.insert(val);
	}

#ifdef __DEBUG__
	assert(!set.empty());
#endif

	size_t index = key->hash % redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &setMap = redis->redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = setMap.find(key);
#ifdef __DEBUG__
		assert(it == setMap.end());
#endif
		setMap.insert(std::make_pair(key,std::move(set)));
	}

	return REDIS_OK;
}

int32_t xRdb::rdbLoadZset(xRio *rdb,int32_t type)
{
	xRedis::SortSet sortSet;
	
	rObj *key;
	int32_t  rdbver;
	int32_t  len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb, nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		double socre;
		if(rdbLoadBinaryDoubleValue(rdb,&socre) == -1)
		{
			return REDIS_ERR;
		}
		
		if ((val = rdbLoadObject(type, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_ZSET;
		val->calHash();
		sortSet.sortMap.insert(std::make_pair(socre,val));
		sortSet.keyMap.insert(std::make_pair(val,socre));
	}
	
#ifdef __DEBUG__
	assert(!sortSet.sortMap.empty());
	assert(!sortSet.keyMap.empty());
#endif 

	size_t index = key->hash % redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &zsetMap = redis->redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = zsetMap.find(key);
#ifdef __DEBUG__
		assert(it == zsetMap.end());
#endif
		auto iter = map.find(key);
#ifdef __DEBUG__
		assert(iter == map.end());
#endif
		zsetMap.insert(std::make_pair(key,std::move(sortSet)));
		map.insert(key);
	}
		
	return REDIS_OK;
}

int32_t xRdb::rdbLoadList(xRio *rdb, int32_t type)
{
	std::deque<rObj*> list;
	rObj *key;
	int32_t  rdbver;
	int32_t  len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_LIST;
	key->calHash();
	
	if ((len = rdbLoadLen(rdb, nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((rdbver = rdbLoadType(rdb)) == -1)
		{
			return REDIS_ERR;
		}

		if (rdbver != REDIS_RDB_TYPE_STRING)
		{
			return REDIS_ERR;
		}

		if ((val = rdbLoadObject(rdbver, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_LIST;
		val->calHash();
		list.push_back(val);
	}

#ifdef __DEBUG__
	assert(!list.empty());
#endif 

	size_t index = key->hash% redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &listMap = redis->redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(key);
#ifdef __DEBUG__
		assert(it == listMap.end());
#endif
		auto iter = map.find(key);
#ifdef __DEBUG__
		assert(iter == map.end());
#endif
		map.insert(key);
		listMap.insert(std::make_pair(key, std::move(list)));
	}

	return REDIS_OK;
}

int32_t xRdb::rdbLoadHash(xRio *rdb,int32_t type)
{
	std::unordered_map<rObj*,rObj*,Hash,Equal> rhash;
	rObj *key,*kkey,*val;
	int32_t len,rdbver;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->calHash();
	key->type = OBJ_HASH;
	
	if ((len = rdbLoadLen(rdb,nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	for(int32_t i = 0 ; i < len; i ++)
	{
		if ((rdbver = rdbLoadType(rdb)) == -1)
		{
			return REDIS_ERR;
		}
		
		if(rdbver != REDIS_RDB_TYPE_HASH)
		{
			return REDIS_ERR;
		}

		if ((kkey = rdbLoadStringObject(rdb)) == nullptr)
		{
			return REDIS_ERR;
		}
		
		kkey->calHash();
		kkey->type = OBJ_HASH;
		
		if ((val = rdbLoadObject(rdbver,rdb)) == nullptr)
		{
			return REDIS_ERR;
		}
		
		val->calHash();
		val->type = OBJ_HASH;

		rhash.insert(std::make_pair(kkey,val));
	}
#ifdef __DEBUG__
	assert(!rhash.empty());
#endif 

	size_t index  = key->hash% redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &hashMap = redis->redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
	
		auto it = hashMap.find(key);
#ifdef __DEBUG__
		assert(it == hashMap.end());
#endif

		auto iter = map.find(key);
#ifdef __DEBUG__
		if(iter == map.end());
#endif
		hashMap.insert(std::make_pair(key,std::move(rhash)));
		map.insert(key);
	}
	
	return REDIS_OK;
}


int32_t xRdb::rdbRestoreString(rObj *key,xRio *rdb,int32_t type)
{
	rObj *val;
	if ((val = rdbLoadObject(type,rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->calHash();
	key->type = OBJ_STRING;
	val->type = OBJ_STRING;
	size_t index  = key->hash% redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &stringMap = redis->redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(key);
#ifdef _DEBUG__
		assert(it == map.end());
#endif

		auto iter = stringMap.find(key);
#ifdef _DEBUG__
		assert(iter == stringMap.end());
#endif
		map.insert(key);
		stringMap.insert(std::make_pair(key,val));
	}
	return REDIS_OK;
}

int32_t xRdb::rdbRestoreHash(rObj *key,xRio *rdb,int32_t type)
{
	std::unordered_map<rObj*,rObj*,Hash,Equal> rhash;
	rObj *kkey,*val;
	int32_t len,rdbver;
	key->calHash();
	key->type = OBJ_HASH;
	
	if ((len = rdbLoadLen(rdb,nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	for(int32_t i = 0 ; i < len; i ++)
	{
		if ((rdbver = rdbLoadType(rdb)) == -1)
		{
			return REDIS_ERR;
		}
		
		if(rdbver != REDIS_RDB_TYPE_HASH)
		{
			return REDIS_ERR;
		}

		if ((kkey = rdbLoadStringObject(rdb)) == nullptr)
		{
			return REDIS_ERR;
		}
		
		kkey->calHash();
		kkey->type = OBJ_HASH;
		
		if ((val = rdbLoadObject(rdbver,rdb)) == nullptr)
		{
			return REDIS_ERR;
		}
		
		val->calHash();
		val->type = OBJ_HASH;

		rhash.insert(std::make_pair(kkey,val));
	}
#ifdef __DEBUG__
	assert(!rhash.empty());
#endif 

	size_t index  = key->hash% redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &hashMap = redis->redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
	
		auto it = hashMap.find(key);
#ifdef __DEBUG__
		assert(it == hashMap.end());
#endif

		auto iter = map.find(key);
#ifdef __DEBUG__
		if(iter == map.end());
#endif
		hashMap.insert(std::make_pair(key,std::move(rhash)));
		map.insert(key);
	}

	return REDIS_OK;
}

int32_t xRdb::rdbRestoreList(rObj *key,xRio *rdb,int32_t type)
{
	std::deque<rObj*> list;
	int32_t  rdbver;
	int32_t  len;
	key->type = OBJ_LIST;
	key->calHash();
	
	if ((len = rdbLoadLen(rdb, nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((rdbver = rdbLoadType(rdb)) == -1)
		{
			return REDIS_ERR;
		}

		if (rdbver != REDIS_RDB_TYPE_STRING)
		{
			return REDIS_ERR;
		}

		if ((val = rdbLoadObject(rdbver, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_LIST;
		val->calHash();
		list.push_back(val);
	}

#ifdef __DEBUG__
	assert(!list.empty());
#endif 

	size_t index = key->hash% redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &listMap = redis->redisShards[index].listMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = listMap.find(key);
#ifdef __DEBUG__
		assert(it == listMap.end());
#endif
		auto iter = map.find(key);
#ifdef __DEBUG__
		assert(iter == map.end());
#endif
		map.insert(key);
		listMap.insert(std::make_pair(key, std::move(list)));
	}

	return REDIS_OK;
}

int32_t xRdb::rdbRestoreZset(rObj *key,xRio *rdb,int32_t type)
{
	xRedis::SortSet sortSet;
	int32_t  rdbver;
	int32_t  len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb, nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		double socre;
		if(rdbLoadBinaryDoubleValue(rdb,&socre) == -1)
		{
			return REDIS_ERR;
		}
		
		if ((val = rdbLoadObject(type, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_ZSET;
		val->calHash();
		sortSet.sortMap.insert(std::make_pair(socre,val));
		sortSet.keyMap.insert(std::make_pair(val,socre));
	}
	
#ifdef __DEBUG__
	assert(!sortSet.sortMap.empty());
	assert(!sortSet.keyMap.empty());
#endif 

	size_t index = key->hash % redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &zsetMap = redis->redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = zsetMap.find(key);
#ifdef __DEBUG__
		assert(it == zsetMap.end());
#endif
		auto iter = map.find(key);
#ifdef __DEBUG__
		assert(iter == map.end());
#endif
		zsetMap.insert(std::make_pair(key,std::move(sortSet)));
		map.insert(key);
	}
		
	return REDIS_OK;
}

int32_t xRdb::rdbRestoreSet(rObj *key,xRio *rdb,int32_t type)
{
	std::unordered_set<rObj*,Hash,Equal> set;
	int32_t  len;
	key->type = OBJ_ZSET;
	key->calHash();

	if ((len = rdbLoadLen(rdb, nullptr)) == -1)
	{
		return REDIS_ERR;
	}

	set.reserve(len);
	
	for (int32_t i = 0; i < len; i++)
	{
		rObj *val;
		if ((val = rdbLoadObject(type, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_ZSET;
		val->calHash();
		
#ifdef __DEBUG__
		auto it = set.find(val);
		assert(it != set.end());
#endif
		set.insert(val);
	}

#ifdef __DEBUG__
	assert(!set.empty());
#endif

	size_t index = key->hash % redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &setMap = redis->redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = setMap.find(key);
#ifdef __DEBUG__
		assert(it == setMap.end());
#endif
		setMap.insert(std::make_pair(key,std::move(set)));
	}
	
	return REDIS_OK;
}

int32_t xRdb::rdbRestoreExpire(rObj *key,xRio *rdb,int32_t type)
{
	int64_t expire;
	if ((expire = rdbLoadMillisecondTime(rdb)) == -1)
	{
		return REDIS_ERR;
	}

	int64_t curExpire = xTimeStamp::now().getMicroSecondsSinceEpoch();
	if (curExpire > expire)
	{
		key->calHash();
		key->type = OBJ_EXPIRE;
		std::unique_lock <std::mutex> lck(redis->expireMutex);
		xTimer *timer = redis->loop.runAfter((expire - curExpire) / 1000000,
				key, false, std::bind(&xRedis::setExpireTimeOut, redis, std::placeholders::_1));
		redis->expireTimers.insert(std::make_pair(key, timer));
	}
	else
	{
		redis->removeCommand(key);
		zfree(key);
	}
	
	return REDIS_OK;
}

int32_t xRdb::rdbLoadString(xRio *rdb,int32_t type)
{
	rObj *key,*val;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return  REDIS_ERR;
	}

	if ((val = rdbLoadObject(type,rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->calHash();
	key->type = OBJ_STRING;
	val->type = OBJ_STRING;
	size_t index  = key->hash% redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &stringMap = redis->redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(key);
#ifdef _DEBUG__
		assert(it == map.end());
#endif

		auto iter = stringMap.find(key);
#ifdef _DEBUG__
		assert(iter == stringMap.end());
#endif
		map.insert(key);
		stringMap.insert(std::make_pair(key,val));
	}
	return REDIS_OK;
}

bool   xRdb::rdbReplication(char *filename,const TcpConnectionPtr &conn)
{
	xRio rdb;
	FILE *fp ;
	if((fp = fopen(filename,"r")) == nullptr)
	{
		return false;
	}
	
	rioInitWithFile(&rdb,fp);
	struct stat sb;
	if(fstat(fileno(fp),&sb) == -1)
	{
		return false;
	}
	else
	{
		LOG_INFO<<"load dump.rdb size :"<<sb.st_size;
	}

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
	if (n < 0)
	{
		return false;
	}

	size_t sendSize;
	off_t offset = 0;
	ssize_t nwrote = 0;
	while(sendBytes)
	{
			if(sendBytes <  len)
			{
				len = sendBytes;
			}
#ifdef __linux__
			nwrote = ::sendfile(conn->getSockfd(),fd,&offset,len);
#endif

			if(nwrote >=0)
			{
				sendBytes -=nwrote;
			}
			else
			{
				//FIXME
			}
	}

	fclose(fp);
	return true;
}

int32_t  xRdb::closeFile(FILE *fp)
{
	if (fclose(fp) == EOF)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

FILE * xRdb::createFile()
{
	FILE *fp ;
	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)getpid());
	fp = fopen(tmpfile,"w");
	if(!fp)
	{
		 LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		 return nullptr;
	}
	
	return fp;	
}

int32_t xRdb::rdbSyncWrite(const char *buf,FILE * fp,size_t len)
{
	xRio rdb;
	rioInitWithFile(&rdb,fp);
	if(rioWrite(&rdb,buf,len) == 0)
	{
		return REDIS_ERR;
	}
}


int  xRdb::createDumpPayload(xRio *rdb,rObj *obj)
{
	size_t index = obj->hash% redis->kShards;
	auto &mu = redis->redisShards[index].mtx;
	auto &map = redis->redisShards[index].redis;
	auto &stringMap = redis->redisShards[index].stringMap;
	auto &hashMap = redis->redisShards[index].hashMap;
	auto &listMap = redis->redisShards[index].listMap;
	auto &zsetMap = redis->redisShards[index].zsetMap;
	auto &setMap = redis->redisShards[index].setMap;
	
	{
		std::unique_lock <std::mutex> lck(mu);
		auto iter = map.find(obj);
		if(iter != map.end())
		{
			if((*iter)->type == OBJ_STRING)
			{
				auto iterr = stringMap.find(obj);
#ifdef __DEBUG__
				assert(iterr != stringMap.end());
				assert(iterr->first->type == OBJ_STRING);
#endif
				if (rdbSaveValue(rdb,iterr->second) == -1)
				{
					return REDIS_ERR;
				}
			}
			else if((*iter)->type == OBJ_LIST)
			{
				auto iterr = listMap.find(obj);
#ifdef __DEBUG__
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);
#endif
				if (rdbSaveLen(rdb, iterr->second.size()) == -1)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == -1)
					{
						return REDIS_ERR;
					}
				}
			}
			else if((*iter)->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(obj);
#ifdef __DEBUG__
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);
#endif

				if(rdbSaveLen(rdb,iterr->second.size()) == -1)
				{
					return REDIS_ERR;
				}

				for(auto &iterrr : iterr->second)
				{
					if (rdbSaveKeyValuePair(rdb,iterrr.first,iterrr.second) == -1)
					{
						return REDIS_ERR;
					}
				}
			}
			else if((*iter)->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(obj);
#ifdef __DEBUG__
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.keyMap.size() == iterr->second.sortMap.size());
#endif
				if(rdbSaveLen(rdb,iterr->second.keyMap.size()) == -1)
				{
					return REDIS_ERR;
				}

				for(auto &iterrr : iterr->second.keyMap)
				{
					if (rdbSaveBinaryDoubleValue(rdb,iterrr.second) == -1)
					{
						return  REDIS_ERR;
					}

					if (rdbSaveValue(rdb, iterrr.first) == -1)
					{
						return REDIS_ERR;
					}
				}
			}
			else if((*iter)->type == OBJ_SET)
			{
				auto iterr = setMap.find(obj);
#ifdef __DEBUG__
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);
#endif	
				if(rdbSaveLen(rdb,iterr->second.size()) == -1)
				{
					return REDIS_ERR;
				}

				for(auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == -1)
					{
						return REDIS_ERR;
					}
				}
			}
			else
			{
				LOG_ERROR<<"unkown key type "<<(*iter)->type;
				assert(false);
			}
		}
	}

	if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1)
	{
		return REDIS_ERR;
	}

	return REDIS_OK;
}


int  xRdb::verifyDumpPayload(xRio *rdb,rObj *obj)
{	
	int type;
	while(1)
	{
		if ((type = rdbLoadType(rdb)) == -1)
		{
			return  REDIS_ERR;
		}

		if(type == RDB_OPCODE_EOF)
		{
			break;
		}

		switch(type)
		{
			case REDIS_STRING:
			{
				if(rdbRestoreString(obj,rdb,type) != REDIS_OK)
				{
					LOG_WARN<<"rdbRestoreString error";
					return REDIS_ERR;
				}

				break;
			}
			case REDIS_EXPIRE:
			{
				if(rdbRestoreExpire(obj,rdb,type) != REDIS_OK)
				{
					LOG_WARN<<"rdbRestoreExpire error";
					return REDIS_ERR;
				}

				break;
			}
			case REDIS_HASH:
			{
				if(rdbRestoreHash(obj,rdb,type) != REDIS_OK)
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

int32_t  xRdb::rdbSyncClose(char *fileName,FILE * fp)
{
	if (fflush(fp) == EOF) return REDIS_ERR;
	if (fsync(fileno(fp)) == -1) return REDIS_ERR;
	if (fclose(fp) == EOF) return REDIS_ERR;


	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)getpid());
	
	if(rename(tmpfile,fileName) == -1)
	{
		return REDIS_ERR;
	}

	return REDIS_OK;
}

int32_t  xRdb::rdbWrite(char *filename,const char *buf, size_t len)
{
	FILE *fp ;
	xRio rdb;
	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)getpid());
	fp = fopen(tmpfile,"w");
	if(!fp)
	{
		 LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		 return REDIS_ERR;
	}

	rioInitWithFile(&rdb,fp);
	if(rioWrite(&rdb,buf,len) == 0)
	{
		return REDIS_ERR;
	}

	if (fflush(fp) == EOF) return REDIS_ERR;
	if (fsync(fileno(fp)) == -1) return REDIS_ERR;
	if (fclose(fp) == EOF) return REDIS_ERR;

	if(rename(tmpfile,filename) == -1)
	{
		return REDIS_ERR;
	}
	
	return REDIS_OK;
}

int32_t xRdb::rdbLoad(char *filename)
{
	uint32_t dbid;
	int32_t type,rdbver;
	char buf[1024];
	FILE *fp ;
	xRio rdb;

	if((fp = fopen(filename,"r")) == nullptr)
	{
		return REDIS_ERR;
	}

	rioInitWithFile(&rdb,fp);
	if(rioRead(&rdb,buf,9) == 0)
	{
		return REDIS_ERR;
	}
	
	buf[9] = '\0';
	if(memcmp(buf,"REDIS",5) !=0)
	{
		fclose(fp);
		LOG_ERROR<<"Wrong signature trying to load DB from file";
		return REDIS_ERR;
	}

	struct stat sb;
	if(fstat(fileno(fp),&sb) == -1)
	{
		return REDIS_ERR;
	}
	else
	{
		LOG_INFO<<"dump.rdb file size "<<sb.st_size;
	}
	
	while(1)
	{
		if ((type = rdbLoadType(&rdb)) == -1)
		{
			return  REDIS_ERR;
		}

		if(type == RDB_OPCODE_EOF )
		{
			break;
		}

		switch(type)
		{
			case REDIS_STRING:
			{
				if(rdbLoadString(&rdb,type) != REDIS_OK )
				{
					LOG_WARN<<"rdbLoadString error";
					return REDIS_ERR;
				}

				break;
			}
			case REDIS_EXPIRE:
			{
				if(rdbLoadExpire(&rdb,type) != REDIS_OK)
				{
					LOG_WARN<<"rdbLoadExpire error";
					return REDIS_ERR;
				}

				break;
			}
			case REDIS_HASH:
			{
				if(rdbLoadHash(&rdb,type) != REDIS_OK)
				{
					LOG_WARN<<"rdbLoadHash error";
					return REDIS_ERR;
				}
				break;
			}
			case REDIS_LIST:
			{
				if (rdbLoadList(&rdb, type) != REDIS_OK)
				{
					LOG_WARN << "rdbLoadList error";
					return REDIS_ERR;
				}
				break;
			}
			case REDIS_SET:
			{
				if (rdbLoadSet(&rdb, type) != REDIS_OK)
				{
					LOG_WARN << "rdbLoadSet error";
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

	uint64_t cksum,expected = rdb.cksum;
	if(rioRead(&rdb,&cksum,8) == 0)
	{
		return REDIS_ERR;
	}

	memrev64ifbe(&cksum);
	if (cksum == 0)
	{
		LOG_WARN<<"RDB file was saved with checksum disabled: no check performed";
		return REDIS_ERR;
	}
	else if(cksum != expected)
	{
		LOG_WARN<<"Wrong RDB checksum. Aborting now";
		return REDIS_ERR;
	}

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

	if(len <=4 ) return 0;
	outlen = len- 4;
	if((out = zmalloc(outlen + 1)) == nullptr) return 0;
	comprlen = lzf_compress(s,len,out,outlen);
	if(comprlen == 0)
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

	if( len > 20)
	{
		n = rdbSaveLzfStringObject(rdb, (unsigned char*)s,len);
		if (n == -1) return -1;
      	if (n > 0) return n;
	}
	
	if((n = rdbSaveLen(rdb,len) == -1))
	{
		return -1;
	}

	nwritten += n;
	if(len > 0)
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
	if (rioRead(rdb,val,sizeof(*val)) == 0)  return -1;
   	 memrev64ifbe(val);
}

int64_t xRdb::rdbLoadMillisecondTime(xRio *rdb)
{
	int64_t t64;
	if (rioRead(rdb, &t64, 8) == 0) return -1;
	return t64;
}

rObj * xRdb::rdbLoadObject(int32_t rdbtype, xRio *rdb)
{
	rObj *o = nullptr;
	size_t len;
	uint32_t i;
	if(rdbtype == REDIS_RDB_TYPE_STRING)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if(rdbtype == REDIS_RDB_TYPE_LIST)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if(rdbtype == REDIS_RDB_TYPE_SET)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if(rdbtype == REDIS_RDB_TYPE_ZSET)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if(rdbtype == REDIS_RDB_TYPE_HASH)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if(rdbtype == REDIS_RDB_TYPE_EXPIRE)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else
	{
		LOG_WARN<<"rdbLoadObject type error";
	}
	return o;
}

int32_t xRdb::rdbSaveStringObject(xRio *rdb,rObj *obj)
{
	return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
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
	else if(o->type == OBJ_ZSET)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
	}
	else if(o->type == OBJ_HASH)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
	}
	else if(o->type == OBJ_SET)
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

int32_t xRdb::rdbSaveKeyValuePair(xRio *rdb,rObj *key,rObj *val)
{
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
	if (rdb && rioWrite(rdb,p,len) == 0)
	{ 
		 return -1;
	}

	return len;
}

int32_t xRdb::rdbSaveRio(xRio *rdb,int32_t *error)
{
	char magic[10];
	int32_t j;
	int64_t now = time(0);
	uint64_t cksum;
	snprintf(magic,sizeof(magic),"REDIS%04d",REDIS_RDB_VERSION);
	if (rdbWriteRaw(rdb,magic,9) == -1)
	{
		*error = errno;
		return REDIS_ERR;
	}

	if(rdbSaveStruct(rdb) != REDIS_OK)
	{
		return REDIS_ERR;
	}
	
	if(rdbSaveExpre(rdb) != REDIS_OK)
	{
		return REDIS_ERR;
	}

	if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1)
	{
		return REDIS_ERR;
	}

	cksum = rdb->cksum;
	memrev64ifbe(&cksum);
	if(rioWrite(rdb,&cksum,8) == 0)
	{
		*error = errno;
		return REDIS_ERR;
	}
	
	return REDIS_OK;
}

int32_t xRdb::rdbSave(char *filename)
{
	char tmpfile[256];
	FILE *fp;
	xRio rdb;
	int32_t error;

	snprintf(tmpfile,256,"temp-%d.rdb",(int32_t)getpid());
	fp = fopen(tmpfile,"w");
	if(!fp)
	{
		 LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		 return REDIS_ERR;
	}

	rioInitWithFile(&rdb,fp);
	if(rdbSaveRio(&rdb,&error) == REDIS_ERR)
	{
		errno = error;
		goto werr;
	}

	if (fflush(fp) == EOF) goto werr;
	if (fsync(fileno(fp)) == -1) goto werr;
	if (fclose(fp) == EOF) goto werr;

	if(rename(tmpfile,filename) == -1)
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
