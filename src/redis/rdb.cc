#include "rdb.h"
#include "redis.h"

Rdb::Rdb(Redis *redis)
	:redis(redis),
	blockEnabled(true)
{

}

Rdb::~Rdb()
{

}

/* Returns REDIS_OK or 0 for success/failure. */
size_t Rdb::rioBufferWrite(Rio *r, const void *buf, size_t len)
{
	r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr, (char*)buf, len);
	r->io.buffer.pos += len;
	return REDIS_OK;
}

/* Returns REDIS_OK or 0 for success/failure. */
size_t Rdb::rioBufferRead(Rio *r, void *buf, size_t len)
{
	if (sdslen(r->io.buffer.ptr) - r->io.buffer.pos < len) return 0; /* not enough buffer to return len bytes. */
	memcpy(buf, r->io.buffer.ptr + r->io.buffer.pos, len);
	r->io.buffer.pos += len;
	return REDIS_OK;
}

/* Returns read/write position in buffer. */
off_t Rdb::rioBufferTell(Rio *r)
{
	return r->io.buffer.pos;
}

/* Flushes any buffer to target device if applicable. Returns REDIS_OK on success
 * and 0 on failures. */
int Rdb::rioBufferFlush(Rio *r)
{
	UNUSED(r);
	return REDIS_OK; /* Nothing to do, our write just appends to the buffer. */
}


off_t Rdb::rioTell(Rio *r)
{
	return r->tellFuc(r);
}

off_t Rdb::rioFlush(Rio *r)
{
	return r->flushFuc(r);
}

size_t Rdb::rioWrite(Rio *r, const void *buf, size_t len)
{
	while (len)
	{
		size_t bytesToWrite = (r->maxProcessingChunk &&
			r->maxProcessingChunk < len) ? r->maxProcessingChunk : len;
		if (r->updateFuc)
		{
			r->updateFuc(r, buf, bytesToWrite);
		}

		if (r->writeFuc(r, buf, bytesToWrite) == 0)
		{
			return 0;
		}

		buf = (char*)buf + bytesToWrite;
		len -= bytesToWrite;
		r->processedBytes += bytesToWrite;
	}
	return REDIS_OK;
}

size_t Rdb::rioRepliRead(Rio *r, void *buf, size_t len)
{
	::fseek(r->io.file.fp, r->processedBytes, SEEK_SET);
	size_t readBytes = ::fread(buf, 1, len, r->io.file.fp);
	if (readBytes == 0)
	{
		return 0;
	}

	if (r->updateFuc)
	{
		r->updateFuc(r, buf, readBytes);
	}

	r->processedBytes += readBytes;
	return readBytes;
}

size_t Rdb::rioRead(Rio *r, void *buf, size_t len)
{
	while (len)
	{
		size_t bytesToRead = (r->maxProcessingChunk &&
			r->maxProcessingChunk < len) ? r->maxProcessingChunk : len;
		if (r->readFuc(r, buf, bytesToRead) == 0)
		{
			return 0;
		}

		if (r->updateFuc)
		{
			r->updateFuc(r, buf, bytesToRead);
		}

		buf = (char*)buf + bytesToRead;
		len -= bytesToRead;
		r->processedBytes += bytesToRead;
	}
	return REDIS_OK;
}

size_t Rdb::rioFileRead(Rio *r, void *buf, size_t len)
{
	return ::fread(buf, len, 1, r->io.file.fp);
}

size_t Rdb::rioFileWrite(Rio *r, const void *buf, size_t len)
{
	size_t retval;
	retval = ::fwrite(buf, len, 1, r->io.file.fp);
	r->io.file.buffered += len;

	if (r->io.file.autosync && r->io.file.buffered >= r->io.file.autosync)
	{
		::fflush(r->io.file.fp);
	}
	return retval;
}

off_t Rdb::rioFileTell(Rio *r)
{
#ifdef _WIN32
	return ::ftell(r->io.file.fp);
#else
	return ::ftello(r->io.file.fp);
#endif
}

int32_t Rdb::rioFileFlush(Rio *r)
{
	return (::fflush(r->io.file.fp) == 0) ? REDIS_OK : 0;
}

void Rdb::rioGenericUpdateChecksum(Rio *r, const void *buf, size_t len)
{
	r->cksum = crc64(r->cksum, (const unsigned char*)buf, len);
}

void Rdb::rioInitWithBuffer(Rio *r, sds s)
{
	r->readFuc = std::bind(&Rdb::rioBufferRead, this,
		std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	r->writeFuc = std::bind(&Rdb::rioBufferWrite, this,
		std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	r->tellFuc = std::bind(&Rdb::rioBufferTell, this, std::placeholders::_1);
	r->flushFuc = std::bind(&Rdb::rioBufferFlush, this, std::placeholders::_1);
	r->io.buffer.ptr = s;
	r->io.buffer.pos = 0;
}

void Rdb::rioInitWithFile(Rio *r, FILE *fp)
{
	r->readFuc = std::bind(&Rdb::rioFileRead, this,
		std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	r->writeFuc = std::bind(&Rdb::rioFileWrite, this,
		std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	r->tellFuc = std::bind(&Rdb::rioFileTell, this, std::placeholders::_1);
	r->flushFuc = std::bind(&Rdb::rioFileFlush, this, std::placeholders::_1);
	r->updateFuc = std::bind(&Rdb::rioGenericUpdateChecksum, this,
		std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	r->cksum = 0;
	r->processedBytes = 0;
	r->maxProcessingChunk = 1024 * 64;
	r->io.file.fp = fp;
	r->io.file.buffered = 0;
	r->io.file.autosync = 0;
}

int32_t Rdb::rdbEncodeInteger(int64_t value, uint8_t *enc)
{
	if (value >= -(1 << 7) && value <= (1 << 7) - 1)
	{
		enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT8;
		enc[1] = value & 0xFF;
		return 2;
	}
	else if (value >= -(1 << 15) && value <= (1 << 15) - 1)
	{
		enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT16;
		enc[1] = value & 0xFF;
		enc[2] = (value >> 8) & 0xFF;
		return 3;
	}
	else if (value >= -((int64_t)1 << 31) && value <= ((int64_t)1 << 31) - 1)
	{
		enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT32;
		enc[1] = value & 0xFF;
		enc[2] = (value >> 8) & 0xFF;
		enc[3] = (value >> 16) & 0xFF;
		enc[4] = (value >> 24) & 0xFF;
		return 5;
	}
	else
	{
		return 0;
	}
}

int32_t Rdb::rdbTryIntegerEncoding(char *s, size_t len, uint8_t *enc)
{
	int64_t value;
	char *endptr, buf[32];

	value = strtoll(s, &endptr, 10);
	if (endptr[0] != '\0')
	{
		return 0;
	}

	ll2string(buf, 32, value);
	if (strlen(buf) != len || memcmp(buf, s, len))
	{
		return 0;
	}
	return rdbEncodeInteger(value, enc);
}

uint32_t Rdb::rdbLoadLen(Rio *rdb, int32_t *isencoded)
{
	unsigned char buf[2];
	uint32_t len;
	int32_t type;
	if (isencoded)
	{
		*isencoded = 0;
	}

	if (rioRead(rdb, buf, REDIS_OK) == 0)
	{
		return REDIS_RDB_LENERR;
	}
	type = (buf[0] & 0xC0) >> 6;

	if (type == REDIS_RDB_ENCVAL)
	{
		if (isencoded)
		{
			*isencoded = REDIS_OK;
		}
		return buf[0] & 0x3F;
	}
	else if (type == REDIS_RDB_6BITLEN)
	{
		return buf[0] & 0x3F;
	}
	else if (type == REDIS_RDB_14BITLEN)
	{
		if (rioRead(rdb, buf + 1, REDIS_OK) == 0)
		{
			return REDIS_RDB_LENERR;
		}
		return ((buf[0] & 0x3F) << 8) | buf[REDIS_OK];
	}
	else
	{
		if (rioRead(rdb, &len, 4) == 0)
		{
			return REDIS_RDB_LENERR;
		}
		return ntohl(len);
	}
}

RedisObjectPtr Rdb::rdbLoadIntegerObject(Rio *rdb, int32_t enctype, int32_t encode)
{
	unsigned char enc[4];
	int64_t val;

	if (enctype == REDIS_RDB_ENC_INT8)
	{
		if (rioRead(rdb, enc, REDIS_OK) == 0)
		{
			return nullptr;
		}
		val = (signed char)enc[0];
	}
	else if (enctype == REDIS_RDB_ENC_INT16)
	{
		uint16_t v;
		if (rioRead(rdb, enc, 2) == 0)
		{
			return nullptr;
		}
		v = enc[0] | (enc[REDIS_OK] << 8);
		val = (int16_t)v;
	}
	else if (enctype == REDIS_RDB_ENC_INT32)
	{
		uint32_t v;
		if (rioRead(rdb, enc, 4) == 0)
		{
			return nullptr;
		}
		v = enc[0] | (enc[REDIS_OK] << 8) | (enc[2] << 16) | (enc[3] << 24);
		val = (int32_t)v;
	}
	else
	{
		val = 0;
		assert(false);
	}

	if (encode)
	{
		return createStringObjectFromLongLong(val);
	}
	else
	{
		return createObject(REDIS_STRING, sdsfromlonglong(val));
	}
}

RedisObjectPtr Rdb::rdbLoadEncodedStringObject(Rio *rdb)
{
	return rdbGenericLoadStringObject(rdb, 1);
}

RedisObjectPtr Rdb::rdbLoadLzfStringObject(Rio *rdb)
{
	RedisObjectPtr obj = nullptr;
	uint32_t len, clen;
	unsigned char *c = nullptr;
	sds val = nullptr;

	if ((clen = rdbLoadLen(rdb, nullptr)) == REDIS_RDB_LENERR)
	{
		return nullptr;
	}

	if ((len = rdbLoadLen(rdb, nullptr)) == REDIS_RDB_LENERR)
	{
		return nullptr;
	}

	if ((c = (unsigned char *)zmalloc(clen)) == nullptr)
	{
		goto err;
	}

	if ((val = sdsnewlen(nullptr, len)) == nullptr)
	{
		goto err;
	}

	if (rioRead(rdb, c, clen) == 0)
	{
		goto err;
	}
	if (lzfDecompress(c, clen, val, len) == 0)
	{
		goto err;
	}

	obj = createStringObject(val, len);
	sdsfree(val);
	zfree(c);
	return obj;
err:
	zfree(c);
	sdsfree(val);
	return nullptr;
}

RedisObjectPtr Rdb::rdbGenericLoadStringObject(Rio *rdb, int32_t encode)
{
	int32_t isencoded;
	uint32_t len;
	RedisObjectPtr o;

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
			assert(false);
			break;
		}
	}

	if (len == REDIS_RDB_LENERR)
	{
		return nullptr;
	}

	o = createStringObject(nullptr, len);
	if (len && rioRead(rdb, (void*)o->ptr, len) == 0)
	{
		return nullptr;
	}
	return o;
}

RedisObjectPtr Rdb::rdbLoadStringObject(Rio *rdb)
{
	return rdbGenericLoadStringObject(rdb, 0);
}

int32_t Rdb::rdbSaveBinaryDoubleValue(Rio *rdb, double val)
{
	memrev64ifbe(&val);
	return rdbWriteRaw(rdb, &val, sizeof(val));
}

int32_t Rdb::rdbSaveStruct(Rio *rdb)
{
	int64_t now = mstime();
	size_t n = 0;
	auto &redisShards = redis->getRedisShards();
	for (auto &it : redisShards)
	{
		auto &mu = it.mtx;
		auto &map = it.redisMap;
		auto &stringMap = it.stringMap;
		auto &hashMap = it.hashMap;
		auto &listMap = it.listMap;
		auto &zsetMap = it.zsetMap;
		auto &setMap = it.setMap;

		if (blockEnabled) mu.lock();
		for (auto &iter : map)
		{
			int64_t expire = redis->getExpire(iter);
			if (iter->type == OBJ_STRING)
			{
				auto iterr = stringMap.find(iter);
				assert(iterr != stringMap.end());
				assert(iterr->first->type == OBJ_STRING);
				if (rdbSaveKeyValuePair(rdb, iterr->first,
					iterr->second, expire, now) == REDIS_ERR)
				{
					return REDIS_ERR;
				}
			}
			else if (iter->type == OBJ_LIST)
			{
				auto iterr = listMap.find(iter);
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);

				if (rdbSaveKey(rdb, iterr->first) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				if (rdbSaveLen(rdb, iterr->second.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else if (iter->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(iter);
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);

				if (rdbSaveKey(rdb, iterr->first) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				if (rdbSaveLen(rdb, iterr->second.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr.first) == REDIS_ERR)
					{
						return REDIS_ERR;
					}

					if (rdbSaveValue(rdb, iterrr.second) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else if (iter->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(iter);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.first.size() == iterr->second.second.size());

				if (rdbSaveKey(rdb, iterr->first) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				if (rdbSaveLen(rdb, iterr->second.first.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second.first)
				{
					if (rdbSaveBinaryDoubleValue(rdb, iterrr.second) == REDIS_ERR)
					{
						return REDIS_ERR;
					}

					if (rdbSaveValue(rdb, iterrr.first) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else if (iter->type == OBJ_SET)
			{
				auto iterr = setMap.find(iter);
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);

				if (rdbSaveKey(rdb, iterr->first) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				if (rdbSaveLen(rdb, iterr->second.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else
			{
				assert(false);
			}
		}

		if (blockEnabled) mu.unlock();
	}
	return REDIS_OK;
}

int32_t Rdb::rdbLoadSet(Rio *rdb, int32_t type)
{
	RedisObjectPtr key;
	int32_t len;

	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_SET;
	if ((len = rdbLoadLen(rdb, nullptr)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	std::unordered_set<RedisObjectPtr, Hash, Equal> set;
	for (int32_t i = 0; i < len; i++)
	{
		RedisObjectPtr val;
		if ((val = rdbLoadObject(type, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_SET;
		auto it = set.find(val);
		assert(it == set.end());
		set.insert(val);
	}

	assert(!set.empty());

	auto &redisShards = redis->getRedisShards();
	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &setMap = redisShards[index].setMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = setMap.find(key);
		assert(it == setMap.end());
		setMap.insert(std::make_pair(key, std::move(set)));
	}
	return REDIS_OK;
}

int32_t Rdb::rdbLoadZset(Rio *rdb, int32_t type)
{
	RedisObjectPtr key;
	int32_t len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_ZSET;
	if ((len = rdbLoadLen(rdb, nullptr)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	Redis::SortIndexMap indexMap;
	Redis::SortMap sortMap;
	for (int32_t i = 0; i < len; i++)
	{
		RedisObjectPtr val;
		double socre;
		if (rdbLoadBinaryDoubleValue(rdb, &socre) == REDIS_ERR)
		{
			return REDIS_ERR;
		}

		if ((val = rdbLoadObject(type, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_ZSET;
		sortMap.insert(std::make_pair(socre, val));
		indexMap.insert(std::make_pair(val, socre));
	}

	assert(!sortMap.empty());
	assert(!indexMap.empty());

	auto &redisShards = redis->getRedisShards();
	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &zsetMap = redisShards[index].zsetMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = zsetMap.find(key);
		assert(it == zsetMap.end());

		auto iter = map.find(key);
		assert(iter == map.end());

		zsetMap.insert(std::make_pair(key, std::make_pair(std::move(indexMap), std::move(sortMap))));
		map.insert(key);
	}
	return REDIS_OK;
}

int32_t Rdb::rdbLoadList(Rio *rdb, int32_t type)
{
	std::deque<RedisObjectPtr> list;
	RedisObjectPtr key;
	int32_t len;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_LIST;
	if ((len = rdbLoadLen(rdb, nullptr)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	for (int32_t i = 0; i < len; i++)
	{
		RedisObjectPtr val;
		if ((val = rdbLoadObject(type, rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_LIST;
		list.push_back(val);
	}

	assert(!list.empty());
	auto &redisShards = redis->getRedisShards();
	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
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

	return REDIS_OK;
}

int32_t Rdb::rdbLoadHash(Rio *rdb, int32_t type)
{
	RedisObjectPtr key;
	int32_t len, rdbver;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_HASH;

	std::unordered_map<RedisObjectPtr, RedisObjectPtr, Hash, Equal> rhash;
	if ((len = rdbLoadLen(rdb, nullptr)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	for (int32_t i = 0; i < len; i++)
	{
		RedisObjectPtr key, val;
		if ((key = rdbLoadStringObject(rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		key->type = OBJ_HASH;
		if ((val = rdbLoadStringObject(rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		val->type = OBJ_HASH;
		rhash.insert(std::make_pair(key, val));
	}

	assert(!rhash.empty());
	auto &redisShards = redis->getRedisShards();
	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &hashMap = redisShards[index].hashMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = hashMap.find(key);
		assert(it == hashMap.end());

		auto iter = map.find(key);
		if (iter == map.end());

		hashMap.insert(std::make_pair(key, std::move(rhash)));
		map.insert(key);
	}
	return REDIS_OK;
}

int32_t Rdb::rdbLoadString(Rio *rdb, int32_t type, int64_t expiretime, int64_t now)
{
	RedisObjectPtr key, val;
	if ((key = rdbLoadStringObject(rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	if ((val = rdbLoadObject(type, rdb)) == nullptr)
	{
		return REDIS_ERR;
	}

	key->type = OBJ_STRING;
	val->type = OBJ_STRING;
	auto &redisShards = redis->getRedisShards();
	size_t index = key->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
	auto &stringMap = redisShards[index].stringMap;
	{
		std::unique_lock <std::mutex> lck(mu);
		auto it = map.find(key);
		assert(it == map.end());

		auto iter = stringMap.find(key);
		assert(iter == stringMap.end());
		map.insert(key);
		stringMap.insert(std::make_pair(key, val));
	}

	if (now < expiretime)
	{
		RedisObjectPtr k = createStringObject(key->ptr, sdslen(key->ptr));
		k->type = OBJ_EXPIRE;
		redis->setExpire(k, (expiretime - now) / 1000);
	}
	return REDIS_OK;
}

bool Rdb::rdbReplication(char *filename, const TcpConnectionPtr &conn)
{
	Rio rdb;
	FILE *fp;
	if ((fp = ::fopen(filename, "r")) == nullptr)
	{
		return false;
	}

	rioInitWithFile(&rdb, fp);
	int32_t sendlen = startLoading(fp);

	Buffer buf;
	buf.appendInt32(sendlen);
	conn->send(&buf);

#ifdef _WIN32
	int32_t fd = ::_fileno(fp);
#else
	int32_t fd = ::fileno(fp);
#endif

	if (fd < 0)
	{
		return false;
	}

	off_t offset = 0;
	ssize_t nwrote = 0;

	while (sendlen)
	{
#ifdef __linux__
		nwrote = ::sendfile(conn->getSockfd(), fd, &offset, REDIS_SLAVE_SYNC_SIZE);
#endif
		if (nwrote >= 0)
		{
			sendlen -= nwrote;
		}
	}

	::fclose(fp);
	return true;
}

int32_t Rdb::rdbSyncWrite(const char *buf, FILE *fp, size_t len)
{
	Rio rdb;
	rioInitWithFile(&rdb, fp);
	if (rioWrite(&rdb, buf, len) == 0)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

int32_t Rdb::createDumpPayload(Rio *rdb, const RedisObjectPtr &obj)
{
	auto &redisShards = redis->getRedisShards();
	size_t index = obj->hash % redis->kShards;
	auto &mu = redisShards[index].mtx;
	auto &map = redisShards[index].redisMap;
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
				if (rdbSaveValue(rdb, iterr->second) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

			}
			else if ((*iter)->type == OBJ_LIST)
			{
				auto iterr = listMap.find(obj);
				assert(iterr != listMap.end());
				assert(iterr->first->type == OBJ_LIST);
				if (rdbSaveLen(rdb, iterr->second.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else if ((*iter)->type == OBJ_HASH)
			{
				auto iterr = hashMap.find(obj);
				assert(iterr != hashMap.end());
				assert(iterr->first->type == OBJ_HASH);
				if (rdbSaveLen(rdb, iterr->second.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveKeyValuePair(rdb, iterrr.first,
						iterrr.second, -1, -1) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else if ((*iter)->type == OBJ_ZSET)
			{
				auto iterr = zsetMap.find(obj);
				assert(iterr != zsetMap.end());
				assert(iterr->first->type == OBJ_ZSET);
				assert(iterr->second.first.size() == iterr->second.second.size());
				if (rdbSaveLen(rdb, iterr->second.first.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second.first)
				{
					if (rdbSaveBinaryDoubleValue(rdb, iterrr.second) == REDIS_ERR)
					{
						return REDIS_ERR;
					}

					if (rdbSaveValue(rdb, iterrr.first) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else if ((*iter)->type == OBJ_SET)
			{
				auto iterr = setMap.find(obj);
				assert(iterr != setMap.end());
				assert(iterr->first->type == OBJ_SET);
				if (rdbSaveLen(rdb, iterr->second.size()) == REDIS_ERR)
				{
					return REDIS_ERR;
				}

				for (auto &iterrr : iterr->second)
				{
					if (rdbSaveValue(rdb, iterrr) == REDIS_ERR)
					{
						return REDIS_ERR;
					}
				}
			}
			else
			{
				assert(false);
			}
		}
	}

	if (rdbSaveType(rdb, RDB_OPCODE_EOF) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

int32_t Rdb::verifyDumpPayload(Rio *rdb, const RedisObjectPtr &obj)
{
	return REDIS_OK;
}

int32_t Rdb::rdbSyncClose(const char *fileName, FILE *fp)
{
	if (::fflush(fp) == EOF) return REDIS_ERR;
#ifndef _WIN32
	if (::fsync(fileno(fp)) == REDIS_ERR) return REDIS_ERR;
#endif
	if (::fclose(fp) == EOF) return REDIS_ERR;

	char tmpfile[256];
	snprintf(tmpfile, 256, "temp-%d.rdb", std::this_thread::get_id());

	if (::rename(tmpfile, fileName) == REDIS_ERR) return REDIS_ERR;
	return REDIS_OK;
}

int32_t Rdb::rdbWrite(char *filename, const char *buf, size_t len)
{
	FILE *fp;
	Rio rdb;
	char tmpfile[256];
	snprintf(tmpfile, 256, "temp-%d.rdb", std::this_thread::get_id());
	fp = ::fopen(tmpfile, "w");
	if (!fp)
	{
		LOG_TRACE << "Failed opening .rdb for saving:" << strerror(errno);
		return REDIS_ERR;
	}

	rioInitWithFile(&rdb, fp);
	if (rioWrite(&rdb, buf, len) == 0)
	{
		return REDIS_ERR;
	}

	if (::fflush(fp) == EOF)
	{
		return REDIS_ERR;
	}

#ifndef _WIN32
	if (::fsync(fileno(fp)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
#endif

	if (::fclose(fp) == EOF)
	{
		return REDIS_ERR;
	}

	if (::rename(tmpfile, filename) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	return REDIS_OK;
}

int32_t Rdb::startLoading(FILE *fp)
{
	struct stat sb;
#ifdef _WIN32
	if (::fstat(::_fileno(fp), &sb) == REDIS_ERR)
#else
	if (::fstat(::fileno(fp), &sb) == REDIS_ERR)
#endif
	{
		return REDIS_ERR;
	}
	LOG_INFO << "dump.rdb file size " << sb.st_size;
	return sb.st_size;
}

/* This is just a wrapper for the low level function rioRead() that will
 * automatically abort if it is not possible to read the specified amount
 * of bytes. */
void Rdb::rdbLoadRaw(Rio *rdb, int32_t *buf, uint64_t len)
{
	if (rioRead(rdb, buf, len) == 0)
	{
		return; /* Not reached. */
	}
}

time_t Rdb::rdbLoadTime(Rio *rdb)
{
	int32_t t32;
	rdbLoadRaw(rdb, &t32, 4);
	return (time_t)t32;
}

int32_t Rdb::rdbLoadRio(Rio *rdb)
{
	uint32_t dbid;
	int32_t type, rdbver;
	char buf[1024];

	if (rioRead(rdb, buf, REDIS_RDB_VERSION) == 0)
	{
		return REDIS_ERR;
	}
	buf[9] = '\0';

	if (memcmp(buf, "REDIS", 5) != 0)
	{
		LOG_WARN << "Wrong signature trying to load DB from file";
		errno = EINVAL;
		return REDIS_ERR;
	}

	rdbver = atoi(buf + 5);
	if (rdbver < REDIS_OK || rdbver > REDIS_RDB_VERSION)
	{
		LOG_WARN << "Can't handle RDB format version " << rdbver;
		errno = EINVAL;
		return REDIS_OK;
	}

	int64_t expiretime = REDIS_ERR, now = mstime();

	while (1)
	{
		if ((type = rdbLoadType(rdb)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}

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
			if ((dbid = rdbLoadLen(rdb, nullptr)) == RDB_LENERR)
			{
				return REDIS_ERR;
			}
			if (dbid >= (unsigned)redis->dbnum)
			{
				LOG_WARN << "FATAL: Data file was created with a Redis "
					"server configured to handle more than "
					"databases. Exiting " << redis->dbnum;
				exit(REDIS_OK);
			}

			continue; /* Read next opcode. */
		}
		else if (type == RDB_OPCODE_RESIZEDB)
		{
			uint64_t dbSize, expiresSize;
			if ((dbSize = rdbLoadLen(rdb, nullptr)) == RDB_LENERR)
			{
				return REDIS_ERR;
			}

			if ((expiresSize = rdbLoadLen(rdb, nullptr)) == RDB_LENERR)
			{
				return REDIS_ERR;
			}
			continue;
		}
		else if (type == RDB_OPCODE_AUX)
		{
			/* AUX: generic string-string fields. Use to add state to RDB
			 * which is backward compatible. Implementations of RDB loading
			 * are requierd to skip AUX fields they don't understand.
			 *
			 * An AUX field is composed of two strings: key and value. */
			RedisObjectPtr auxkey, auxval;
			if ((auxkey = rdbLoadStringObject(rdb)) == nullptr)
			{
				return REDIS_ERR;
			}

			if ((auxval = rdbLoadStringObject(rdb)) == nullptr)
			{
				return REDIS_ERR;
			}

			if (((char*)auxkey->ptr)[0] == '%')
			{
				/* All the fields with a name staring with '%' are considered
				 * information fields and are logged at startup with a log
				 * level of NOTICE. */
				LOG_WARN << "RDB " << (char*)auxkey->ptr << " " << (char*)auxval->ptr;
			}
			continue; /* Read type again. */
		}
		else if (type == REDIS_STRING)
		{
			if (rdbLoadString(rdb, type, expiretime, now) == REDIS_ERR)
			{
				return REDIS_ERR;
			}
		}
		else if (type == REDIS_HASH)
		{
			if (rdbLoadHash(rdb, type) == REDIS_ERR)
			{
				return REDIS_ERR;
			}
		}
		else if (type == REDIS_LIST)
		{
			if (rdbLoadList(rdb, type) == REDIS_ERR)
			{
				return REDIS_ERR;
			}
		}
		else if (type == REDIS_SET)
		{
			if (rdbLoadSet(rdb, type) == REDIS_ERR)
			{
				return REDIS_ERR;
			}
		}
		else if (type == REDIS_ZSET)
		{
			if (rdbLoadZset(rdb, type) == REDIS_ERR)
			{
				return REDIS_ERR;
			}
		}
		else
		{
			assert(false);
		}

		expiretime = REDIS_ERR;
	}

	uint64_t cksum;
	uint64_t expected = rdb->cksum;

	if (rioRead(rdb, &cksum, 8) == 0)
	{
		return REDIS_ERR;
	}
	memrev64ifbe(&cksum);

	if (cksum == 0)
	{
		LOG_WARN << "RDB file was saved with checksum disabled: no check performed";
		return REDIS_ERR;
	}
	else if (cksum != expected)
	{
		LOG_WARN << "Wrong RDB checksum. Aborting now";
		return REDIS_ERR;
	}
	return REDIS_OK;
}

int32_t Rdb::rdbLoad(const char *filename)
{
	FILE *fp;
	Rio rdb;
	int32_t retval;
	if ((fp = ::fopen(filename, "r")) == nullptr)
	{
		return REDIS_ERR;
	}

	startLoading(fp);
	rioInitWithFile(&rdb, fp);
	retval = rdbLoadRio(&rdb);
	::fclose(fp);
	return retval;
}

int32_t Rdb::rdbLoadType(Rio *rdb)
{
	uint8_t type;
	if (rioRead(rdb, &type, REDIS_OK) == 0)
	{
		return REDIS_ERR;
	}
	return type;
}

uint32_t Rdb::rdbLoadUType(Rio *rdb)
{
	uint32_t type;
	if (rioRead(rdb, &type, REDIS_OK) == 0)
	{
		return REDIS_ERR;
	}
	return type;
}

size_t Rdb::rdbSaveLen(Rio *rdb, uint32_t len)
{
	unsigned char buf[2];
	size_t nwritten;

	if (len < (REDIS_OK << 6))
	{
		buf[0] = (len & 0xFF) | (REDIS_RDB_6BITLEN << 6);
		if (rdbWriteRaw(rdb, buf, REDIS_OK) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten = REDIS_OK;
	}
	else if (len < (REDIS_OK << 14))
	{
		buf[0] = ((len >> 8) & 0xFF) | (REDIS_RDB_14BITLEN << 6);
		buf[REDIS_OK] = len & 0xFF;
		if (rdbWriteRaw(rdb, buf, 2) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten = 2;
	}
	else
	{
		buf[0] = (REDIS_RDB_32BITLEN << 6);
		if (rdbWriteRaw(rdb, buf, REDIS_OK) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		len = htonl(len);
		if (rdbWriteRaw(rdb, &len, 4) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten = REDIS_OK + 4;
	}
	return nwritten;
}

int32_t Rdb::rdbSaveLzfStringObject(Rio *rdb, uint8_t *s, size_t len)
{
	size_t comprlen, outlen;
	unsigned char byte;
	int32_t n, nwritten = 0;
	void *out;

	if (len <= 4)
	{
		return REDIS_NULL;
	}

	outlen = len - 4;
	if ((out = zmalloc(outlen + 1)) == nullptr)
	{
		return REDIS_NULL;
	}

	comprlen = lzfCompress(s, len, out, outlen);
	if (comprlen == 0)
	{
		zfree(out);
		return REDIS_NULL;
	}

	byte = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_LZF;
	if ((n = rdbWriteRaw(rdb, &byte, REDIS_OK)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	nwritten += n;

	if ((n = rdbSaveLen(rdb, comprlen)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	nwritten += n;

	if ((n = rdbSaveLen(rdb, len)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	nwritten += n;

	if ((n = rdbWriteRaw(rdb, out, comprlen)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	nwritten += n;

	zfree(out);
	return nwritten;
}

size_t Rdb::rdbSaveRawString(Rio *rdb, const char *s, size_t len)
{
	int32_t n, nwritten = 0;

	if (len > 20)
	{
		n = rdbSaveLzfStringObject(rdb, (unsigned char*)s, len);
		if (n == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		if (n > 0)
		{
			return n;
		}
	}

	if ((n = rdbSaveLen(rdb, len) == REDIS_ERR))
	{
		return REDIS_ERR;
	}

	nwritten += n;
	if (len > 0)
	{
		if (rdbWriteRaw(rdb, (void*)s, len) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += len;
	}
	return nwritten;
}

int32_t Rdb::rdbLoadBinaryDoubleValue(Rio *rdb, double *val)
{
	if (rioRead(rdb, val, sizeof(*val)) == 0)
	{
		return REDIS_ERR;
	}
	memrev64ifbe(val);
	return REDIS_OK;
}

int64_t Rdb::rdbLoadMillisecondTime(Rio *rdb)
{
	int64_t t64;
	if (rioRead(rdb, &t64, 8) == 0)
	{
		return REDIS_ERR;
	}
	return t64;
}

RedisObjectPtr Rdb::rdbLoadObject(int32_t rdbtype, Rio *rdb)
{
	RedisObjectPtr o = nullptr;
	if (rdbtype == REDIS_RDB_TYPE_STRING)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if (rdbtype == REDIS_RDB_TYPE_LIST)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if (rdbtype == REDIS_RDB_TYPE_SET)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if (rdbtype == REDIS_RDB_TYPE_ZSET)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if (rdbtype == REDIS_RDB_TYPE_HASH)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else if (rdbtype == REDIS_RDB_TYPE_EXPIRE)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;
		}
	}
	else
	{
		assert(false);
	}
	return o;
}

ssize_t Rdb::rdbSaveLongLongAsStringObject(Rio *rdb, int64_t value)
{
	unsigned char buf[32];
	ssize_t n, nwritten = 0;
	int enclen = rdbEncodeInteger(value, buf);
	if (enclen > 0)
	{
		return rdbWriteRaw(rdb, buf, enclen);
	}
	else
	{
		/* Encode as string */
		enclen = ll2string((char*)buf, 32, value);
		if ((n = rdbSaveLen(rdb, enclen)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}

		nwritten += n;
		if ((n = rdbWriteRaw(rdb, buf, enclen)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += n;
	}
	return nwritten;
}

int32_t Rdb::rdbSaveStringObject(Rio *rdb, const RedisObjectPtr &obj)
{
	if (obj->encoding == OBJ_ENCODING_INT)
	{
		return rdbSaveLongLongAsStringObject(rdb, *(int32_t*)obj->ptr);
	}
	else
	{
		return rdbSaveRawString(rdb, obj->ptr, sdslen(obj->ptr));
	}
}

int32_t Rdb::rdbSaveType(Rio *rdb, uint8_t type)
{
	return rdbWriteRaw(rdb, &type, REDIS_OK);
}

int32_t Rdb::rdbSaveObjectType(Rio *rdb, const RedisObjectPtr &o)
{
	switch (o->type)
	{
	case REDIS_STRING:
	{
		return rdbSaveType(rdb, REDIS_RDB_TYPE_STRING);
	}
	case REDIS_LIST:
	{
		return rdbSaveType(rdb, REDIS_RDB_TYPE_LIST);
	}

	case REDIS_SET:
	{
		return rdbSaveType(rdb, REDIS_RDB_TYPE_SET);
	}

	case REDIS_ZSET:
	{
		return rdbSaveType(rdb, REDIS_RDB_TYPE_ZSET);
	}

	case REDIS_HASH:
	{
		return rdbSaveType(rdb, REDIS_RDB_TYPE_HASH);
	}
	case REDIS_EXPIRE:
	{
		return rdbSaveType(rdb, REDIS_RDB_TYPE_EXPIRE);
	}
	default:
	{
		LOG_WARN << "Unknown object type " << o->type << " " << o->ptr;
	}
	}
	return  REDIS_ERR;
}

int32_t Rdb::rdbSaveObject(Rio *rdb, const RedisObjectPtr &o)
{
	int32_t n, nwritten = 0;

	if (o->type == OBJ_STRING)
	{
		if ((n = rdbSaveStringObject(rdb, o)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += n;
	}
	else if (o->type == OBJ_LIST)
	{
		if ((n = rdbSaveStringObject(rdb, o)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += n;
	}
	else if (o->type == OBJ_ZSET)
	{
		if ((n = rdbSaveStringObject(rdb, o)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += n;
	}
	else if (o->type == OBJ_HASH)
	{
		if ((n = rdbSaveStringObject(rdb, o)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += n;
	}
	else if (o->type == OBJ_SET)
	{
		if ((n = rdbSaveStringObject(rdb, o)) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += n;
	}
	else
	{
		assert(false);
	}
	return nwritten;
}

int32_t Rdb::rdbSaveKeyValuePair(Rio *rdb, const RedisObjectPtr &key,
	const RedisObjectPtr &val, int64_t expiretime, int64_t now)
{
	if (expiretime != REDIS_ERR)
	{
		/* If this key is already expired skip it */
		if (expiretime < now)
		{
			return 0;
		}

		if (rdbSaveType(rdb, RDB_OPCODE_EXPIRETIME_MS) == REDIS_ERR)
		{
			return REDIS_ERR;
		}

		if (rdbSaveMillisecondTime(rdb, expiretime) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
	}

	if (rdbSaveObjectType(rdb, key) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	if (rdbSaveStringObject(rdb, key) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	if (rdbSaveObject(rdb, val) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

int32_t Rdb::rdbSaveValue(Rio *rdb, const RedisObjectPtr &value)
{
	if (rdbSaveStringObject(rdb, value) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

int32_t Rdb::rdbSaveMillisecondTime(Rio *rdb, int64_t t)
{
	int64_t t64 = (int64_t)t;
	return rdbWriteRaw(rdb, &t64, 8);
}

int32_t Rdb::rdbSaveKey(Rio *rdb, const RedisObjectPtr &key)
{
	if (rdbSaveObjectType(rdb, key) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	if (rdbSaveStringObject(rdb, key) == REDIS_ERR)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

int32_t Rdb::rdbWriteRaw(Rio *rdb, void *p, size_t len)
{
	if (rdb && rioWrite(rdb, p, len) == 0)
	{
		return REDIS_ERR;
	}
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

int32_t Rdb::rdbSaveRio(Rio *rdb, int32_t *error, int32_t flags)
{
	char magic[10];
	int64_t now = time(0);
	uint64_t cksum;

	snprintf(magic, sizeof(magic), "REDIS%04d", REDIS_RDB_VERSION);
	if (rdbWriteRaw(rdb, magic, 9) == REDIS_ERR)
	{
		goto werr;
	}
	if (rdbSaveInfoAuxFields(rdb, flags) == REDIS_ERR)
	{
		goto werr;
	}

	for (int i = 0; i < redis->dbnum; i++)
	{
		if (rdbSaveType(rdb, RDB_OPCODE_SELECTDB) == REDIS_ERR)
		{
			goto werr;
		}

		if (rdbSaveLen(rdb, i) == REDIS_ERR)
		{
			goto werr;
		}

		uint32_t dbSize, expireSize;
		dbSize = redis->getDbsize();
		expireSize = redis->getExpireSize();

		if (rdbSaveType(rdb, RDB_OPCODE_RESIZEDB) == REDIS_ERR)
		{
			goto werr;
		}

		if (rdbSaveLen(rdb, dbSize) == REDIS_ERR)
		{
			goto werr;
		}

		if (rdbSaveLen(rdb, expireSize) == REDIS_ERR)
		{
			goto werr;
		}

		if (rdbSaveStruct(rdb) == REDIS_ERR)
		{
			goto werr;
		}
	}

	if (rdbSaveType(rdb, RDB_OPCODE_EOF) == REDIS_ERR)
	{
		goto werr;
	}

	cksum = rdb->cksum;
	memrev64ifbe(&cksum);
	if (rioWrite(rdb, &cksum, 8) == 0)
	{
		goto werr;
	}
	return REDIS_OK;
werr:
	if (error) *error = errno;
	return REDIS_ERR;
}

int32_t Rdb::rdbSave(const char *filename)
{
	char tmpfile[256];
	FILE *fp;
	Rio rdb;
	int32_t error;
	snprintf(tmpfile, 256, "temp-%d.rdb", std::this_thread::get_id());
	fp = ::fopen(tmpfile, "w");
	if (!fp)
	{
		LOG_TRACE << "Failed opening rdb for saving:" << strerror(errno);
		return REDIS_ERR;
	}

	rioInitWithFile(&rdb, fp);
	if (rdbSaveRio(&rdb, &error, RDB_SAVE_NONE) == REDIS_ERR)
	{
		goto werr;
	}

	if (::fflush(fp) == EOF) goto werr;
#ifndef _WIN32
	if (::fsync(fileno(fp)) == REDIS_ERR) goto werr;
#endif
	if (::fclose(fp) == EOF) goto werr;
	if (::rename(tmpfile, filename) == REDIS_ERR)
	{
		LOG_TRACE << "Error moving temp DB file on the final:" << strerror(errno);
#ifdef _WIN32
		_unlink(tmpfile);
#else
		unlink(tmpfile);
#endif
		return REDIS_ERR;
	}
	return REDIS_OK;
werr:
	LOG_WARN << "Write error saving DB on disk:" << strerror(errno);
	::fclose(fp);
#ifdef _WIN32
	_unlink(tmpfile);
#else
	unlink(tmpfile);
#endif
	return REDIS_ERR;
}

/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
ssize_t Rdb::rdbSaveRawString(Rio *rdb, uint8_t *s, size_t len)
{
	int32_t enclen;
	ssize_t n, nwritten = 0;

	/* Try integer encoding */
	if (len <= 11)
	{
		uint8_t buf[5];
		if ((enclen = rdbTryIntegerEncoding((char*)s, len, buf)) > 0)
		{
			if (rdbWriteRaw(rdb, buf, enclen) == REDIS_ERR)
			{
				return REDIS_ERR;
			}
			return enclen;
		}
	}

	/* Try LZF compression - under 20 bytes it's unable to compress even
	 * aaaaaaaaaaaaaaaaaa so skip it */
	if (len > 20)
	{
		n = rdbSaveLzfStringObject(rdb, s, len);
		if (n == REDIS_ERR) return REDIS_ERR;
		if (n > 0) return n;
		/* Return value of 0 means data can't be compressed, save the old way */
	}

	/* Store verbatim */
	if ((n = rdbSaveLen(rdb, len)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	nwritten += n;
	if (len > 0)
	{
		if (rdbWriteRaw(rdb, s, len) == REDIS_ERR)
		{
			return REDIS_ERR;
		}
		nwritten += len;
	}
	return nwritten;
}

/* Save an AUX field. */
ssize_t Rdb::rdbSaveAuxField(Rio *rdb, char *key, size_t keylen, char *val, size_t vallen)
{
	ssize_t ret, len = 0;
	if ((ret = rdbSaveType(rdb, RDB_OPCODE_AUX)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	len += ret;
	if ((ret = rdbSaveRawString(rdb, key, keylen)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	len += ret;
	if ((ret = rdbSaveRawString(rdb, val, vallen)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	len += ret;
	return len;
}

/* Wrapper for rdbSaveAuxField() used when key/val length can be obtained
 * with strlen(). */
ssize_t Rdb::rdbSaveAuxFieldStrStr(Rio *rdb, char *key, char *val)
{
	return rdbSaveAuxField(rdb, key, strlen(key), val, strlen(val));
}

/* Wrapper for strlen(key) + integer type (up to long long range). */
ssize_t Rdb::rdbSaveAuxFieldStrInt(Rio *rdb, char *key, int64_t val)
{
	char buf[LONG_STR_SIZE];
	int32_t vlen = ll2string(buf, sizeof(buf), val);
	return rdbSaveAuxField(rdb, key, strlen(key), buf, vlen);
}

int32_t Rdb::rdbSaveInfoAuxFields(Rio *rdb, int32_t flags)
{
	int32_t redisBits = (sizeof(void*) == 8) ? 64 : 32;
	int32_t aofPreamble = (flags & RDB_SAVE_AOF_PREAMBLE) != 0;
	char version = REDIS_RDB_VERSION;

#ifndef _WIN32
	/* Add a few fields about the state when the RDB was created. */
	if (rdbSaveAuxFieldStrStr(rdb, "redis-ver", &version) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	if (rdbSaveAuxFieldStrInt(rdb, "redis-bits", redisBits) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	if (rdbSaveAuxFieldStrInt(rdb, "ctime", time(0)) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	if (rdbSaveAuxFieldStrInt(rdb, "used-mem", zmalloc_used_memory()) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

	if (rdbSaveAuxFieldStrInt(rdb, "aof-preamble", aofPreamble) == REDIS_ERR)
	{
		return REDIS_ERR;
	}

#endif
	return REDIS_OK;
}

/* Print informations during RDB checking. */
void Rdb::rdbCheckInfo(const char *fmt, ...)
{
	char msg[1024];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	printf("[offset %llu] %s\n",
		(uint64_t)(rdbState.rio ?
			rdbState.rio->processedBytes : 0), msg);
}

void Rdb::rdbCheckSetupSignals(void)
{

}

void Rdb::rdbCheckSetError(const char *fmt, ...)
{

}

/* RDB check main: called form redis.c when Redis is executed with the
* redis-check-rdb alias, on during RDB loading errors.
*
* The function works in two ways: can be called with argc/argv as a
* standalone executable, or called with a non NULL 'fp' argument if we
* already have an open file to check. This happens when the function
* is used to check an RDB preamble inside an AOF file.
*
* When called with fp = NULL, the function never returns, but exits with the
* status code according to success (RDB is sane) or error (RDB is corrupted).
* Otherwise if called with a non NULL fp, the function returns C_OK or
* C_ERR depending on the success or failure. */

void Rdb::checkRdb(int32_t argc, char **argv, FILE *fp)
{
	if (argc != 2 && fp == nullptr)
	{
		fprintf(stderr, "Usage: %s <rdb-file-name>\n", argv[0]);
		exit(1);
	}

	/* In order to call the loading functions we need to create the shared
	* integer objects, however since this function may be called from
	* an already initialized Redis instance, check if we really need to. */

	if (shared.integers[0] == nullptr)
	{
		createSharedObjects();
	}

	rdbCheckMode = 1;
	rdbCheckInfo("Checking RDB file %s", argv[1]);


}

