#include "redishash.h"

#include "redisdb.h"

RedisHash::RedisHash(RedisDB* redis, const Options& options, const std::string& path)
	:redis(redis),
	db(new DB(options, path)) {

}

RedisHash::~RedisHash() {

}

Status RedisHash::open() {
	return db->open();
}

Status RedisHash::hset(const std::string_view& key, const std::string_view& field, const std::string_view& value, int32_t* res) {
	WriteBatch batch;

	HashLock hashlock(&lockmgr, key);
	int32_t version = 0;
	uint32_t statistic = 0;
	std::string metaValue;
	Status s = db->get(ReadOptions(), key, &metaValue);
	if (s.ok()) {
		ParsedHashesMetaValue pmetavalue(&metaValue);
		if (pmetavalue.isStale() || pmetavalue.getCount() == 0) {
			version = pmetavalue.initialMetaValue();
			pmetavalue.setCount(1);
			batch.put(key, metaValue);
			HashesDataKey dataKey(key, version, field);
			batch.put(dataKey.encode(), value);
			*res = 1;
		}
		else {
			version = pmetavalue.getVersion();
			std::string dataValue;
			HashesDataKey hashDataKey(key, version, field);
			s = db->get(ReadOptions(), hashDataKey.encode(), &dataValue);
			if (s.ok()) {
				*res = 0;
				if (dataValue == std::string(value.data(), value.size())) {
					return Status::OK();
				}
				else {
					batch.put(hashDataKey.encode(), value);
					statistic++;
				}
			}
			else if (s.isNotFound()) {
				pmetavalue.modifyCount(1);
				batch.put(key, metaValue);
				batch.put(hashDataKey.encode(), value);
				*res = 1;
			}
			else {
				return s;
			}
		}
	}
	else if (s.isNotFound()) {
		char str[4];
		encodeFixed32(str, 1);
		HashesMetaValue metaValue(std::string(str, sizeof(int32_t)));
		version = metaValue.updateVersion();
		batch.put(key, metaValue.encode());
		HashesDataKey dataKey(key, version, field);
		batch.put(dataKey.encode(), value);
		*res = 1;
	}
	else {
		return s;
	}

	s = db->write(WriteOptions(), &batch);
	return s;
}

Status RedisHash::hget(const std::string_view& key, const std::string_view& field, std::string* value) {
	std::string metaValue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;

	int32_t version = 0;
	Status s = db->get(readopts, key, &metaValue);
	if (s.ok()) {
		ParsedHashesMetaValue phashmetavalue(&metaValue);
		if (phashmetavalue.isStale()) {
			return Status::notFound("Stale");
		}
		else if (phashmetavalue.getCount() == 0) {
			return Status::notFound("");
		}
		else {
			version = phashmetavalue.getVersion();
			HashesDataKey datakey(key, version, field);
			s = db->get(readopts, datakey.encode(), value);
		}
	}
	return s;
}

Status RedisHash::hmset(const std::string_view& key, const std::vector<FieldValue>& fvs) {

}

Status RedisHash::hmget(const std::string_view& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {

}

Status RedisHash::hgetall(const std::string_view& key, std::vector<FieldValue>* fvs) {
	std::string metaValue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;

	int32_t version = 0;
	Status s = db->get(readopts, key, &metaValue);
	if (s.ok()) {
		ParsedHashesMetaValue phashmedatavalue(&metaValue);
		if (phashmedatavalue.isStale()) {
			return Status::notFound("Stale");
		}
		else if (phashmedatavalue.getVersion() == 0) {
			return Status::notFound("");
		}
		else {
			version = phashmedatavalue.getVersion();
			HashesDataKey hdatakey(key, version, "");
			std::string_view prefix = hdatakey.encode();
			auto iter = db->newIterator(ReadOptions());
			for (iter->seek(prefix); iter->valid() && StartsWith(iter->key(), prefix); iter->next()) {
				ParsedDataKey pdatakey(iter->key());
				fvs->push_back({ pdatakey.getDataToString(), std::string(iter->value().data(), iter->value().size()) });
			}
		}
	}
	return s;
}
