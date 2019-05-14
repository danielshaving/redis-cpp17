#include "redishash.h"
#include "redisdb.h"

RedisHash::RedisHash(RedisDB* redis, const Options& options, const std::string& path)
	:redis(redis),
	db(new DB(options, path)) {

}

RedisHash::~RedisHash() {

}

Status RedisHash::Open() {
	return db->Open();
}

Status RedisHash::Hset(const std::string_view& key, 
	const std::string_view& field, const std::string_view& value, int32_t* res) {
	WriteBatch batch;

	HashLock hashlock(&lockmgr, key);
	int32_t version = 0;
	uint32_t statistic = 0;
	std::string metaValue;
	Status s = db->Get(ReadOptions(), key, &metaValue);
	if (s.ok()) {
		ParsedHashesMetaValue pmetavalue(&metaValue);
		if (pmetavalue.IsStale() || pmetavalue.GetCount() == 0) {
			version = pmetavalue.InitialMetaValue();
			pmetavalue.SetCount(1);
			batch.Put(key, metaValue);
			HashesDataKey dataKey(key, version, field);
			batch.Put(dataKey.Encode(), value);
			*res = 1;
		}
		else {
			version = pmetavalue.GetVersion();
			std::string dataValue;
			HashesDataKey hashDataKey(key, version, field);
			s = db->Get(ReadOptions(), hashDataKey.Encode(), &dataValue);
			if (s.ok()) {
				*res = 0;
				if (dataValue == std::string(value.data(), value.size())) {
					return Status::OK();
				}
				else {
					batch.Put(hashDataKey.Encode(), value);
					statistic++;
				}
			}
			else if (s.IsNotFound()) {
				pmetavalue.ModifyCount(1);
				batch.Put(key, metaValue);
				batch.Put(hashDataKey.Encode(), value);
				*res = 1;
			}
			else {
				return s;
			}
		}
	}
	else if (s.IsNotFound()) {
		char str[4];
		EncodeFixed32(str, 1);
		HashesMetaValue metaValue(std::string(str, sizeof(int32_t)));
		version = metaValue.UpdateVersion();
		batch.Put(key, metaValue.Encode());
		HashesDataKey dataKey(key, version, field);
		batch.Put(dataKey.Encode(), value);
		*res = 1;
	}
	else {
		return s;
	}

	s = db->Write(WriteOptions(), &batch);
	return s;
}

Status RedisHash::Hget(const std::string_view& key, 
	const std::string_view& field, std::string* value) {
	std::string metaValue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;

	int32_t version = 0;
	Status s = db->Get(readopts, key, &metaValue);
	if (s.ok()) {
		ParsedHashesMetaValue phashmetavalue(&metaValue);
		if (phashmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (phashmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}
		else {
			version = phashmetavalue.GetVersion();
			HashesDataKey datakey(key, version, field);
			s = db->Get(readopts, datakey.Encode(), value);
		}
	}
	return s;
}

Status RedisHash::Hmset(const std::string_view& key,
	const std::vector<FieldValue>& fvs) {

}

Status RedisHash::Hmget(const std::string_view& key,
	const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {

}

Status RedisHash::Hgetall(const std::string_view& key,
	std::vector<FieldValue>* fvs) {
	std::string metaValue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;

	int32_t version = 0;
	Status s = db->Get(readopts, key, &metaValue);
	if (s.ok()) {
		ParsedHashesMetaValue phashmedatavalue(&metaValue);
		if (phashmedatavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (phashmedatavalue.GetVersion() == 0) {
			return Status::NotFound("");
		}
		else {
			version = phashmedatavalue.GetVersion();
			HashesDataKey hdatakey(key, version, "");
			std::string_view prefix = hdatakey.Encode();
			auto iter = db->NewIterator(ReadOptions());
			for (iter->Seek(prefix); iter->Valid() && 
				StartsWith(iter->key(), prefix); iter->Next()) {
				ParsedDataKey pdatakey(iter->key());
				fvs->push_back({ pdatakey.GetDataToString(), 
					std::string(iter->value().data(), iter->value().size()) });
			}
		}
	}
	return s;
}
