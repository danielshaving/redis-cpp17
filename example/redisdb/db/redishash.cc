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

Status RedisHash::CompactRange(const std::string_view* begin,
	  const std::string_view* end, const ColumnFamilyType& type) {
	if (type == kMeta || type == kMetaAndData) {
		db->CompactRange(begin, end);
	}
	
	if (type == kData || type == kMetaAndData) {
		db->CompactRange(begin, end);
	}
	return Status::OK();	  
}

Status RedisHash::DestroyDB(const std::string path, const Options& options) {
	return db->DestroyDB(path, options);
}

Status RedisHash::HSet(const std::string_view& key,
	const std::string_view& field, const std::string_view& value, int32_t* res) {
	int32_t version = 0;
	uint32_t statistic = 0;
	std::string metavalue;
	WriteBatch batch;
		
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &metavalue);
	if (s.ok()) {
		ParsedHashesMetaValue phashesmetavalue(&metavalue);
		if (phashesmetavalue.IsStale() || phashesmetavalue.GetCount() == 0) {
			version = phashesmetavalue.InitialMetaValue();
			phashesmetavalue.SetCount(1);
			batch.Put(key, metavalue);
			HashesDataKey dataKey(key, version, field);
			batch.Put(dataKey.Encode(), value);
			*res = 1;
		}
		else {
			version = phashesmetavalue.GetVersion();
			std::string dataValue;
			HashesDataKey hashDataKey(key, version, field);
			s = db->Get(ReadOptions(), hashDataKey.Encode(), &dataValue);
			if (s.ok()) {
				*res = 0;
				if (dataValue == ToString(value)) {
					return Status::OK();
				}
				else {
					batch.Put(hashDataKey.Encode(), value);
					statistic++;
				}
			}
			else if (s.IsNotFound()) {
				phashesmetavalue.ModifyCount(1);
				batch.Put(key, metavalue);
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
		HashesMetaValue metavalue(std::string(str, sizeof(int32_t)));
		version = metavalue.UpdateVersion();
		batch.Put(key, metavalue.Encode());
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

Status RedisHash::HGet(const std::string_view& key,
	const std::string_view& field, std::string* value) {
	std::string metavalue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;

	int32_t version = 0;
	Status s = db->Get(readopts, key, &metavalue);
	if (s.ok()) {
		ParsedHashesMetaValue phashmetavalue(&metavalue);
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

Status RedisHash::HMSet(const std::string_view& key,
	const std::vector<FieldValue>& fvs) {
	uint32_t statistic = 0;
	std::unordered_set<std::string> fields;
	std::vector<FieldValue> filteredfvs;
	for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
		std::string field = iter->field;
		if (fields.find(field) == fields.end()) {
			fields.insert(field);
			filteredfvs.push_back(*iter);
		}
	}

	WriteBatch batch;
	HashLock l(&lockmgr, key);

	int32_t version = 0;
	std::string metavalue;
	Status s = db->Get(ReadOptions(), key, &metavalue);
	if (s.ok()) {
		ParsedHashesMetaValue phashesmetavalue(&metavalue);
		if (phashesmetavalue.IsStale()
			|| phashesmetavalue.GetCount() == 0) {
			version = phashesmetavalue.InitialMetaValue();
			phashesmetavalue.SetCount(filteredfvs.size());
			batch.Put(key, metavalue);
			for (const auto& fv : filteredfvs) {
				HashesDataKey hashesdatakey(key, version, fv.field);
				batch.Put(hashesdatakey.Encode(), fv.value);
			}
		}
		else {
			int32_t count = 0;
			std::string datavalue;
			version = phashesmetavalue.GetVersion();
			for (const auto& fv : filteredfvs) {
				HashesDataKey hashesdatakey(key, version, fv.field);
				s = db->Get(ReadOptions(), hashesdatakey.Encode(), &datavalue);
				if (s.ok()) {
					batch.Put(hashesdatakey.Encode(), fv.value);
				}
				else if (s.IsNotFound()) {
					batch.Put(hashesdatakey.Encode(), fv.value);
					count++;
				}
				else {
					return s;
				}
			}
			phashesmetavalue.ModifyCount(count);
			batch.Put(key, metavalue);
		}
	}
	else if (s.IsNotFound()) {
		char str[4];
		EncodeFixed32(str, filteredfvs.size());
		HashesMetaValue hashesmetavalue(std::string(str, sizeof(int32_t)));
		version = hashesmetavalue.UpdateVersion();
		batch.Put(key, hashesmetavalue.Encode());
		for (const auto& fv : filteredfvs) {
			HashesDataKey hashesdatakey(key, version, fv.field);
			batch.Put(hashesdatakey.Encode(), fv.value);
		}
	}
	s = db->Write(WriteOptions(), &batch);
	return s;
}

Status RedisHash::HMGet(const std::string_view& key,
	const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {
	vss->clear();
	int32_t version = 0;
	bool isstable = false;
	std::string value;
	std::string metavalue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;

	Status s = db->Get(readopts, key, &metavalue);
	if (s.ok()) {
		ParsedHashesMetaValue phashesmetavalue(&metavalue);
		if ((isstable = phashesmetavalue.IsStale() || phashesmetavalue.GetCount() == 0)) {
			for (size_t idx = 0; idx < fields.size(); ++idx) {
				vss->push_back({ std::string(), Status::NotFound("") });
			}
			return Status::NotFound(isstable ? "Stale" : "");
		}
		else {
			version = phashesmetavalue.GetVersion();
			for (const auto& field : fields) {
				HashesDataKey hashesdatakey(key, version, field);
				s = db->Get(readopts,
					hashesdatakey.Encode(), &value);
				if (s.ok()) {
					vss->push_back({ value, Status::OK() });
				}
				else if (s.IsNotFound()) {
					vss->push_back({ std::string(), Status::NotFound("") });
				}
				else {
					vss->clear();
					return s;
				}
			}
		}
		return Status::OK();
	}
	else if (s.IsNotFound()) {
		for (size_t idx = 0; idx < fields.size(); ++idx) {
			vss->push_back({ std::string(), Status::NotFound("") });
		}
	}
	return s;
}

Status RedisHash::HGetall(const std::string_view& key,
	std::vector<FieldValue>* fvs) {
	std::string metavalue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;

	int32_t version = 0;
	Status s = db->Get(readopts, key, &metavalue);
	if (s.ok()) {
		ParsedHashesMetaValue phashesmetavalue(&metavalue);
		if (phashesmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (phashesmetavalue.GetVersion() == 0) {
			return Status::NotFound("");
		}
		else {
			version = phashesmetavalue.GetVersion();
			HashesDataKey hdatakey(key, version, "");
			std::string_view prefix = hdatakey.Encode();
			auto iter = db->NewIterator(ReadOptions());
			for (iter->Seek(prefix); iter->Valid() &&
				StartsWith(iter->key(), prefix); iter->Next()) {
				ParsedDataKey pdatakey(iter->key());
				fvs->push_back({ pdatakey.GetDataToString(),
					ToString(iter->value()) });
			}
		}
	}
	return s;
}

Status RedisHash::HDel(const std::string_view& key,
	const std::vector<std::string>& fields, int32_t* ret) {
	std::vector<std::string> filteredfields;
	std::unordered_set<std::string> fieldset;

	for (auto& it : fields) {
		if (fieldset.find(it) == fieldset.end()) {
			fieldset.insert(it);
			filteredfields.push_back(it);
		}
	}

	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;
	WriteBatch batch;

	std::string metavalue;
	int32_t delcnt = 0;
	int32_t version = 0;
	Status s = db->Get(readopts, key, &metavalue);
	if (s.ok()) {
		ParsedHashesMetaValue phashesmetavalue(&metavalue);
		if (phashesmetavalue.IsStale()
			|| phashesmetavalue.GetCount() == 0) {
			*ret = 0;
			return Status::OK();
		}
		else {
			std::string datavalue;
			version = phashesmetavalue.GetVersion();
			for (const auto& field : filteredfields) {
				HashesDataKey hashesdatakey(key, version, field);
				s = db->Get(readopts, hashesdatakey.Encode(), &datavalue);
				if (s.ok()) {
					delcnt++;
					batch.Delete(hashesdatakey.Encode());
				}
				else if (s.IsNotFound()) {
					continue;
				}
				else {
					return s;
				}
			}

			*ret = delcnt;
			phashesmetavalue.ModifyCount(-delcnt);
			batch.Put(key, metavalue);
		}
	}
	else if (s.IsNotFound()) {
		*ret = 0;
		return Status::OK();
	}
	else {
		return s;
	}

	s = db->Write(WriteOptions(), &batch);
	return s;
}

Status RedisHash::Expire(const std::string_view& key, int32_t ttl) {
	std::string metavalue;
	HashLock l(&lockmgr, key);
	Status s;
	return s;
}

Status RedisHash::Del(const std::string_view& key) {
	std::string metavalue;
	HashLock l(&lockmgr, key);
	WriteBatch batch;
	Status s = db->Get(ReadOptions(), key, &metavalue);
	if (s.ok()) {
		ParsedHashesMetaValue phashesmetavalue(&metavalue);
		if (phashesmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (phashesmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}
		else {
			batch.Delete(key);
			int32_t version = phashesmetavalue.GetVersion();
			HashesDataKey hdatakey(key, version, "");
			std::string_view prefix = hdatakey.Encode();
			auto iter = db->NewIterator(ReadOptions());
			for (iter->Seek(prefix); iter->Valid() &&
				StartsWith(iter->key(), prefix); iter->Next()) {
				batch.Delete(iter->key());
			}
		}
	}
	return db->Write(WriteOptions(), &batch);
}

Status RedisHash::HKeys(const std::string_view& key,
	std::vector<std::string>* fields) {

}

Status RedisHash::ScanKeyNum(KeyInfo* keyinfo) {
	uint64_t keys = 0;
	uint64_t expires = 0;
	uint64_t ttlsum = 0;
	uint64_t invaildkeys = 0;

	std::string key;
	ReadOptions iterops;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iterops.snapshot = snapshot;
	iterops.fillcache = false;
	int64_t curtime = time(0);

	std::shared_ptr<Iterator> iter = db->NewIterator(iterops);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		ParsedHashesMetaValue phashesmetavalue(iter->value());
		if (phashesmetavalue.IsStale()) {
			invaildkeys++;
		}
		else {
			if (!phashesmetavalue.IsPermanentSurvival()) {
				expires++;
				ttlsum += phashesmetavalue.GetTimestamp() - curtime;
			}
		}
	}

	keyinfo->keys = keys;
	keyinfo->expires = expires;
	keyinfo->avgttl = (expires != 0) ? ttlsum / expires : 0;
	keyinfo->invaildkeys = invaildkeys;
	return Status::OK();
}

Status RedisHash::ScanKeys(const std::string& pattern,
	std::vector<std::string>* keys) {
	std::string key;
	std::string metavalue;
	
	ReadOptions iterops;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iterops.snapshot = snapshot;
	iterops.fillcache = false;

	std::shared_ptr<Iterator> iter = db->NewIterator(iterops);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {	
		ParsedHashesMetaValue phashesmetavalue(iter->value());
		if (!phashesmetavalue.IsStale()
			&& phashesmetavalue.GetCount() != 0) {
			key = ToString(iter->key());
			if (StringMatchLen(pattern.data(),
				pattern.size(), key.data(), key.size(), 0)) {
				keys->push_back(key);
			}
		}
	}
	return Status::OK();
}

Status RedisHash::HExists(const std::string_view& key, const std::string_view& field) {
	std::string value;
	return HGet(key, field, &value);
}

