#include "redisset.h"
#include "redisdb.h"

RedisSet::RedisSet(RedisDB* redis, 
    const Options& options, const std::string& path) 
    :redis(redis),
	db(new DB(options, path)) {
}

RedisSet::~RedisSet() {

}

Status RedisSet::Open() {
    return db->Open();
}

Status RedisSet::DestroyDB(const std::string path, const Options& options) {
    return db->DestroyDB(path, options);
}

Status RedisSet::SAdd(const std::string_view& key,
        const std::vector<std::string>& members, int32_t* ret) {
    std::unordered_set<std::string> unique;
    std::vector<std::string> filteredmembers;        

    for (const auto& member : members) {
        if (unique.find(member) == unique.end()) {
            unique.insert(member);
            filteredmembers.push_back(member);
        }
    }

    WriteBatch batch;
    HashLock l(&lockmgr, key);
    int32_t version = 0;
    std::string metavalue;
    Status s = db->Get(ReadOptions(), key, &metavalue);
    if (s.ok()) {
        ParsedSetsMetaValue psetsvalue(&metavalue);
        if (psetsvalue.IsStale()  
            || psetsvalue.GetCount() == 0) {
            psetsvalue.SetCount(filteredmembers.size());
            batch.Put(key, metavalue);
            for (const auto& member : filteredmembers) {
                SetsMemberKey setsmemberkey(key, version, member);
                batch.Put(setsmemberkey.Encode(), std::string_view());
            }
            *ret = filteredmembers.size();
        }
        else {
            int32_t cnt = 0;
            std::string membervalue;
            version = psetsvalue.GetVersion();
            for (const auto& member : filteredmembers) {
                SetsMemberKey setsmemberkey(key, version, member);
                s = db->Get(ReadOptions(), setsmemberkey.Encode(), &membervalue);
                if (s.ok()) {

                } else if (s.IsNotFound()) {
                    cnt++;
                    batch.Put(setsmemberkey.Encode(), std::string_view());
                } else {
                    return s;
                }
            }

            *ret = cnt;
            if (cnt == 0) {
                return Status::OK();
            } else {
                psetsvalue.ModifyCount(cnt);
                batch.Put(key, metavalue);
            }
        }
    }
    else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, filteredmembers.size());
        SetsMetaValue setsmetavalue(std::string_view(str, sizeof(int32_t)));
        version = setsmetavalue.UpdateVersion();
        batch.Put(key, setsmetavalue.Encode());
        for (const auto& member : filteredmembers) {
            SetsMemberKey setsmemberkey(key, version, member);
            batch.Put(setsmemberkey.Encode(), std::string_view());
        }
        *ret = filteredmembers.size();
    }
    else {
        return s;
    }
     return db->Write(WriteOptions(), &batch);
}

Status RedisSet::SCard(const std::string_view& key, int32_t* ret) {
    *ret = 0;
    std::string metavalue;
    Status s = db->Get(ReadOptions(), key, &metavalue);
    if (s.ok()) {
        ParsedSetsMetaValue psetsvalue(&metavalue);
        if (psetsvalue.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            *ret = psetsvalue.GetCount();
            if (*ret == 0) {
                return Status::NotFound("Deleted");
            }
        }
    }
    return s;
}

Status RedisSet::SMembers(const std::string_view& key,
        std::vector<std::string>* members) {
    std::string metavalue;
    int32_t version = 0;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;
    Status s = db->Get(readopts, key, &metavalue);
    if (s.ok()) {   
        ParsedSetsMetaValue psetsvalue(&metavalue);
        if (psetsvalue.IsStale()) {
            return Status::NotFound("Stale");
        } else if (psetsvalue.GetCount() == 0) {
            return Status::NotFound("");
        } else {
            version = psetsvalue.GetVersion();
            SetsMemberKey setsmemberkey(key, version, std::string_view());
            std::string_view prefix = setsmemberkey.Encode();
            auto iter = db->NewIterator(ReadOptions());
            for (iter->Seek(prefix);
                iter->Valid() && StartsWith(iter->key(), prefix);
                iter->Next()) {
                ParsedDataKey pdatakey(iter->key());
                members->push_back(pdatakey.GetDataToString());
            }
        }
    }
    return s;
}

Status RedisSet::SIsmember(const std::string_view& key, 
        const std::string_view& member, int32_t* ret) {
    std::string metavalue;
    int32_t version = 0;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;

    Status s = db->Get(readopts, key, &metavalue);
    if (s.ok()) {
        
    }
    else {

    }
}

Status RedisSet::ScanKeyNum(KeyInfo* keyinfo) {
	uint64_t keys = 0;
	uint64_t expires = 0;
	uint64_t ttlsum = 0;
	uint64_t invaildkeys = 0;

	std::string key;
	ReadOptions iteratoroptions;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iteratoroptions.snapshot = snapshot;
	iteratoroptions.fillcache = false;
	int64_t curtime = time(0);

	std::shared_ptr<Iterator> iter = db->NewIterator(iteratoroptions);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		ParsedSetsMetaValue psetsmetavalue(iter->value());
		if (psetsmetavalue.IsStale()) {
			invaildkeys++;
		}
		else {
			if (!psetsmetavalue.IsPermanentSurvival()) {
				expires++;
				ttlsum += psetsmetavalue.GetTimestamp() - curtime;
			}
		}
	}

	keyinfo->keys = keys;
	keyinfo->expires = expires;
	keyinfo->avgttl = (expires != 0) ? ttlsum / expires : 0;
	keyinfo->invaildkeys = invaildkeys;
	return Status::OK();
}

Status RedisSet::ScanKeys(const std::string& pattern,
			std::vector<std::string>* keys) {
	std::string key;
	ReadOptions iteratoroptions;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iteratoroptions.snapshot = snapshot;
	iteratoroptions.fillcache = false;

	std::shared_ptr<Iterator> iter = db->NewIterator(iteratoroptions);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		ParsedSetsMetaValue psetsmetavalue(iter->value());
		if (!psetsmetavalue.IsStale()
			&& psetsmetavalue.GetCount() != 0) {
			key =ToString(iter->key());
			if (StringMatchLen(pattern.data(),
					pattern.size(), key.data(), key.size(), 0)) {
				keys->push_back(key);
			}
		}
	}
	return Status::OK();
}

Status RedisSet::Expire(const std::string_view& key, int32_t ttl) {
    Status s;
	return s;
}

Status RedisSet::Del(const std::string_view& key) {
    std::string metavalue;
    HashLock l(&lockmgr, key);
    Status s = db->Get(ReadOptions(),  key, &metavalue);
    if (s.ok()) {
        ParsedSetsMetaValue psetsmetavalue(&metavalue);
        if (psetsmetavalue.IsStale()) {
            return Status::NotFound("Stale");
        } else if (psetsmetavalue.GetCount() == 0) {
            return Status::NotFound("");
        } else {
            uint32_t statistic = psetsmetavalue.GetCount();
            psetsmetavalue.InitialMetaValue();
            s = db->Put(WriteOptions(), key, metavalue);
        }
    }
    return s;
}