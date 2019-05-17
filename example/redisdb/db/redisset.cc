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