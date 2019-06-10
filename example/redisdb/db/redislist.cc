#include "redislist.h"

RedisList::RedisList(RedisDB* redis,
	const Options& options, const std::string& path)
	:redis(redis),
	db(new DB(options, path)) {

}

RedisList::~RedisList() {

}

Status RedisList::Open() {
	return db->Open();
}

Status RedisList::DestroyDB(const std::string path, const Options& options) {
	return db->DestroyDB(path, options);
}

Status RedisList::LPush(const std::string_view& key,
	const std::vector<std::string>& values, uint64_t* ret) {
	*ret = 0;
	ListsDataKey lkey(key, 0, 0);

	WriteBatch batch;
	HashLock l(&lockmgr, key);

	uint64_t index = 0;
	int32_t version = 0;
	std::string metavalue;
	Status s = db->Get(ReadOptions(), lkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedListsMetaValue plistsmetavalue(&metavalue);
		if (plistsmetavalue.IsStale()
			|| plistsmetavalue.GetCount() == 0) {
			version = plistsmetavalue.InitialMetaValue();
		}
		else {
			version = plistsmetavalue.GetVersion();
		}

		for (const auto& value : values) {
			index = plistsmetavalue.GetLeftIndex();
			plistsmetavalue.ModifyLeftIndex(1);
			plistsmetavalue.ModifyCount(1);
			ListsDataKey listsdatakey(key, version, index);
			batch.Put(listsdatakey.Encode(), value);
		}

		batch.Put(lkey.Encode(), metavalue);
		*ret = plistsmetavalue.GetCount();
	}
	else if (s.IsNotFound()) {
		char str[8];
		EncodeFixed64(str, values.size());
		ListsMetaValue listsvalue(std::string_view(str, sizeof(uint64_t)));
		version = listsvalue.UpdateVersion();

		for (const auto& value : values) {
			index = listsvalue.GetLeftIndex();
			listsvalue.ModifyLeftIndex(1);
			ListsDataKey listsdatakey(key, version, index);
			batch.Put(listsdatakey.Encode(), value);
		}

		batch.Put(lkey.Encode(), listsvalue.Encode());
		*ret = listsvalue.GetRightIndex() - listsvalue.GetLeftIndex() - 1;
	}
	else {
		return s;
	}
	return db->Write(WriteOptions(), &batch);
}

Status RedisList::LPop(const std::string_view& key, std::string* element) {
	uint32_t statistic = 0;
	ListsDataKey lkey(key, 0, 0);

	WriteBatch batch;
	HashLock l(&lockmgr, key);
	std::string metavalue;
	Status s = db->Get(ReadOptions(), key, &metavalue);
	if (s.ok()) {
		ParsedListsMetaValue plistsmetavalue(&metavalue);
		if (plistsmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (plistsmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}
		else {
			int32_t version = plistsmetavalue.GetVersion();
			uint64_t firstnodeindex = plistsmetavalue.GetLeftIndex() + 1;
			ListsDataKey listsdatakey(key, version, firstnodeindex);
			s = db->Get(ReadOptions(), listsdatakey.Encode(), element);
			if (s.ok()) {
				batch.Delete(listsdatakey.Encode());
				statistic++;
				plistsmetavalue.ModifyCount(-1);
				plistsmetavalue.ModifyLeftIndex(-1);
				batch.Put(lkey.Encode(), metavalue);
				s = db->Write(WriteOptions(), &batch);
				return s;
			}
			else {
				return s;
			}
		}
	}
	return s;
}

Status RedisList::LRange(const std::string_view& key, int64_t start, int64_t stop,
	std::vector<std::string>* ret) {
	ListsDataKey lkey(key, 0, 0);

	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock sl(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;

	std::string metavalue;
	Status s = db->Get(ReadOptions(), lkey.Encode() , &metavalue);
	if (s.ok()) {
		ParsedListsMetaValue plistsmetavalue(&metavalue);
		if (plistsmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (plistsmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}
		else {
			int32_t version = plistsmetavalue.GetVersion();
			uint64_t originleftindex = plistsmetavalue.GetLeftIndex() + 1;
			uint64_t originrightindex = plistsmetavalue.GetRightIndex() - 1;
			uint64_t sublistleftindex = start >= 0 ?
				originleftindex + start :
				originrightindex + start + 1;
			uint64_t sublistrightindex = stop >= 0 ?
				originleftindex + stop :
				originrightindex + stop + 1;

			if (sublistleftindex > sublistrightindex
				|| sublistleftindex > originrightindex
				|| sublistrightindex < originleftindex) {
				return Status::OK();
			}
			else {
				if (sublistleftindex < originleftindex) {
					sublistleftindex = originleftindex;
				}
				if (sublistrightindex > originrightindex) {
					sublistrightindex = originrightindex;
				}

				auto iter = db->NewIterator(ReadOptions());
				uint64_t currentindex = sublistleftindex;
				ListsDataKey startdatakey(key, version, currentindex);
				for (iter->Seek(startdatakey.Encode());
					iter->Valid() && currentindex <= sublistrightindex;
					iter->Next(), currentindex++) {
					ret->push_back(std::string(iter->value().data(), iter->value().size()));
				}
				return Status::OK();
			}
		}
	}
	else {
		return s;
	}
}

Status RedisList::LLen(const std::string_view& key, uint64_t* len) {
	*len = 0;
	std::string metavalue;
	ListsDataKey lkey(key, 0, 0);

	Status s = db->Get(ReadOptions(), lkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedListsMetaValue plistsmetavalue(&metavalue);
		if (plistsmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		} 
		else if (plistsmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		} 
		else {
			*len = plistsmetavalue.GetCount();
			return s;
		}
	}
	return s;
}

Status RedisList::LInsert(const std::string_view& key, const BeforeOrAfter& beforeorafter,
         const std::string& pivot, const std::string& value,
         int64_t* ret) {
	*ret = 0;
	ListsDataKey lkey(key, 0, 0);

	WriteBatch batch;
 	HashLock l(&lockmgr, key);

	std::string metavalue;
	Status s = db->Get(ReadOptions(), lkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedListsMetaValue plistsmetavalue(&metavalue);
		if (plistsmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		} 
		else if (plistsmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		} 
		else {
			bool findpivot = false;
			uint64_t pivotindex = 0;
			uint32_t version = plistsmetavalue.GetVersion();
			uint64_t currentindex = plistsmetavalue.GetLeftIndex() + 1;	
			auto iter = db->NewIterator(ReadOptions());
			ListsDataKey startdatakey(key, version, currentindex);

			for (iter->Seek(startdatakey.Encode());
				iter->Valid() && currentindex < plistsmetavalue.GetRightIndex();
				iter->Next(), currentindex++) {
				if (strcmp(iter->value().data(), pivot.data()) == 0) {
					findpivot = true;
					pivotindex = currentindex;
					break;
				}
			}

			if (!findpivot) {
				*ret = -1;
				return Status::NotFound("");
			} 
			else {
				uint64_t targetindex;
				std::vector<std::string> listnodes;
				uint64_t midindex =
					plistsmetavalue.GetLeftIndex() + 
					(plistsmetavalue.GetRightIndex() - plistsmetavalue.GetLeftIndex()) / 2;
				if (pivotindex <= midindex) {
					targetindex = (beforeorafter == Before)
						? pivotindex - 1 : pivotindex;

					currentindex = plistsmetavalue.GetLeftIndex() + 1;
					auto firsthalfiter = db->NewIterator(ReadOptions());
					ListsDataKey startdatakey(key, version, currentindex);
					for (firsthalfiter->Seek(startdatakey.Encode());
						firsthalfiter->Valid() && currentindex <= pivotindex;
						firsthalfiter->Next(), currentindex++) {
						if (currentindex == pivotindex) {
							if (beforeorafter == After) {
								listnodes.push_back(std::string(
									firsthalfiter->value().data(), firsthalfiter->value().size()));
							}
							break;
						}
						listnodes.push_back(std::string(
							firsthalfiter->value().data(), firsthalfiter->value().size()));
					}

					currentindex = plistsmetavalue.GetLeftIndex();
					for (const auto& node : listnodes) {
						ListsDataKey listsdatakey(key, version, currentindex++);
						batch.Put(listsdatakey.Encode(), node);
					}
					plistsmetavalue.ModifyLeftIndex(1);
				}
				else {
					targetindex = (beforeorafter == Before)
						? pivotindex : pivotindex + 1;
					currentindex = pivotindex;
					auto afterhalfiter = db->NewIterator(ReadOptions());
					ListsDataKey startdatakey(key, version, currentindex);
					for (afterhalfiter->Seek(startdatakey.Encode());
						afterhalfiter->Valid()
						&& currentindex < plistsmetavalue.GetRightIndex();
							afterhalfiter->Next(), currentindex++) {
							if (currentindex == pivotindex
								&& beforeorafter == BeforeOrAfter::After) {
								continue;
							}
							listnodes.push_back(std::string(
									afterhalfiter->value().data(), afterhalfiter->value().size()));
					}

					currentindex = targetindex + 1;
					for (const auto& node : listnodes) {
						ListsDataKey listsdatakey(key, version, currentindex++);
						batch.Put(listsdatakey.Encode(), node);
					}
					plistsmetavalue.ModifyRightIndex(1);
				}

				plistsmetavalue.ModifyCount(1);
				batch.Put(lkey.Encode(), metavalue);
				ListsDataKey liststargetkey(key, version, targetindex);
				batch.Put(liststargetkey.Encode(), value);
				*ret = plistsmetavalue.GetCount();
				return db->Write(WriteOptions(), &batch);
			}
		}
	}
	else if (s.IsNotFound()) {
		*ret = 0;
	}
	return s;
}

Status RedisList::LRem(const std::string_view& key, int64_t count,
              const std::string_view& value, uint64_t* ret) {
	*ret = 0;
	WriteBatch batch;		
	HashLock l(&lockmgr, key);	  
}

Status RedisList::ScanKeyNum(KeyInfo* keyinfo) {
	uint64_t keys = 0;
	uint64_t expires = 0;
	uint64_t ttlsum = 0;
	uint64_t invaildkeys = 0;

	std::string key;
	ReadOptions iteropts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iteropts.snapshot = snapshot;
	iteropts.fillcache = false;
	int64_t curtime = time(0);

	std::shared_ptr<Iterator> iter = db->NewIterator(iteropts);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		ParsedListsMetaValue plistsmetavalue(iter->value());
		if (plistsmetavalue.IsStale()) {
			invaildkeys++;
		}
		else {
			if (!plistsmetavalue.IsPermanentSurvival()) {
				expires++;
				ttlsum += plistsmetavalue.GetTimestamp() - curtime;
			}
		}
	}

	keyinfo->keys = keys;
	keyinfo->expires = expires;
	keyinfo->avgttl = (expires != 0) ? ttlsum / expires : 0;
	keyinfo->invaildkeys = invaildkeys;
	return Status::OK();
}

Status RedisList::ScanKeys(const std::string& pattern,
			std::vector<std::string>* keys) {
	std::string key;
	ReadOptions iteropts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iteropts.snapshot = snapshot;
	iteropts.fillcache = false;

	std::shared_ptr<Iterator> iter = db->NewIterator(iteropts);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		ParsedListsMetaValue plistsmetavalue(iter->value());
		if (!plistsmetavalue.IsStale()
			&& plistsmetavalue.GetCount() != 0) {
			key =ToString(iter->key());
			if (StringMatchLen(pattern.data(),
					pattern.size(), key.data(), key.size(), 0)) {
				keys->push_back(key);
			}
		}
	}
	return Status::OK();
}

Status RedisList::Expire(const std::string_view& key, int32_t ttl) {
	Status s;
	return s;
}

Status RedisList::Del(const std::string_view& key) {
	ListsDataKey lkey(key, 0, 0);
	std::string metavalue;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), lkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedListsMetaValue plistsmetavalue(&metavalue);
		if (plistsmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		} else if (plistsmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		} else {
			uint32_t statistic = plistsmetavalue.GetCount();
			plistsmetavalue.InitialMetaValue();
			s = db->Put(WriteOptions(), lkey.Encode(), metavalue);
		}
	}
	return s;
}