#include "rediszset.h"

RedisZset::RedisZset(RedisDB* redis, const Options& options, const std::string& path)
	:redis(redis),
	db(new DB(options, path)) {
}

RedisZset::~RedisZset() {

}

Status RedisZset::Open() {
	return db->Open();
}

Status RedisZset::DestroyDB(const std::string path, const Options& options) {
	return db->DestroyDB(path, options);
}

Status RedisZset::ZAdd(const std::string_view& key,
	const std::vector<ScoreMember>& scoremembers, int32_t* ret) {
	*ret = 0;
	uint32_t statistic = 0;
	std::unordered_set<std::string> unique;
	std::vector<ScoreMember> filteredscoremembers;
	for (const auto& sm : scoremembers) {
		if (unique.find(sm.member) == unique.end()) {
			unique.insert(sm.member);
			filteredscoremembers.push_back(sm);
		}
	}

	ZSetsScoreKey zkey(key, 0, 0, 0);
	char scorebuf[8];
	int32_t version = 0;
	std::string metavalue;
	WriteBatch batch;

	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), zkey.Encode(), &metavalue);
	if (s.ok()) {
		bool vaild = true;
		ParsedZSetsMetaValue pzsetmetavalue(&metavalue);
		if (pzsetmetavalue.IsStale() || pzsetmetavalue.GetCount() == 0) {
			vaild = false;
			version = pzsetmetavalue.InitialMetaValue();
		}
		else {
			vaild = true;
			version = pzsetmetavalue.GetVersion();
		}

		int32_t cnt = 0;
		std::string datavalue;
		for (const auto& sm : filteredscoremembers) {
			bool notfound = true;
			ZSetsScoreKey zmemberkey(key, 0, version, sm.member);
			if (vaild) {
				s = db->Get(ReadOptions(), zmemberkey.Encode(), &datavalue);
				if (s.ok()) {
					notfound = false;
					uint64_t tmp = DecodeFixed64(datavalue.data());
					const void* ptrtmp = reinterpret_cast<const void*>(&tmp);
					double oldscore = *reinterpret_cast<const double*>(ptrtmp);
					if (oldscore == sm.score) {
						continue;
					}
					else {
						ZSetsScoreKey zscorekey(key, version, oldscore, sm.member);
						batch.Delete(zscorekey.Encode());
						statistic++;
					}
				}
				else if (!s.IsNotFound()) {
					return s;
				}
			}

			const void* ptrscore = reinterpret_cast<const void*>(&sm.score);
			EncodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));
			batch.Put(zmemberkey.Encode(), std::string_view(scorebuf, sizeof(uint64_t)));

			ZSetsScoreKey zscorekey(key, version, sm.score, sm.member);
			batch.Put(zscorekey.Encode(), std::string_view());
			if (notfound) {
				cnt++;
			}
		}

		pzsetmetavalue.ModifyCount(cnt);
		batch.Put(zkey.Encode(), metavalue);
		*ret = cnt;
	}
	else if (s.IsNotFound()) {
		char buf[4];
		EncodeFixed32(buf, filteredscoremembers.size());
		ZSetsMetaValue zmetavalue(std::string_view(buf, sizeof(int32_t)));
		version = zmetavalue.UpdateVersion();

		batch.Put(zkey.Encode(), zmetavalue.Encode());
		
		for (const auto& sm : filteredscoremembers) {
			const void* ptrscore = reinterpret_cast<const void*>(&sm.score);
			EncodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));

			ZSetsScoreKey zmemberkey(key, 0, version, sm.member);
			batch.Put(zmemberkey.Encode(), std::string_view(scorebuf, sizeof(uint64_t)));
			
			ZSetsScoreKey zsetscorekey(key, version, sm.score, sm.member);
			batch.Put(zsetscorekey.Encode(), std::string_view());
		}
		*ret = filteredscoremembers.size();
	}
	else {
		return s;
	}
	
	s = db->Write(WriteOptions(), &batch);
	return s;
}

Status RedisZset::ZRange(const std::string_view& key,
	int32_t start, int32_t stop, std::vector<ScoreMember>* scoremembers) {
	scoremembers->clear();
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock sl(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;

	ZSetsScoreKey zkey(key, 0, 0, 0);
	std::string metavalue;
	Status s = db->Get(readopts, zkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetmetavalue(&metavalue);
		if (pzsetmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (pzsetmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}
		else {
			int32_t count = pzsetmetavalue.GetCount();
			int32_t version = pzsetmetavalue.GetVersion();
			int32_t startindex = start >= 0 ? start : count + start;
			int32_t stopindex = stop >= 0 ? stop : count + stop;
			startindex = startindex <= 0 ? 0 : startindex;
			stopindex = stopindex >= count ? count - 1 : stopindex;
			if (startindex > stopindex
				|| startindex >= count
				|| stopindex < 0) {
				return s;
			}

			int32_t curindex = 0;
			ScoreMember scoremember;
			ZSetsScoreKey zscorekey(key, version,
				std::numeric_limits<double>::lowest(), std::string_view());

			auto iter = db->NewIterator(readopts);
			for (iter->Seek(zscorekey.Encode()); iter->Valid()
				&& curindex <= stopindex;
				iter->Next(), ++curindex) {
				if (curindex >= startindex) {
					ParsedZSetsScoreKey pscorekey(iter->key());
					scoremember.score = pscorekey.GetScore();
					scoremember.member = pscorekey.GetMemberToString();
					scoremembers->push_back(scoremember);
				}
			}
		}
	}
	return s;
}

Status RedisZset::ZRank(const std::string_view& key,
	const std::string_view& member, int32_t* rank) {
	*rank = -1;

	std::string metavalue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock sl(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;

	ZSetsScoreKey zkey(key, 0, 0, 0);
	Status s = db->Get(readopts, zkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetmetavalue(&metavalue);
		if (pzsetmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (pzsetmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}
		else {
			bool found = false;
			int32_t version = pzsetmetavalue.GetVersion();
			int32_t index = 0;
			int32_t stopindex = pzsetmetavalue.GetCount() - 1;
			ZSetsScoreKey zsetscorekey(key, version,
				std::numeric_limits<double>::lowest(), "");
			auto iter = db->NewIterator(readopts);
			for (iter->Seek(zsetscorekey.Encode());
				iter->Valid() && index <= stopindex;
				iter->Next(), ++index) {
				ParsedZSetsScoreKey pzsetscorekey(iter->key());
				if (!pzsetscorekey.GetMember().compare(member)) {
					found = true;
					break;
				}
			}

			if (found) {
				*rank = index;
				return Status::OK();
			}
			else {
				return Status::NotFound("");
			}
		}
	}
	return s;
}

Status RedisZset::ZCard(const std::string_view& key, int32_t* card) {
	*card = 0;
	std::string metavalue;
	ZSetsScoreKey zkey(key, 0, 0, 0);

	Status s = db->Get(ReadOptions(), zkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzetmetavalue(&metavalue);
		if (pzetmetavalue.IsStale()) {
			*card = 0;
			return Status::NotFound("Stale");
		}
		else if (pzetmetavalue.GetCount() == 0) {
			*card = 0;
			return Status::NotFound("");
		}
		else {
			*card = pzetmetavalue.GetCount();
		}
	}
	return s;
}

Status RedisZset::ZIncrby(const std::string_view& key,
	const std::string_view& member, double increment, double* ret) {
	*ret = 0;
	uint32_t statistic = 0;
	double score = 0;
	char scorebuf[8];
	int32_t version = 0;
	std::string metavalue;
	WriteBatch batch;
	HashLock l(&lockmgr, key);
	
	ZSetsScoreKey zkey(key, 0, 0, 0);
	Status s = db->Get(ReadOptions(), zkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetmetavalue(&metavalue);
		if (pzsetmetavalue.IsStale()
			|| pzsetmetavalue.GetCount() == 0) {
			version = pzsetmetavalue.InitialMetaValue();
		}
		else {
			version = pzsetmetavalue.GetVersion();
		}

		std::string datavalue;
		ZSetsScoreKey zmemberkey(key, 0, version, member);
		s = db->Get(ReadOptions(), zmemberkey.Encode(), &datavalue);
		if (s.ok()) {
			uint64_t tmp = DecodeFixed64(datavalue.data());
			const void* ptrtmp = reinterpret_cast<const void*>(&tmp);
			double oldscore = *reinterpret_cast<const double*>(ptrtmp);
			score = oldscore + increment;
			ZSetsScoreKey zscorekey(key, version, oldscore, member);
			batch.Delete(zscorekey.Encode());
			statistic++;
		}
		else if (s.IsNotFound()) {
			score = increment;
			pzsetmetavalue.ModifyCount(1);
			batch.Put(zkey.Encode(), metavalue);
		}
		else {
			return s;
		}
	}
	else if (s.IsNotFound()) {
		char buf[8];
		EncodeFixed32(buf, 1);
		ZSetsMetaValue zmetavalue(std::string_view(buf, sizeof(int32_t)));
		version = zmetavalue.UpdateVersion();
		batch.Put(key, zmetavalue.Encode());
		score = increment;
	}
	else {
		return s;
	}

	ZSetsScoreKey zmemberkey(key, 0, version, member);
	const void* ptrscore = reinterpret_cast<const void*>(&score);
	EncodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));
	batch.Put(zmemberkey.Encode(), std::string_view(scorebuf, sizeof(uint64_t)));

	ZSetsScoreKey zscorekey(key, version, score, member);
	batch.Put(zscorekey.Encode(), "");
	*ret = score;
	s = db->Write(WriteOptions(), &batch);
	return s;
}

Status RedisZset::ScanKeyNum(KeyInfo* keyinfo) {
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
		ParsedStringsMetaValue pstringsvalue(iter->value());
		if (pstringsvalue.IsStale()) {
			invaildkeys++;
		}
		else {
			if (!pstringsvalue.IsPermanentSurvival()) {
				expires++;
				ttlsum += pstringsvalue.GetTimestamp() - curtime;
			}
		}
	}

	keyinfo->keys = keys;
	keyinfo->expires = expires;
	keyinfo->avgttl = (expires != 0) ? ttlsum / expires : 0;
	keyinfo->invaildkeys = invaildkeys;
	return Status::OK();
}

Status RedisZset::ScanKeys(const std::string& pattern,
			std::vector<std::string>* keys) {
	std::string key;
	ReadOptions iteropts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iteropts.snapshot = snapshot;
	iteropts.fillcache = false;
	
	std::shared_ptr<Iterator> iter = db->NewIterator(iteropts);
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		ParsedHashesMetaValue phashesmetavalue(iter->value());
		if (!phashesmetavalue.IsStale()
			&& phashesmetavalue.GetCount() != 0) {
			key =ToString(iter->key());

			ZSetsScoreKey zkey(pattern, 0, 0, 0);
			const std::string_view keyview = zkey.Encode();
			if (StringMatchLen(keyview.data(),
					keyview.size(), key.data(), key.size(), 0)) {
				keys->push_back(key);
			}
		}
	}
	return Status::OK();
}


Status RedisZset::ZCount(const std::string_view& key, double min, double max,
	bool leftclose, bool rightclose, int32_t* ret) {
		
}

Status RedisZset::Expire(const std::string_view& key, int32_t ttl) {
	std::string metavalue;
	ZSetsScoreKey zkey(key, 0, 0, 0);

	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), zkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetsmetavalue(&metavalue);
		if (pzsetsmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		} else if (pzsetsmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}

		if (ttl > 0) {
			pzsetsmetavalue.SetRelativeTimestamp(ttl);
		} else {
			pzsetsmetavalue.InitialMetaValue();
		}
		s = db->Put(WriteOptions(), zkey.Encode(), metavalue);
	}
	return s;
}

Status RedisZset::Del(const std::string_view& key) {
	ZSetsScoreKey zkey(key, 0, 0, 0);
	std::string metavalue;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), zkey.Encode(), &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetsmetavalue(&metavalue);
		if (pzsetsmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		} else if (pzsetsmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		} else {
			uint32_t statistic = pzsetsmetavalue.GetCount();
			pzsetsmetavalue.InitialMetaValue();
			s = db->Put(WriteOptions(), zkey.Encode(), metavalue);
		}
	}
	return s;
}