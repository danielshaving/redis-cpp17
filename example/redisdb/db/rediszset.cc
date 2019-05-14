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

Status RedisZset::Zadd(const std::string_view& key,
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

	char scorebuf[8];
	int32_t version = 0;
	std::string metaValue;
	WriteBatch batch;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &metaValue);
	if (s.ok()) {
		bool vaild = true;
		ParsedZSetsMetaValue pzsetmetavalue(&metaValue);
		if (pzsetmetavalue.IsStale() || pzsetmetavalue.GetCount() == 0) {
			vaild = false;
			version = pzsetmetavalue.InitialMetaValue();
		}
		else {
			vaild = true;
			version = pzsetmetavalue.GetVersion();
		}

		int32_t cnt = 0;
		std::string dataValue;
		for (const auto& sm : filteredscoremembers) {
			bool notfound = true;
			ZSetsMemberKey zmemberkey(key, version, sm.member);
			if (vaild) {
				s = db->Get(ReadOptions(), zmemberkey.Encode(), &dataValue);
				if (s.ok()) {
					notfound = false;
					uint64_t tmp = DecodeFixed64(dataValue.data());
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
		batch.Put(key, metaValue);
		*ret = cnt;
	}
	else if (s.IsNotFound()) {
		char buf[4];
		EncodeFixed32(buf, filteredscoremembers.size());
		ZSetsMetaValue zmetavalue(std::string_view(buf, sizeof(int32_t)));
		version = zmetavalue.UpdateVersion();
		batch.Put(key, zmetavalue.Encode());

		for (const auto& sm : filteredscoremembers) {
			ZSetsMemberKey zmemberkey(key, version, sm.member);
			const void* ptrscore = reinterpret_cast<const void*>(&sm.score);
			EncodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));
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
}

Status RedisZset::Zrange(const std::string_view& key,
	int32_t start, int32_t stop, std::vector<ScoreMember>* scoremembers) {
	scoremembers->clear();

	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock sl(db, snapshot);
	readopts.snapshot = snapshot;

	std::string metaValue;
	Status s = db->Get(ReadOptions(), key, &metaValue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetmetavalue(&metaValue);
		if (pzsetmetavalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else if (pzsetmetavalue.GetCount() == 0) {
			return Status::NotFound("");
		}
		else {
			int32_t count = pzsetmetavalue.GetCount();
			int32_t version = pzsetmetavalue.GetVersion();
			int32_t startIndex = start >= 0 ? start : count + start;
			int32_t stopIndex = stop >= 0 ? stop : count + stop;
			startIndex = startIndex <= 0 ? 0 : startIndex;
			stopIndex = stopIndex >= count ? count - 1 : stopIndex;
			if (startIndex > stopIndex
				|| startIndex >= count
				|| stopIndex < 0) {
				return s;
			}

			int32_t curIndex = 0;
			ScoreMember scoremember;
			ZSetsScoreKey zscorekey(key, version,
				std::numeric_limits<double>::lowest(), std::string_view());
			auto iter = db->NewIterator(ReadOptions());
			for (iter->Seek(zscorekey.Encode());
				iter->Valid() && curIndex <= stopIndex;
				iter->Next(), ++curIndex) {
				if (curIndex >= startIndex) {
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

Status RedisZset::Zrank(const std::string_view& key,
	const std::string_view& member, int32_t* rank) {
	*rank = -1;

	std::string metavalue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock sl(db, snapshot);
	readopts.snapshot = snapshot;

	Status s = db->Get(readopts, key, &metavalue);
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
			ScoreMember scoremember;
			ZSetsScoreKey zsetscorekey(key, version,
				std::numeric_limits<double>::lowest(), std::string_view());
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

Status RedisZset::Zcard(const std::string_view& key, int32_t* card) {
	*card = 0;
	std::string metavalue;

	Status s = db->Get(ReadOptions(), key, &metavalue);
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

Status RedisZset::Zincrby(const std::string_view& key,
	const std::string_view& member,
	double increment,
	double* ret) {
	*ret = 0;
	uint32_t statistic = 0;
	double score = 0;
	char scorebuf[8];
	int32_t version = 0;
	std::string metavalue;
	WriteBatch batch;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &metavalue);
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
		ZSetsMemberKey zmemberkey(key, version, member);
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
			batch.Put(key, metavalue);
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

	ZSetsMemberKey zmemberkey(key, version, member);
	const void* ptrscore = reinterpret_cast<const void*>(&score);
	EncodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));
	batch.Put(zmemberkey.Encode(), std::string_view(scorebuf, sizeof(uint64_t)));

	ZSetsScoreKey zscorekey(key, version, score, member);
	batch.Put(zscorekey.Encode(), std::string_view());
	*ret = score;
	s = db->Write(WriteOptions(), &batch);
	return s;
}
