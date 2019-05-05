#include "rediszset.h"

RedisZset::RedisZset(RedisDB* redis, const Options& options, const std::string& path)
	:redis(redis),
	db(new DB(options, path)) {
}

RedisZset::~RedisZset() {

}

Status RedisZset::open() {
	return db->open();
}

Status RedisZset::zadd(const std::string_view& key,
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
	HashLock hashlock(&lockmgr, key);
	Status s = db->get(ReadOptions(), key, &metaValue);
	if (s.ok()) {
		bool vaild = true;
		ParsedZSetsMetaValue pzsetmetavalue(&metaValue);
		if (pzsetmetavalue.isStale() || pzsetmetavalue.getCount() == 0) {
			vaild = false;
			version = pzsetmetavalue.initialMetaValue();
		}
		else {
			vaild = true;
			version = pzsetmetavalue.getVersion();
		}

		int32_t cnt = 0;
		std::string dataValue;
		for (const auto& sm : filteredscoremembers) {
			bool notfound = true;
			ZSetsMemberKey zmemberkey(key, version, sm.member);
			if (vaild) {
				s = db->get(ReadOptions(), zmemberkey.encode(), &dataValue);
				if (s.ok()) {
					notfound = false;
					uint64_t tmp = decodeFixed64(dataValue.data());
					const void* ptrtmp = reinterpret_cast<const void*>(&tmp);
					double oldscore = *reinterpret_cast<const double*>(ptrtmp);
					if (oldscore == sm.score) {
						continue;
					}
					else {
						ZSetsScoreKey zscorekey(key, version, oldscore, sm.member);
						batch.del(zscorekey.encode());
						statistic++;
					}
				}
				else if (!s.isNotFound()) {
					return s;
				}
			}

			const void* ptrscore = reinterpret_cast<const void*>(&sm.score);
			encodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));
			batch.put(zmemberkey.encode(), std::string_view(scorebuf, sizeof(uint64_t)));

			ZSetsScoreKey zscorekey(key, version, sm.score, sm.member);
			batch.put(zscorekey.encode(), std::string_view());
			if (notfound) {
				cnt++;
			}
		}

		pzsetmetavalue.modifyCount(cnt);
		batch.put(key, metaValue);
		*ret = cnt;
	}
	else if (s.isNotFound()) {
		char buf[4];
		encodeFixed32(buf, filteredscoremembers.size());
		ZSetsMetaValue zmetavalue(std::string_view(buf, sizeof(int32_t)));
		version = zmetavalue.updateVersion();
		batch.put(key, zmetavalue.encode());

		for (const auto& sm : filteredscoremembers) {
			ZSetsMemberKey zmemberkey(key, version, sm.member);
			const void* ptrscore = reinterpret_cast<const void*>(&sm.score);
			encodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));
			batch.put(zmemberkey.encode(), std::string_view(scorebuf, sizeof(uint64_t)));

			ZSetsScoreKey zsetscorekey(key, version, sm.score, sm.member);
			batch.put(zsetscorekey.encode(), std::string_view());
		}
		*ret = filteredscoremembers.size();
	}
	else {
		return s;
	}
	s = db->write(WriteOptions(), &batch);
}

Status RedisZset::zrange(const std::string_view& key,
	int32_t start, int32_t stop, std::vector<ScoreMember>* scoremembers) {
	scoremembers->clear();

	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;

	std::string metaValue;
	Status s = db->get(ReadOptions(), key, &metaValue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetmetavalue(&metaValue);
		if (pzsetmetavalue.isStale()) {
			return Status::notFound("Stale");
		}
		else if (pzsetmetavalue.getCount() == 0) {
			return Status::notFound("");
		}
		else {
			int32_t count = pzsetmetavalue.getCount();
			int32_t version = pzsetmetavalue.getVersion();
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
			auto iter = db->newIterator(ReadOptions());
			for (iter->seek(zscorekey.encode());
				iter->valid() && curIndex <= stopIndex;
				iter->next(), ++curIndex) {
				if (curIndex >= startIndex) {
					ParsedZSetsScoreKey pscorekey(iter->key());
					scoremember.score = pscorekey.getScore();
					scoremember.member = pscorekey.getMemberToString();
					scoremembers->push_back(scoremember);
				}
			}
		}
	}
	return s;
}

Status RedisZset::zrank(const std::string_view& key,
	const std::string_view& member, int32_t* rank) {
	*rank = -1;

	std::string metavalue;
	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	readopts.snapshot = snapshot;

	Status s = db->get(readopts, key, &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetmetavalue(&metavalue);
		if (pzsetmetavalue.isStale()) {
			return Status::notFound("Stale");
		}
		else if (pzsetmetavalue.getCount() == 0) {
			return Status::notFound("");
		}
		else {
			bool found = false;
			int32_t version = pzsetmetavalue.getVersion();
			int32_t index = 0;
			int32_t stopindex = pzsetmetavalue.getCount() - 1;
			ScoreMember scoremember;
			ZSetsScoreKey zsetscorekey(key, version,
				std::numeric_limits<double>::lowest(), std::string_view());
			auto iter = db->newIterator(readopts);
			for (iter->seek(zsetscorekey.encode());
				iter->valid() && index <= stopindex;
				iter->next(), ++index) {
				ParsedZSetsScoreKey pzsetscorekey(iter->key());
				if (!pzsetscorekey.getMember().compare(member)) {
					found = true;
					break;
				}
			}

			if (found) {
				*rank = index;
				return Status::OK();
			}
			else {
				return Status::notFound("");
			}
		}
	}
	return s;
}

Status RedisZset::zcard(const std::string_view& key, int32_t* card) {
	*card = 0;
	std::string metavalue;

	Status s = db->get(ReadOptions(), key, &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzetmetavalue(&metavalue);
		if (pzetmetavalue.isStale()) {
			*card = 0;
			return Status::notFound("Stale");
		}
		else if (pzetmetavalue.getCount() == 0) {
			*card = 0;
			return Status::notFound("");
		}
		else {
			*card = pzetmetavalue.getCount();
		}
	}
	return s;
}

Status RedisZset::zincrby(const std::string_view& key,
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
	Status s = db->get(ReadOptions(), key, &metavalue);
	if (s.ok()) {
		ParsedZSetsMetaValue pzsetmetavalue(&metavalue);
		if (pzsetmetavalue.isStale()
			|| pzsetmetavalue.getCount() == 0) {
			version = pzsetmetavalue.initialMetaValue();
		}
		else {
			version = pzsetmetavalue.getVersion();
		}

		std::string datavalue;
		ZSetsMemberKey zmemberkey(key, version, member);
		s = db->get(ReadOptions(), zmemberkey.encode(), &datavalue);
		if (s.ok()) {
			uint64_t tmp = decodeFixed64(datavalue.data());
			const void* ptrtmp = reinterpret_cast<const void*>(&tmp);
			double oldscore = *reinterpret_cast<const double*>(ptrtmp);
			score = oldscore + increment;
			ZSetsScoreKey zscorekey(key, version, oldscore, member);
			batch.del(zscorekey.encode());
			statistic++;
		}
		else if (s.isNotFound()) {
			score = increment;
			pzsetmetavalue.modifyCount(1);
			batch.put(key, metavalue);
		}
		else {
			return s;
		}
	}
	else if (s.isNotFound()) {
		char buf[8];
		encodeFixed32(buf, 1);
		ZSetsMetaValue zmetavalue(std::string_view(buf, sizeof(int32_t)));
		version = zmetavalue.updateVersion();
		batch.put(key, zmetavalue.encode());
		score = increment;
	}
	else {
		return s;
	}

	ZSetsMemberKey zmemberkey(key, version, member);
	const void* ptrscore = reinterpret_cast<const void*>(&score);
	encodeFixed64(scorebuf, *reinterpret_cast<const uint64_t*>(ptrscore));
	batch.put(zmemberkey.encode(), std::string_view(scorebuf, sizeof(uint64_t)));

	ZSetsScoreKey zscorekey(key, version, score, member);
	batch.put(zscorekey.encode(), std::string_view());
	*ret = score;
	s = db->write(WriteOptions(), &batch);
	return s;
}
