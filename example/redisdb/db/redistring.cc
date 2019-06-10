#include "redistring.h"
#include "redisdb.h"
#include "util.h"

RedisString::RedisString(RedisDB* redis,
	const Options& options, const std::string& path)
	:redis(redis),
	db(new DB(options, path)) {

}

RedisString::~RedisString() {

}

Status RedisString::Open() {
	return db->Open();
}

Status RedisString::DestroyDB(const std::string path, const Options& options) {
	return db->DestroyDB(path, options);
}

Status RedisString::Set(const std::string_view& key,
	const std::string_view& value) {
	StringsMetaValue stringsvalue(value);
	HashLock l(&lockmgr, key);
	return db->Put(WriteOptions(), key, stringsvalue.Encode());
}

Status RedisString::Get(const std::string_view& key,
	std::string* value) {
	value->clear();
	Status s = db->Get(ReadOptions(), key, value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(value);
		if (pstringsvalue.IsStale()) {
			value->clear();
			return Status::NotFound("Stale");
		}
		else {
			pstringsvalue.StripSuffix();
		}
	}
	return s;
}

Status RedisString::Setxx(const std::string_view& key,
	const std::string_view& value, int32_t* ret, const int32_t ttl) {
	bool notfound = true;
	std::string oldValue;
	StringsMetaValue stringsvalue(value);
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldValue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringvalue(oldValue);
		if (!pstringvalue.IsStale()) {
			notfound = false;
		}
	}
	else if (!s.IsNotFound()) {
		return s;
	}

	if (notfound) {
		*ret = 0;
		return s;
	}
	else {
		*ret = 1;
		if (ttl > 0) {
			stringsvalue.SetRelativeTimestamp(ttl);
		}
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
}

Status RedisString::GetSet(const std::string_view& key,
	const std::string_view& value, std::string* oldvalue) {
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(oldvalue);
		if (pstringsvalue.IsStale()) {
			*oldvalue = "";
		}
		else {
			pstringsvalue.StripSuffix();
		}
	}
	else if (!s.IsNotFound()) {
		return s;
	}

	StringsMetaValue stringsvalue(value);
	return db->Put(WriteOptions(), key, stringsvalue.Encode());
}

Status RedisString::SetBit(const std::string_view& key,
	int64_t offset, int32_t value, int32_t* ret) {
	std::string metavalue;
	if (offset < 0) {
		return Status::InvalidArgument("offset< 0");
	}

	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &metavalue);
	if (s.ok() || s.IsNotFound()) {
		std::string datavalue;
		if (s.ok()) {
			ParsedStringsMetaValue pstringsvalue(&metavalue);
			if (!pstringsvalue.IsStale()) {
				datavalue = pstringsvalue.GetValueToString();
			}
		}
		size_t byte = offset >> 3;
		size_t bit = 7 - (offset & 0x7);
		char byteVal;
		size_t valueLength = datavalue.length();
		if (byte + 1 > valueLength) {
			*ret = 0;
			byteVal = 0;
		}
		else {
			*ret = ((datavalue[byte] & (1 << bit)) >> bit);
			byteVal = datavalue[byte];
		}

		if (*ret == value) {
			return Status::OK();
		}

		byteVal &= static_cast<char>(~(1 << bit));
		byteVal |= static_cast<char>((value & 0x1) << bit);
		if (byte + 1 <= valueLength) {
			datavalue.replace(byte, 1, &byteVal, 1);
		}
		else {
			datavalue.append(byte + 1 - valueLength - 1, 0);
			datavalue.append(1, byteVal);
		}

		StringsMetaValue stringsvalue(datavalue);
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
	else {
		return s;
	}
}

Status RedisString::GetBit(const std::string_view& key,
	int64_t offset, int32_t* ret) {
	std::string metavalue;
	Status s = db->Get(ReadOptions(), key, &metavalue);
	if (s.ok() || s.IsNotFound()) {
		std::string dataValue;
		if (s.ok()) {
			ParsedStringsMetaValue pstringsvalue(&metavalue);
			if (pstringsvalue.IsStale()) {
				*ret = 0;
				return Status::OK();
			}
			else {
				dataValue = pstringsvalue.GetValueToString();
			}
		}

		size_t byte = offset >> 3;
		size_t bit = 7 - (offset & 0x7);
		if (byte + 1 > dataValue.length()) {
			*ret = 0;
		}
		else {
			*ret = ((dataValue[byte] & (1 << bit)) >> bit);
		}
	}
	else {
		return s;
	}
	return Status::OK();
}

Status RedisString::MSet(const std::vector<KeyValue>& kvs) {
	std::vector<std::string> keys;
	for (const auto& kv : kvs) {
		keys.push_back(kv.key);
	}

	MultiHashLock l(&lockmgr, keys);
	WriteBatch batch;
	for (const auto& kv : kvs) {
		StringsMetaValue stringsvalue(kv.value);
		batch.Put(kv.key, stringsvalue.Encode());
	}
	return db->Write(WriteOptions(), &batch);
}

Status RedisString::MGet(const std::vector<std::string>& keys,
	std::vector<ValueStatus>* vss) {
	vss->clear();

	ReadOptions readopts;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock sl(db, snapshot);
	readopts.snapshot = snapshot;
	readopts.fillcache = false;

	Status s;
	std::string value;
	for (const auto& key : keys) {
		s = db->Get(readopts, key, &value);
		if (s.ok()) {
			ParsedStringsMetaValue pstringsvalue(&value);
			if (pstringsvalue.IsStale()) {
				vss->push_back({ std::string(), Status::NotFound("Stale") });
			}
			else {
				vss->push_back({ pstringsvalue.GetValueToString(), Status::OK() });
			}
		}
		else if (s.IsNotFound()) {
			vss->push_back({ std::string(), Status::NotFound(" ") });
		}
		else {
			vss->clear();
			return s;
		}
	}
	return Status::OK();
}

Status RedisString::Setex(const std::string_view& key,
	const std::string_view & value, int32_t ttl) {

}

Status RedisString::Setnx(const std::string_view& key,
	const std::string_view& value, int32_t* ret, const int32_t ttl) {
	*ret = 0;
	std::string oldValue;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldValue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldValue);
		if (pstringsvalue.IsStale()) {
			StringsMetaValue stringsvalue(value);
			if (ttl > 0) {
				stringsvalue.SetRelativeTimestamp(ttl);
			}

			s = db->Put(WriteOptions(), key, stringsvalue.Encode());
			if (s.ok()) {
				*ret = 1;
			}
		}
	}
	else if (s.IsNotFound()) {
		StringsMetaValue stringsvalue(value);
		if (ttl > 0) {
			stringsvalue.SetRelativeTimestamp(ttl);
		}

		s = db->Put(WriteOptions(), key, stringsvalue.Encode());
		if (s.ok()) {
			*ret = 1;
		}
	}
	return s;
}

Status RedisString::MSetnx(const std::vector<KeyValue>& kvs,
	int32_t * ret) {
	Status s;
	bool exists = false;
	*ret = 0;
	std::string value;
	for (size_t i = 0; i < kvs.size(); i++) {
		s = db->Get(ReadOptions(), kvs[i].key, &value);
		if (s.ok()) {
			ParsedStringsMetaValue pstringsvalue(&value);
			if (!pstringsvalue.IsStale()) {
				exists = true;
				break;
			}
		}
	}

	if (!exists) {
		s = MSet(kvs);
		if (s.ok()) {
			*ret = 1;
		}
	}
	return s;
}

Status RedisString::Setvx(const std::string_view& key,
	const std::string_view& value,
	const std::string_view& newvalue,
	int32_t * ret, const int32_t ttl) {
	*ret = 0;
	std::string oldvalue;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldvalue);
		if (pstringsvalue.IsStale()) {
			*ret = 0;
		}
		else {
			if (!value.compare(pstringsvalue.GetValue())) {
				StringsMetaValue stringsvalue(newvalue);
				if (ttl > 0) {
					stringsvalue.SetRelativeTimestamp(ttl);
				}

				s = db->Put(WriteOptions(), key, stringsvalue.Encode());
				if (!s.ok()) {
					return s;
				}

				*ret = 1;
			}
			else {
				*ret = -1;
			}
		}
	}
	else if (s.IsNotFound()) {
		*ret = 0;
	}
	else {
		return s;
	}
	return Status::OK();
}

Status RedisString::Del(const std::string_view& key) {
	std::string value;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&value);
		if (pstringsvalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		return db->Delete(WriteOptions(), key);
	}
	return s;
}

Status RedisString::Delvx(const std::string_view& key,
	const std::string_view& value, int32_t* ret) {
	*ret = 0;
	std::string oldvalue;
	HashLock ll(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldvalue);
		if (pstringsvalue.IsStale()) {
			*ret = 0;
			return Status::NotFound("Stale");
		}
		else {
			if (!value.compare(pstringsvalue.GetValue())) {
				*ret = 1;
				return db->Delete(WriteOptions(), key);
			}
			else {
				*ret = -1;
			}
		}
	}
	else if (s.IsNotFound()) {
		*ret = 0;
	}
	return s;
}

Status RedisString::Setrange(const std::string_view& key,
	int64_t startoffset, const std::string_view& value, int32_t* ret) {
	std::string oldvalue;
	std::string newvalue;
	if (startoffset < 0) {
		return Status::InvalidArgument("offset< 0");
	}

	Status s = db->Get(ReadOptions(), key, &oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldvalue);
		pstringsvalue.StripSuffix();
		if (pstringsvalue.IsStale()) {
			std::string tmp(startoffset, '\0');
			newvalue = tmp.append(value.data());
			*ret = newvalue.length();
		}
		else {
			if (static_cast<size_t>(startoffset) > oldvalue.length()) {
				oldvalue.resize(startoffset);
				newvalue = oldvalue.append(value.data());
			}
			else {
				std::string head = oldvalue.substr(0, startoffset);
				std::string tail;
				if (startoffset + value.size() - 1 < oldvalue.length() - 1) {
					tail = oldvalue.substr(startoffset + value.size());
				}
				newvalue = head + value.data() + tail;
			}
		}

		*ret = newvalue.length();
		StringsMetaValue stringsvalue(newvalue);
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
	else if (s.IsNotFound()) {
		std::string tmp(startoffset, '\0');
		newvalue = tmp.append(value.data());
		*ret = newvalue.length();
		StringsMetaValue stringsvalue(newvalue);
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
	return s;
}

Status RedisString::Getrange(const std::string_view& key,
	int64_t startoffset, int64_t endoffset, std::string* ret) {
	*ret = "";
	std::string value;
	Status s = db->Get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&value);
		if (pstringsvalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else {
			pstringsvalue.StripSuffix();
			int64_t size = value.size();
			int64_t start = startoffset >= 0 ? startoffset : size + startoffset;
			int64_t endt = endoffset >= 0 ? endoffset : size + endoffset;
			if (start > size - 1 ||
				(start != 0 && start > endt) ||
				(start != 0 && endt < 0)) {
				return Status::OK();
			}

			if (start < 0) {
				start = 0;
			}

			if (endt >= size) {
				endt = size - 1;
			}

			if (start == 0 && endt < 0) {
				endt = 0;
			}

			*ret = value.substr(start, endt - start + 1);
			return Status::OK();
		}
	}
	else {
		return s;
	}
}

Status RedisString::Strlen(const std::string_view& key, int32_t* len) {
	std::string value;
	Status s = Get(key, &value);
	if (s.ok()) {
		*len = value.size();
	}
	else {
		*len = 0;
	}
	return s;
}

Status RedisString::Expire(const std::string_view & key,
	int32_t ttl) {
	std::string value;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&value);
		if (pstringsvalue.IsStale()) {
			return Status::NotFound("Stale");
		}

		if (ttl > 0) {
			pstringsvalue.SetRelativeTimestamp(ttl);
			return db->Put(WriteOptions(), key, value);
		}
		else {
			return db->Delete(WriteOptions(), key);
		}
	}
	return s;
}

bool RedisString::Scan(const std::string& startkey, const std::string& pattern,
	std::vector<std::string>* keys,
	int64_t* count, std::string* nextkey) {
	std::string key;
	bool isfinish = true;
	ReadOptions iteratoroptions;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock sl(db, snapshot);
	iteratoroptions.snapshot = snapshot;
	iteratoroptions.fillcache = false;
	auto it = db->NewIterator(iteratoroptions);

	it->Seek(startkey);
	while (it->Valid() && (*count) > 0) {
		ParsedStringsMetaValue pstringsvalue(it->value());
		if (pstringsvalue.IsStale()) {
			it->Next();
			continue;
		}
		else {
			key = std::string(it->key().data(), it->key().size());
			if (StringMatchLen(pattern.data(), pattern.size(),
				key.data(), key.size(), 0)) {
				keys->push_back(key);
			}
			(*count)--;
			it->Next();
		}
	}

	std::string prefix = IsTailWildcard(pattern) ?
		pattern.substr(0, pattern.size() - 1) : "";
	if (it->Valid() && (it->key().compare(prefix) <= 0 || StartsWith(it->key(), prefix))) {
		isfinish = false;
		*nextkey = std::string(it->key().data(), it->key().size());
	}
	else {
		*nextkey = "";
	}
	return isfinish;
}

Status RedisString::Expireat(const std::string_view& key,
	int32_t timestamp) {
	std::string value;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvlaue(&value);
		if (pstringsvlaue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else {
			if (timestamp > 0) {
				pstringsvlaue.SetTimestamp(timestamp);
				return db->Put(WriteOptions(), key, value);
			}
			else {
				return db->Delete(WriteOptions(), key);
			}
		}
	}
	return s;
}

Status RedisString::Persist(const std::string_view& key) {
	std::string value;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringvalues(&value);
		if (pstringvalues.IsStale()) {
			return Status::NotFound("Stale");
		}
		else {
			int32_t timestamp = pstringvalues.GetTimestamp();
			if (timestamp == 0) {
				return Status::NotFound("Not have an associated timeout");
			}
			else {
				pstringvalues.SetTimestamp(0);
				return db->Put(WriteOptions(), key, value);
			}
		}
	}
	return s;
}

Status RedisString::TTL(const std::string_view& key,
	int64_t* timestamp) {
	std::string value;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&value);
		if (pstringsvalue.IsStale()) {
			*timestamp = -2;
			return Status::NotFound("Stale");
		}
		else {
			*timestamp = pstringsvalue.GetTimestamp();
			if (*timestamp == 0) {
				*timestamp = -1;
			}
			else {
				int64_t curtime = time(0);
				*timestamp = *timestamp - curtime >= 0 ? *timestamp - curtime : -2;
			}
		}
	}
	else if (s.IsNotFound()) {
		*timestamp = -2;
	}
	return s;
}

Status RedisString::Incrby(const std::string_view& key,
	int64_t value, int64_t* ret) {
	std::string oldvalue;
	std::string newvalue;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldvalue);
		if (pstringsvalue.IsStale()) {
			*ret = value;
			StringsMetaValue stringsvalue(std::to_string(value));
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
		else {
			int32_t timestamp = pstringsvalue.GetTimestamp();
			std::string olduservalue = pstringsvalue.GetValueToString();
			char* end = nullptr;
			int64_t ival = strtoll(olduservalue.c_str(), &end, 10);
			if (*end != 0) {
				return Status::Corruption("Value is not a integer");
			}

			if ((value >= 0 && LLONG_MAX - value < ival) ||
				(value < 0 && LLONG_MIN - value > ival)) {
				return Status::InvalidArgument("Overflow");
			}

			*ret = ival + value;
			newvalue = std::to_string(*ret);
			StringsMetaValue stringsvalue(newvalue);
			stringsvalue.SetTimestamp(timestamp);
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
	}
	else if (s.IsNotFound()) {
		*ret = value;
		StringsMetaValue stringsvalue(std::to_string(value));
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
	else {
		return s;
	}
}

Status RedisString::Incrbyfloat(const std::string_view& key,
	const std::string_view& value, std::string* ret) {
	std::string oldvalue, newvalue;
	long double longdoubleby;
	if (StrToLongDouble(value.data(), value.size(), &longdoubleby) == -1) {
		return Status::Corruption("Value is not a vaild float");
	}

	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldvalue);
		if (pstringsvalue.IsStale()) {
			LongDoubleToStr(longdoubleby, &newvalue);
			*ret = newvalue;
			StringsMetaValue stringsvalue(newvalue);
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
		else {
			int32_t timestamp = pstringsvalue.GetTimestamp();
			std::string olduservalue = pstringsvalue.GetValueToString();
			long double total, oldnumber;
			if (StrToLongDouble(olduservalue.data(),
				olduservalue.size(), &oldnumber) == -1) {
				return Status::Corruption("Value is not a vaild float");
			}
			total = oldnumber + longdoubleby;
			if (LongDoubleToStr(total, &newvalue) == -1) {
				return Status::InvalidArgument("Overflow");
			}

			*ret = newvalue;
			StringsMetaValue stringsvalue(newvalue);
			stringsvalue.SetTimestamp(timestamp);
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
	}
	else if (s.IsNotFound()) {
		LongDoubleToStr(longdoubleby, &newvalue);
		*ret = newvalue;
		StringsMetaValue stringsvalue(newvalue);
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
	else {
		return s;
	}
}

Status RedisString::GetProperty(const std::string& property, uint64_t* out) {
	std::string value;
	db->GetProperty(property, &value);
	*out = std::strtoull(value.c_str(), nullptr, 10);
	return Status::OK();
}

Status RedisString::Decrby(const std::string_view& key, int64_t value, int64_t* ret) {
	std::string oldvalue;
	std::string newvalue;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldvalue);
		if (pstringsvalue.IsStale()) {
			*ret = -value;
			StringsMetaValue stringsvalue(std::to_string(value));
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
		else {
			int32_t timestamp = pstringsvalue.GetTimestamp();
			std::string olduservalue = pstringsvalue.GetValueToString();
			char* end = nullptr;
			int64_t ival = strtoll(olduservalue.c_str(), &end, 10);
			if (*end != 0) {
				return Status::Corruption("Value is not a integer");
			}

			if ((value >= 0 && LLONG_MAX - value < ival) ||
				(value < 0 && LLONG_MIN - value > ival)) {
				return Status::InvalidArgument("Overflow");
			}

			*ret = ival - value;
			newvalue = std::to_string(*ret);
			StringsMetaValue stringsvalue(newvalue);
			stringsvalue.SetTimestamp(timestamp);
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
	}
	else if (s.IsNotFound()) {
		*ret = -value;
		StringsMetaValue stringsvalue(std::to_string(value));
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
	else {
		return s;
	}
}

Status RedisString::Append(const std::string_view& key,
	const std::string_view& value, int32_t* ret) {
	std::string oldvalue;
	*ret = 0;
	HashLock l(&lockmgr, key);
	Status s = db->Get(ReadOptions(), key, &oldvalue);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&oldvalue);
		if (pstringsvalue.IsStale()) {
			*ret = value.size();
			StringsMetaValue stringsvalue(value);
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
		else {
			int32_t timestamp = pstringsvalue.GetTimestamp();
			std::string olduservalue = pstringsvalue.GetValueToString();
			std::string newvalue = olduservalue + std::string(value.data(), value.size());
			StringsMetaValue stringsvalue(newvalue);
			stringsvalue.SetTimestamp(timestamp);
			*ret = newvalue.size();
			return db->Put(WriteOptions(), key, stringsvalue.Encode());
		}
	}
	else if (s.IsNotFound()) {
		*ret = value.size();
		StringsMetaValue stringsvalue(value);
		return db->Put(WriteOptions(), key, stringsvalue.Encode());
	}
	return s;
}

int GetBitCount(const unsigned char* value, int64_t bytes) {
	int bitnum = 0;
	static const unsigned char bitsinbyte[256] =
	{ 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8 };
	for (int i = 0; i < bytes; i++) {
		bitnum += bitsinbyte[static_cast<unsigned int>(value[i])];
	}
	return bitnum;
}

Status RedisString::BitCount(const std::string_view& key, int64_t startoffset, int64_t endoffset,
	int32_t* ret, bool haverange) {
	*ret = 0;
	std::string value;
	Status s = db->Get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsMetaValue pstringsvalue(&value);
		if (pstringsvalue.IsStale()) {
			return Status::NotFound("Stale");
		}
		else {
			pstringsvalue.StripSuffix();
			const unsigned char* bitvalue =
				reinterpret_cast<const unsigned char*>(value.data());
			int64_t valuelength = value.length();
			if (haverange) {
				if (startoffset < 0) {
					startoffset = startoffset + valuelength;
				}
				if (endoffset < 0) {
					endoffset = endoffset + valuelength;
				}
				if (startoffset < 0) {
					startoffset = 0;
				}
				if (endoffset < 0) {
					endoffset = 0;
				}

				if (endoffset >= valuelength) {
					endoffset = valuelength - 1;
				}
				if (startoffset > endoffset) {
					return Status::OK();
				}
			}
			else {
				startoffset = 0;
				endoffset = std::max(valuelength - 1, static_cast<int64_t>(0));
			}
			*ret = GetBitCount(bitvalue + startoffset,
				endoffset - startoffset + 1);
		}
	}
	else {
		return s;
	}
	return Status::OK();
}

Status RedisString::BitOp(BitOpType op, const std::string& destkey,
	const std::vector<std::string>& srckeys, int64_t* ret) {

}

Status RedisString::CompactRange(const std::string_view* begin,
	const std::string_view* end) {
	db->CompactRange(begin, end);
	return Status::OK();
}


Status RedisString::BitPos(const std::string_view& key, int32_t bit, int64_t* ret) {

}

Status RedisString::BitPos(const std::string_view& key, int32_t bit,
	int64_t startoffset, int64_t * ret) {

}

Status RedisString::BitPos(const std::string_view& key, int32_t bit,
	int64_t startoffset, int64_t endoffset,
	int64_t* ret) {

}

Status RedisString::ScanKeyNum(KeyInfo* keyinfo) {
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

Status RedisString::ScanKeys(const std::string& pattern,
	std::vector<std::string>* keys) {
	std::string key;
	ReadOptions iteratoroptions;
	std::shared_ptr<Snapshot> snapshot;
	SnapshotLock ss(db, snapshot);
	iteratoroptions.snapshot = snapshot;
	iteratoroptions.fillcache = false;

	// Note: This is a string type and does not need to pass the column family as
	// a parameter, use the default column family
	std::shared_ptr<Iterator> iter = db->NewIterator(iteratoroptions);
	for (iter->SeekToFirst();
		iter->Valid();
		iter->Next()) {
		ParsedStringsMetaValue pstringsvalue(iter->value());
		if (!pstringsvalue.IsStale()) {
			key = ToString(iter->key());
			if (StringMatchLen(pattern.data(),
				pattern.size(), key.data(), key.size(), 0)) {
				keys->push_back(key);
			}
		}
	}
  	return Status::OK();
}