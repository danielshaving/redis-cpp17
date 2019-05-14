#include "redisdb.h"
#include "comparator.h"

RedisDB::RedisDB(const Options& options, const std::string& path)
	:options(options),
	path(path) {

}

RedisDB::~RedisDB() {

}

const Comparator* ListsDataKeyComparator() {
	static ListsDataKeyComparatorImpl l;;
	return &l;
}

const Comparator* ZSetsScoreKeyComparator() {
	static ZSetsScoreKeyComparatorImpl z;
	return &z;
}

Status RedisDB::Open() {
	options.env->CreateDir(path);

	{
		redisstring.reset(new RedisString(this, options, path + "/strings"));
		Status s = redisstring->Open();
		assert(s.ok());
	}

	{
		redishash.reset(new RedisHash(this, options, path + "/hash"));
		Status s = redishash->Open();
		assert(s.ok());
	}

	{
		Options ops = options;
		ops.comparator = ZSetsScoreKeyComparator();
		rediszset.reset(new RedisZset(this, ops, path + "/zset"));
		Status s = rediszset->Open();
		assert(s.ok());
	}

	{
		Options ops = options;
		ops.comparator = ListsDataKeyComparator();
		redislist.reset(new RedisList(this, ops, path + "/list"));
		Status s = redislist->Open();
		assert(s.ok());
	}

	{
		rediszset.reset(new RedisZset(this, options, path + "/set"));
		Status s = rediszset->Open();
		assert(s.ok());
	}

	return Status::OK();
}

Status RedisDB::Set(const std::string_view& key,
	const std::string_view& value) {
	return redisstring->Set(key, value);
}

Status RedisDB::Get(const std::string_view& key, std::string* value) {
	return redisstring->Get(key, value);
}

Status RedisDB::GetSet(const std::string_view& key, 
	const std::string_view& value, std::string* oldValue) {
	return redisstring->GetSet(key, value, oldValue);
}

Status RedisDB::Setxx(const std::string_view& key, 
	const std::string_view& value, int32_t* ret, const int32_t ttl) {
	return redisstring->Setxx(key, value, ret, ttl);
}

Status RedisDB::SetBit(const std::string_view& key, 
	int64_t offset, int32_t value, int32_t* ret) {
	return redisstring->SetBit(key, offset, value, ret);
}

Status RedisDB::GetBit(const std::string_view& key, int64_t offset, int32_t* ret) {
	return redisstring->GetBit(key, offset, ret);
}

Status RedisDB::Mset(const std::vector<KeyValue>& kvs) {
	return redisstring->Mset(kvs);
}

Status RedisDB::Mget(const std::vector<std::string>& keys,
	std::vector<ValueStatus> * vss) {
	return redisstring->Mget(keys, vss);
}

Status RedisDB::Setnx(const std::string_view& key,
	const std::string& value, int32_t* ret, const int32_t ttl) {
	return redisstring->Setnx(key, value, ret, ttl);
}

Status RedisDB::Msetnx(const std::vector<KeyValue>& kvs, int32_t* ret) {
	return redisstring->Msetnx(kvs, ret);
}

Status RedisDB::Setvx(const std::string_view& key, const std::string_view& value,
	const std::string_view& newValue, int32_t* ret, const int32_t TTL) {
	return redisstring->Setvx(key, value, newValue, ret, TTL);
}

int64_t RedisDB::Delete(const std::vector<std::string>& keys, 
	std::map<DataType, Status>* typeStatus) {
	Status s;
	int64_t Count = 0;
	bool Corruption = false;

	for (const auto& key : keys) {
		// Strings
		Status s = redisstring->Delete(key);
		if (s.ok()) {
			Count++;
		}
		else if (!s.IsNotFound()) {
			Corruption = true;
			(*typeStatus)[DataType::kStrings] = s;
		}
	}

	if (Corruption) {
		return -1;
	}
	else {
		return Count;
	}
}

Status RedisDB::Delvx(const std::string_view& key, const std::string_view& value, int32_t* ret) {
	return redisstring->Delvx(key, value, ret);
}

Status RedisDB::Setrange(const std::string_view& key, int64_t startOffset,
	const std::string_view& value, int32_t* ret) {
	return redisstring->Setrange(key, startOffset, value, ret);
}

Status RedisDB::Getrange(const std::string_view& key, int64_t startOffset, int64_t endOffset,
	std::string* ret) {
	return redisstring->Getrange(key, startOffset, endOffset, ret);
}

Status RedisDB::Hset(const std::string_view& key,
	const std::string_view& field, const std::string_view& value, int32_t* res) {
	return redishash->Hset(key, field, value, res);
}

Status RedisDB::Hgetall(const std::string_view& key, std::vector<FieldValue>* fvs) {
	return redishash->Hgetall(key, fvs);
}

Status RedisDB::Zadd(const std::string_view& key,
	const std::vector<ScoreMember> & scoremembers, int32_t* ret) {
	return rediszset->Zadd(key, scoremembers, ret);
}

Status RedisDB::Zrange(const std::string_view& key,
	int32_t start, int32_t stop, std::vector<ScoreMember>* scoremembers) {
	return rediszset->Zrange(key, start, stop, scoremembers);
}
