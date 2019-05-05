#include "redisdb.h"

RedisDB::RedisDB(const Options& options, const std::string& path)
	:env(new Env()),
	options(options),
	path(path) {

}

RedisDB::~RedisDB() {

}

Status RedisDB::open() {
	env->createDir(path);

	{
		redisstring.reset(new RedisString(this, options, path + "/strings"));
		Status s = redisstring->open();
		assert(s.ok());
	}

	{
		redishash.reset(new RedisHash(this, options, path + "/hash"));
		Status s = redishash->open();
		assert(s.ok());
	}

	return Status::OK();
}

Status RedisDB::set(const std::string_view& key, const std::string_view& value) {
	return redisstring->set(key, value);
}

Status RedisDB::get(const std::string_view& key, std::string* value) {
	return redisstring->get(key, value);
}

Status RedisDB::getset(const std::string_view& key, const std::string_view& value, std::string* oldValue) {
	return redisstring->getset(key, value, oldValue);
}

Status RedisDB::setxx(const std::string_view& key, const std::string_view& value, int32_t* ret, const int32_t ttl) {
	return redisstring->setxx(key, value, ret, ttl);
}

Status RedisDB::setbit(const std::string_view& key, int64_t offset, int32_t value, int32_t* ret) {
	return redisstring->setbit(key, offset, value, ret);
}

Status RedisDB::getbit(const std::string_view& key, int64_t offset, int32_t* ret) {
	return redisstring->getbit(key, offset, ret);
}

Status RedisDB::mset(const std::vector<KeyValue>& kvs) {
	return redisstring->mset(kvs);
}

Status RedisDB::mget(const std::vector<std::string>& keys,
	std::vector<ValueStatus> * vss) {
	return redisstring->mget(keys, vss);
}

Status RedisDB::setnx(const std::string_view& key, const std::string& value, int32_t* ret, const int32_t ttl) {
	return redisstring->setnx(key, value, ret, ttl);
}

Status RedisDB::msetnx(const std::vector<KeyValue>& kvs, int32_t* ret) {
	return redisstring->msetnx(kvs, ret);
}

Status RedisDB::setvx(const std::string_view& key, const std::string_view& value,
	const std::string_view& newValue, int32_t* ret, const int32_t ttl) {
	return redisstring->setvx(key, value, newValue, ret, ttl);
}

int64_t RedisDB::del(const std::vector<std::string>& keys, std::map<DataType, Status>* typeStatus) {
	Status s;
	int64_t count = 0;
	bool corruption = false;

	for (const auto& key : keys) {
		// Strings
		Status s = redisstring->del(key);
		if (s.ok()) {
			count++;
		}
		else if (!s.isNotFound()) {
			corruption = true;
			(*typeStatus)[DataType::kStrings] = s;
		}
	}

	if (corruption) {
		return -1;
	}
	else {
		return count;
	}
}

Status RedisDB::delvx(const std::string_view& key, const std::string_view& value, int32_t* ret) {
	return redisstring->delvx(key, value, ret);
}

Status RedisDB::setrange(const std::string_view& key, int64_t startOffset,
	const std::string_view & value, int32_t * ret) {
	return redisstring->setrange(key, startOffset, value, ret);
}

Status RedisDB::getrange(const std::string_view& key, int64_t startOffset, int64_t endOffset,
	std::string * ret) {
	return redisstring->getrange(key, startOffset, endOffset, ret);
}

Status RedisDB::hset(const std::string_view& key, const std::string_view& field, const std::string_view& value, int32_t* res) {
	return redishash->hset(key, field, value, res);
}

Status RedisDB::hgetall(const std::string_view& key, std::vector<FieldValue>* fvs) {
	return redishash->hgetall(key, fvs);
}

Status RedisDB::zadd(const std::string_view& key,
	const std::vector<ScoreMember> & scoremembers, int32_t* ret) {
	return rediszset->zadd(key, scoremembers, ret);
}

Status RedisDB::zrange(const std::string_view& key, int32_t start, int32_t stop, std::vector<ScoreMember>* scoremembers) {
	return rediszset->zrange(key, start, stop, scoremembers);
}
