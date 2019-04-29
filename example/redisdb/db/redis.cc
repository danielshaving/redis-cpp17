#include "redis.h"

Redis::Redis(const Options &options, const std::string &path) 
	:env(new Env()),
	options(options),
	path(path) {
	
}

Redis::~Redis() {
	
}

Status Redis::open() {
	env->createDir(path);
	
	{
		redisString.reset(new RedisString(this, options, path + "/strings"));
		Status s = redisString->open();
		assert(s.ok());
	}
	
	{
		redisHash.reset(new RedisHash(this, options, path + "/hash"));
		Status s = redisHash->open();
		assert(s.ok());
	}
	
	return Status::OK();
}

Status Redis::set(const std::string_view &key, const std::string_view &value) {
	return redisString->set(key, value);
}

Status Redis::get(const std::string_view &key, std::string *value) {
	return redisString->get(key, value);
}

Status Redis::getSet(const std::string_view &key, const std::string_view &value, std::string *oldValue) {
	return redisString->getSet(key, value, oldValue);
}

Status Redis::setxx(const std::string_view &key, const std::string_view &value, int32_t *ret, const int32_t ttl) {
	return redisString->setxx(key, value, ret, ttl);
}

Status Redis::setBit(const std::string_view &key, int64_t offset, int32_t value, int32_t *ret) {
	return redisString->setBit(key, offset, value, ret);
}

Status Redis::getBit(const std::string_view &key, int64_t offset, int32_t *ret) {
	return redisString->getBit(key, offset, ret);
}

Status Redis::mset(const std::vector <KeyValue> &kvs) {
	return redisString->mset(kvs);
}

Status Redis::mget(const std::vector <std::string> &keys,
			  std::vector <ValueStatus> *vss) {
	return redisString->mget(keys, vss);
}

Status Redis::setnx(const std::string_view &key, const std::string &value, int32_t *ret, const int32_t ttl) {
	return redisString->setnx(key, value, ret, ttl);
}

Status Redis::msetnx(const std::vector <KeyValue> &kvs, int32_t *ret) {
	return redisString->msetnx(kvs, ret);
}

Status Redis::setvx(const std::string_view &key, const std::string_view &value, const std::string_view &newValue, int32_t *ret, const int32_t ttl) {
	return redisString->setvx(key, value, newValue, ret, ttl);
}

int64_t Redis::del(const std::vector <std::string> &keys, std::map<DataType, Status> *typeStatus) {
	Status s;
	int64_t count = 0;
	bool corruption = false;

	for (const auto &key : keys) {
		// Strings
		Status s = redisString->del(key);
		if (s.ok()) {
			count++;
		} else if (!s.isNotFound()) {
			corruption = true;
			(*typeStatus)[DataType::kStrings] = s;
		}
	}

	if (corruption) {
		return -1;
	} else {
		return count;
	}
}

Status Redis::delvx(const std::string_view &key, const std::string_view &value, int32_t *ret) {
	return redisString->delvx(key, value, ret);
}

Status Redis::setrange(const std::string_view &key, int64_t startOffset,
			  const std::string_view &value, int32_t *ret) {
	return redisString->setrange(key, startOffset, value, ret);
}

Status Redis::getrange(const std::string_view &key, int64_t startOffset, int64_t endOffset,
			  std::string *ret) {
	return redisString->getrange(key, startOffset, endOffset, ret);
}

Status Redis::hset(const std::string_view &key, const std::string_view &field, const std::string_view &value, int32_t *res) {
	return redisHash->hset(key, field, value, res);
}

Status Redis::hgetall(const std::string_view &key, std::vector <FieldValue> *fvs) {
	return redisHash->hgetall(key, fvs);
}