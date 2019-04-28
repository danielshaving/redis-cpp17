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
	
	redisString.reset(new RedisString(this, options, path + "/strings"));
	Status s = redisString->open();
	assert(s.ok());
	
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

Status Redis::mset(const std::vector<KeyValue> &kvs) {
	return redisString->mset(kvs);
}

Status Redis::mget(const std::vector<std::string> &keys,
			  std::vector<ValueStatus> *vss) {
	return redisString->mget(keys, vss);
}

