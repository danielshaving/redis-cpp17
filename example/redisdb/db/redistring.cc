#include "redistring.h"
#include "redis.h"

RedisString::RedisString(Redis *redis, const Options &options, const std::string &path)
	:redis(redis),
	db(new DBImpl(options, path)) {
	
}

RedisString::~RedisString() {
	
}

Status RedisString::open() {
	return db->open();
}

Status RedisString::set(const std::string_view &key, const std::string_view &value) {
	StringsValue stringsvalue(value);
	return db->put(WriteOptions(), key, stringsvalue.encode());
}

Status RedisString::get(const std::string_view &key, std::string *value) {
	value->clear();
	Status s = db->get(ReadOptions(), key, value);
	if (s.ok()) {
		ParsedStringsValue stringsvalue(value);
		if (stringsvalue.isStale()) {
			value->clear();
			return Status::notFound("Stale");
		} else {
			stringsvalue.stripSuffix();
		}
	}
	return s;
}

Status RedisString::setxx(const std::string_view &key, const std::string_view &value, int32_t *ret, const int32_t ttl) {
	bool notfound = true;
	std::string oldValue;
	StringsValue stringsvalue(value);
	Status s = db->get(ReadOptions(), key, &oldValue);
	if (s.ok()) {
		ParsedStringsValue pstringvalue(oldValue);
		if (!pstringvalue.isStale()) {
			notfound = false;
		}
	} else if (!s.isNotFound()) {
		return s;
	}

	if (notfound) {
		*ret = 0;
		return s;
	} else {
		*ret = 1;
		if (ttl > 0) {
			stringsvalue.setRelativeTimestamp(ttl);
		}
		return db->put(WriteOptions(), key, stringsvalue.encode());
	}
}

Status RedisString::getSet(const std::string_view &key, const std::string_view &value, std::string *oldValue) {
	Status s = db->get(ReadOptions(), key, oldValue);
	if (s.ok()) {
		ParsedStringsValue pstringsvalue(oldValue);
		if (pstringsvalue.isStale()) {
			*oldValue = "";
		} else {
			pstringsvalue.stripSuffix();
		}
	} else if (!s.isNotFound()) {
		return s;
	}
	
	StringsValue stringsvalue(value);
	return db->put(WriteOptions(), key, stringsvalue.encode());
}

Status RedisString::setBit(const std::string_view &key, int64_t offset, int32_t value, int32_t *ret) {
	std::string metaValue;
	if (offset < 0) {
		return Status::invalidArgument("offset < 0");
	}

	Status s = db->get(ReadOptions(), key, &metaValue);
	if (s.ok() || s.isNotFound()) {
		std::string dataValue;
		if (s.ok()) {
			ParsedStringsValue pstringsvalue(&metaValue);
			if (!pstringsvalue.isStale()) {
				dataValue = pstringsvalue.getValueToString();
			}
		}
		size_t byte = offset >> 3;
		size_t bit = 7 - (offset & 0x7);
		char byteVal;
		size_t valueLength = dataValue.length();
		if (byte + 1 > valueLength) {
			*ret = 0;
			byteVal = 0;
		} else {
			*ret = ((dataValue[byte] & (1 << bit)) >> bit);
			byteVal = dataValue[byte];
		}
		
		if (*ret == value) {
			return Status::OK();
		}
		
		byteVal &= static_cast<char>(~(1 << bit));
		byteVal |= static_cast<char>((value & 0x1) << bit);
		if (byte + 1 <= valueLength) {
			dataValue.replace(byte, 1, &byteVal, 1);
		} else {
			dataValue.append(byte + 1 - valueLength - 1, 0);
			dataValue.append(1, byteVal);
		}
		
		StringsValue stringsvalue(dataValue);
		return db->put(WriteOptions(), key, stringsvalue.encode());
	} else {
		return s;
	}
}

Status RedisString::getBit(const std::string_view &key, int64_t offset, int32_t *ret) {
	std::string metaValue;
	Status s = db->get(ReadOptions(), key, &metaValue);
	if (s.ok() || s.isNotFound()) {
		std::string dataValue;
		if (s.ok()) {
			ParsedStringsValue pstringsvalue(&metaValue);
			if (pstringsvalue.isStale()) {
				*ret = 0;
				return Status::OK();
			} else {
				dataValue = pstringsvalue.getValueToString();
			}
		}
		
		size_t byte = offset >> 3;
		size_t bit = 7 - (offset & 0x7);
		if (byte + 1 > dataValue.length()) {
			*ret = 0;
		} else {
		    *ret = ((dataValue[byte] & (1 << bit)) >> bit);
		}
	} else {
		return s;
	}
	return Status::OK();
}

Status RedisString::mset(const std::vector<KeyValue> &kvs) {
	WriteBatch batch;
	for (const auto &kv : kvs) {
		StringsValue stringsvalue(kv.value);
		batch.put(kv.key, stringsvalue.encode());
	}
	return db->write(WriteOptions(), &batch);
}

Status RedisString::mget(const std::vector <std::string> &keys, std::vector <ValueStatus> *vss) {
	vss->clear();

	Status s;
	std::string value;
	for (const auto &key : keys) {
		s = db->get(ReadOptions(), key, &value);
		if (s.ok()) {
			ParsedStringsValue pstringsvalue(&value);
			if (pstringsvalue.isStale()) {
				vss->push_back({std::string(), Status::notFound("Stale")});
			} else {
				vss->push_back({pstringsvalue.getValueToString(), Status::OK()});
			}
		} else if (s.isNotFound()) {
			vss->push_back({std::string(), Status::notFound(" ")});
		} else {
			vss->clear();
			return s;
		}
	}
	return Status::OK();
}

Status RedisString::setnx(const std::string_view &key, const std::string &value, int32_t *ret, const int32_t ttl) {
	*ret = 0;
	std::string oldValue;
	Status s = db->get(ReadOptions(), key, &oldValue);
	if (s.ok()) {
		ParsedStringsValue pstringsvalue(&oldValue);
		if (pstringsvalue.isStale()) {
			StringsValue stringsvalue(value);
			if (ttl > 0) {
				stringsvalue.setRelativeTimestamp(ttl);
			}

			s = db->put(WriteOptions(), key, stringsvalue.encode());
			if (s.ok()) {
				*ret = 1;
			}
		}
	} else if (s.isNotFound()) {
		StringsValue stringsvalue(value);
		if (ttl > 0) {
			stringsvalue.setRelativeTimestamp(ttl);
		}

		s = db->put(WriteOptions(), key, stringsvalue.encode());
		if (s.ok()) {
			*ret = 1;
		}
	}
	return s;
}

Status RedisString::msetnx(const std::vector <KeyValue>& kvs, int32_t *ret) {
	Status s;
	bool exists = false;
	*ret = 0;
	std::string value;
	for (size_t i = 0; i < kvs.size(); i++) {
		s = db->get(ReadOptions(), kvs[i].key, &value);
		if (s.ok()) {
			ParsedStringsValue pstringsvalue(&value);
			if (!pstringsvalue.isStale()) {
				exists = true;
				break;
			}
		}
	}

	if (!exists) {
		s = mset(kvs);
		if (s.ok()) {
		  *ret = 1;
		}
	}
	return s;
}

Status RedisString::setvx(const std::string_view &key, const std::string_view &value, const std::string_view &newValue, int32_t *ret, const int32_t ttl) {
	*ret = 0;
	std::string oldValue;
	Status s = db->get(ReadOptions(), key, &oldValue);
	if (s.ok()) {
		ParsedStringsValue pstringsvalue(&oldValue);
		if (pstringsvalue.isStale()) {
			*ret = 0;
		} else {
			if (!value.compare(pstringsvalue.getValue())) {
				StringsValue stringsvalue(newValue);
				if (ttl > 0) {
					stringsvalue.setRelativeTimestamp(ttl);
				}

				s = db->put(WriteOptions(), key, stringsvalue.encode());
				if (!s.ok()) {
					return s;
				}

				*ret = 1;
			} else {
				*ret = -1;
			}
		}
	} else if (s.isNotFound()) {
		*ret = 0;
	} else {
		return s;
	}
	return Status::OK();
}

Status RedisString::del(const std::string_view &key) {
	std::string value;
	Status s = db->get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsValue pstringsvalue(&value);
		if (pstringsvalue.isStale()) {
			return Status::notFound("Stale");
		}
		return db->del(WriteOptions(), key);
	}
	return s;
}

Status RedisString::delvx(const std::string_view &key, const std::string_view &value, int32_t *ret) {
	*ret = 0;
	std::string oldValue;
	Status s = db->get(ReadOptions(), key, &oldValue);
	if (s.ok()) {
		ParsedStringsValue pstringsvalue(&oldValue);
		if (pstringsvalue.isStale()) {
			*ret = 0;
			return Status::notFound("Stale");
		} else {
			if (!value.compare(pstringsvalue.getValue())) {
			*ret = 1;
			return db->del(WriteOptions(), key);
		} else {
			*ret = -1;
		}
	}
	} else if (s.isNotFound()) {
		*ret = 0;
	}
	return s;
}

Status RedisString::setrange(const std::string_view &key, int64_t startOffset, const std::string_view &value, int32_t *ret) {
	std::string oldValue;
	std::string newValue;
	if (startOffset < 0) {
		return Status::invalidArgument("offset < 0");
	}

	Status s = db->get(ReadOptions(), key, &oldValue);
	if (s.ok()) {
		ParsedStringsValue pstringsvalue(&oldValue);
		pstringsvalue.stripSuffix();
		if (pstringsvalue.isStale()) {
			std::string tmp(startOffset, '\0');
			newValue = tmp.append(value.data());
			*ret = newValue.length();
		} else {
			if (static_cast<size_t>(startOffset) > oldValue.length()) {
				oldValue.resize(startOffset);
				newValue = oldValue.append(value.data());
			} else {
				std::string head = oldValue.substr(0, startOffset);
				std::string tail;
				if (startOffset + value.size() - 1 < oldValue.length() - 1) {
					tail = oldValue.substr(startOffset + value.size());
				}
				newValue = head + value.data() + tail;
			}
		}

		*ret = newValue.length();
		StringsValue stringsvalue(newValue);
		return db->put(WriteOptions(), key, stringsvalue.encode());
	} else if (s.isNotFound()) {
		std::string tmp(startOffset, '\0');
		newValue = tmp.append(value.data());
		*ret = newValue.length();
		StringsValue stringsvalue(newValue);
		return db->put(WriteOptions(), key, stringsvalue.encode());
	}
	return s;
}

Status RedisString::getrange(const std::string_view &key, int64_t startOffset, int64_t endOffset, std::string *ret) {
	*ret = "";
	std::string value;
	Status s = db->get(ReadOptions(), key, &value);
	if (s.ok()) {
		ParsedStringsValue pstringsvalue(&value);
		if (pstringsvalue.isStale()) {
			return Status::notFound("Stale");
		} else {
			pstringsvalue.stripSuffix();
			int64_t size = value.size();
			int64_t start = startOffset >= 0 ? startOffset : size + startOffset;
			int64_t endt = endOffset >= 0 ? endOffset : size + endOffset;
			if (start > size - 1 ||
			  (start != 0 && start > endt) ||
			  (start != 0 && endt < 0)) {
				return Status::OK();
			}

			if (start < 0) {
				start  = 0;
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
	} else {
		return s;
	}
}
