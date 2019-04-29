#pragma once
#include <memory>
#include <string>

class StringsValue {
public:
	StringsValue(const std::string_view &value)
	:start(nullptr), 
	value(value),
	version(0), 
	timestamp(0) {
		
	}
	
	~StringsValue() {
		if (start != space) {
			delete[] start;
		}
	}
	
	void setTimestamp(int32_t t = 0) {
		timestamp = t;
	}
	
	void setRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
	}
	
	void setVersion(int32_t v = 0) {
		version = v;
	}		
	
	const std::string_view encode() {
		size_t usize = value.size();
		size_t needed = usize + kDefaultValueSuffixLength;
		char *dst;
		
		if (needed <= sizeof(space)) {
			dst = space;
		} else {
			dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}
		
		start = dst;
		size_t len = appendTimestampAndVersion();
		return std::string_view(start, len);
	}
	
	size_t appendTimestampAndVersion() {
		size_t usize = value.size();
		char *dst = start;
		memcpy(dst, value.data(), usize);
		dst += usize;
		encodeFixed32(dst, timestamp);
		return usize + sizeof(int32_t);
	}
  
private:
	char space[200];
	char *start;
	std::string_view value;
	int32_t version;
	int32_t timestamp;
	static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2;
};

class ParsedStringsValue {
public:
	ParsedStringsValue(std::string *str)
	:value(str),
	version(0),
	timestamp(0) {
		if (str->size() >= kStringsValueSuffixLength) {
			userValue = std::string_view(str->data(), str->size() - kStringsValueSuffixLength);
			timestamp = decodeFixed32(str->data() + str->size() - kStringsValueSuffixLength);
		}
	}

	ParsedStringsValue(const std::string_view &str)
		:value(nullptr),
		version(0),
		timestamp(0) {
		if (str.size() >= kStringsValueSuffixLength) {
			userValue = std::string_view(str.data(), str.size() - kStringsValueSuffixLength);
			timestamp = decodeFixed32(str.data() + str.size() - kStringsValueSuffixLength);
		}
	}
  
	~ParsedStringsValue() {
		
	}
	
	void stripSuffix() {
		if (value != nullptr) {
			value->erase(value->size() - kStringsValueSuffixLength, kStringsValueSuffixLength);
		}
	}
	
	void setTimestampToValue() {
		if (value != nullptr) {
			char *dst = value->data() + value->size() - kStringsValueSuffixLength;
			encodeFixed32(dst, timestamp);
		}
	}

	std::string_view getValue() {
		return userValue;
	}
	
	std::string getValueToString() {
		return std::string(userValue.data(), userValue.size());
	}
	
	int32_t getVersion() {
		return version;
	}
	
	void setVersion(int32_t v) {
		version = v;
	}		
	
	int32_t getTimestamp() {
		return timestamp;
	}
	
	void setTimestamp(int32_t t) {
		timestamp = t;
		setTimestampToValue();
	}
	
	void setRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
		setTimestampToValue();
	}
  
	bool isPermanentSurvival() {
		return timestamp == 0;
	}
	
	bool isStale() {
		if (timestamp == 0) {
			return false;
		}
		return timestamp < time(0);
	}

private:
	std::string *value;
	std::string_view userValue;
	int32_t version;
	int32_t timestamp;
	
	static const size_t kStringsValueSuffixLength = sizeof(int32_t);
};

class HashesMetaValue {
public:
	HashesMetaValue(const std::string_view &value)
	:start(nullptr), 
	value(value),
	version(0), 
	timestamp(0) {
		
	}
	
	void setTimestamp(int32_t t = 0) {
		timestamp = t;
	}
	
	void setRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
	}
	
	void setVersion(int32_t v = 0) {
		version = v;
	}		
	
	const std::string_view encode() {
		size_t usize = value.size();
		size_t needed = usize + kDefaultValueSuffixLength;
		char *dst;
		
		if (needed <= sizeof(space)) {
			dst = space;
		} else {
			dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}
		
		start = dst;
		size_t len = appendTimestampAndVersion();
		return std::string_view(start, len);
	}
	
	int32_t updateVersion() {
		int64_t unixTime = time(0);
		if (version >= unixTime) {
			version++;
		} else {
			version = unixTime;
		}
		return version;
	}
  
	size_t appendTimestampAndVersion() {
		size_t usize = value.size();
		char *dst = start;
		memcpy(dst, value.data(), usize);
		dst += usize;
		encodeFixed32(dst, version);
		dst += sizeof(int32_t);
		encodeFixed32(dst, timestamp);
		return usize + 2 * sizeof(int32_t);
	}
  
private:
	char space[200];
	char *start;
	std::string_view value;
	int32_t version;
	int32_t timestamp;
	static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2;
};

class ParsedHashesMetaValue {
public:
	ParsedHashesMetaValue(std::string *str)
	:value(str),
	version(0),
	timestamp(0) {
		if (str->size() >= kHashMetaValueSuffixLength) {
			userValue = std::string_view(str->data(), str->size() - kBaseHashMetaValueSuffixLength);
			version = decodeFixed32(str->data() + str->size() - sizeof(int32_t) * 2);
			timestamp = decodeFixed32(str->data() + str->size() - sizeof(int32_t));
		}
		count = decodeFixed32(str->data());
	}

	ParsedHashesMetaValue(const std::string_view &str)
		:value(nullptr),
		version(0),
		timestamp(0) {
		if (str.size() >= kHashMetaValueSuffixLength) {
			userValue = std::string_view(str.data(), str.size() - kBaseHashMetaValueSuffixLength);
			version = decodeFixed32(str.data() + str.size() - sizeof(int32_t) * 2);
			timestamp = decodeFixed32(str.data() + str.size() - sizeof(int32_t));
		}
		count = decodeFixed32(str.data());
	}
  
	~ParsedHashesMetaValue() {
		
	}
	
	void stripSuffix() {
		if (value != nullptr) {
			value->erase(value->size() - kBaseHashMetaValueSuffixLength, kBaseHashMetaValueSuffixLength);
		}
	}
	
	void setVersionToValue() {
		if (value != nullptr) {
			char *dst = value->data() + value->size() - kBaseHashMetaValueSuffixLength;
			encodeFixed32(dst, version);
		}
	}
	
	void setTimestampToValue() {
		if (value != nullptr) {
			char *dst = value->data() + value->size() - sizeof(int32_t);
			encodeFixed32(dst, timestamp);
		}
	}
	
	int32_t initialMetaValue() {
		this->setCount(0);
		this->setTimestamp(0);
		return this->updateVersion();
	}

	void setCount(int32_t count) {
		count = count;
		if (value != nullptr) {
			char *dst = value->data();
			encodeFixed32(dst, count);
		}
	}

	std::string_view getValue() {
		return userValue;
	}
	
	std::string getValueToString() {
		return std::string(userValue.data(), userValue.size());
	}
	
	int32_t getVersion() {
		return version;
	}
	
	int32_t getCount() {
		return count;
	}
	
	void modifyCount(int32_t delta) {
		count += delta;
		if (value != nullptr) {
			char *dst = value->data();
			encodeFixed32(dst, count);
		}
	}
  
	int32_t updateVersion() {
		int64_t unixTime = time(0);
		if (version >= unixTime) {
			version++;
		} else {
			version = unixTime;
		}
		
		setVersionToValue();
		return version;
	}

	void setVersion(int32_t v) {
		version = v;
	}		
	
	int32_t getTimestamp() {
		return timestamp;
	}
	
	void setTimestamp(int32_t t) {
		timestamp = t;
		setTimestampToValue();
	}
	
	void setRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
		setTimestampToValue();
	}
  
	bool isPermanentSurvival() {
		return timestamp == 0;
	}
	
	bool isStale() {
		if (timestamp == 0) {
			return false;
		}
		return timestamp < time(0);
	}

private:
	std::string *value;
	std::string_view userValue;
	int32_t version;
	int32_t timestamp;
	int32_t count;
	static const size_t kBaseHashMetaValueSuffixLength = 2 * sizeof(int32_t);
	static const size_t kHashMetaValueSuffixLength = sizeof(int32_t);
};

class BaseDataKey {
public:
	BaseDataKey(const std::string_view &key, int32_t v, const std::string_view &data)
	:start(nullptr), key(key), version(v), data(data) {

	}

	~BaseDataKey() {
		if (start != space) {
			  delete[] start;
		}
	}

	const std::string_view encode() {
		size_t usize = key.size() + data.size();
		size_t needed = usize + sizeof(int32_t) * 2;
		char *dst;
		if (needed <= sizeof(space)) {
			dst = space;
		} else {
		    dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}

		start = dst;
		encodeFixed32(dst, key.size());
		dst += sizeof(int32_t);
		memcpy(dst, key.data(), key.size());
		dst += key.size();
		encodeFixed32(dst, version);
		dst += sizeof(int32_t);
		memcpy(dst, data.data(), data.size());
		return std::string_view(start, needed);
	}

private:
	char space[200];
	char *start;
	std::string_view key;
	int32_t version;
	std::string_view data;
};

class ParsedDataKey {
public:
	ParsedDataKey(const std::string *k) {
		const char *ptr = k->data();
		int32_t keyLen = decodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(k->data(), keyLen);
		ptr += keyLen;
		version = decodeFixed32(ptr);
		ptr += sizeof(int32_t);
		data = std::string_view(ptr, k->size() - keyLen - sizeof(int32_t) * 2);
	}

	ParsedDataKey(const std::string_view &k) {
		const char *ptr = k.data();
		int32_t keyLen = decodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(ptr, keyLen);
		ptr += keyLen;
		version = decodeFixed32(ptr);
		ptr += sizeof(int32_t);
		data = std::string_view(ptr, k.size() - keyLen - sizeof(int32_t) * 2);
	}

	~ParsedDataKey() {
		
	}
  
	std::string_view getKey() {
		return key;
	}

	int32_t getVersion() {
		return version;
	}

	std::string_view getData() {
		return data;
	}

	std::string getDataToString() {
		return std::string(data.data(), data.size());
	}

private:
	std::string_view key;
	int32_t version;
	std::string_view data;
};

typedef BaseDataKey HashesDataKey;
typedef BaseDataKey SetsMemberKey;
typedef BaseDataKey ZSetsMemberKey;

struct KeyValue {
	std::string key;
	std::string value;
	
	bool operator == (const KeyValue &kv) const {
		return (kv.key == key && kv.value == value);
	}
	
	bool operator < (const KeyValue &kv) const {
		return key < kv.key;
	}
};

struct ValueStatus {
	std::string value;
	Status status;
	bool operator == (const ValueStatus &vs) const {
		return (vs.value == value && vs.status == status);
	}
};

struct FieldValue {
	std::string field;
	std::string value;
	bool operator == (const FieldValue &fv) const {
		return (fv.field == field && fv.value == value);
	}
};

enum DataType {
	kAll,
	kStrings,
	kHashes,
	kLists,
	kZSets,
	kSets
};