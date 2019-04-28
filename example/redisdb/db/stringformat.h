#pragma once

#include <string>
#include <ratio>
#include <chrono>
#include <string_view>
#include "coding.h"

class StringsValue {
public:
	StringsValue(const std::string_view &value)
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

enum DataType {
	kAll,
	kStrings,
	kHashes,
	kLists,
	kZSets,
	kSets
};
