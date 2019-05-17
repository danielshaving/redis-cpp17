#pragma once
#include <mutex>
#include <memory>
#include <string>
#include <string_view>
#include <iostream>
#include <iomanip>
#include <array>
#include <vector>
#include "db.h"

class StringsValue {
public:
	StringsValue(const std::string_view& value)
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

	void SetTimestamp(int32_t t = 0) {
		timestamp = t;
	}

	void SetRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
	}

	void SetVersion(int32_t v = 0) {
		version = v;
	}

	const std::string_view Encode() {
		size_t usize = value.size();
		size_t needed = usize + kDefaultValueSuffixLength;
		char* dst;

		if (needed <= sizeof(space)) {
			dst = space;
		}
		else {
			dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}

		start = dst;
		size_t len = AppendTimestampAndVersion();
		return std::string_view(start, len);
	}

	size_t AppendTimestampAndVersion() {
		size_t usize = value.size();
		char* dst = start;
		memcpy(dst, value.data(), usize);
		dst += usize;
		EncodeFixed32(dst, timestamp);
		return usize + sizeof(int32_t);
	}

private:
	char space[200];
	char* start;
	std::string_view value;
	int32_t version;
	int32_t timestamp;
	static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2;
};

class ParsedStringsValue {
public:
	ParsedStringsValue(std::string* str)
		:value(str),
		version(0),
		timestamp(0) {
		if (str->size() >= kStringsValueSuffixLength) {
			uservalue = std::string_view(str->data(), str->size() - kStringsValueSuffixLength);
			timestamp = DecodeFixed32(str->data() + str->size() - kStringsValueSuffixLength);
		}
	}

	ParsedStringsValue(const std::string_view& str)
		:value(nullptr),
		version(0),
		timestamp(0) {
		if (str.size() >= kStringsValueSuffixLength) {
			uservalue = std::string_view(str.data(), str.size() - kStringsValueSuffixLength);
			timestamp = DecodeFixed32(str.data() + str.size() - kStringsValueSuffixLength);
		}
	}

	~ParsedStringsValue() {

	}

	void StripSuffix() {
		if (value != nullptr) {
			value->erase(value->size() - kStringsValueSuffixLength, kStringsValueSuffixLength);
		}
	}

	void SetTimestampToValue() {
		if (value != nullptr) {
			char* dst = value->data() + value->size() - kStringsValueSuffixLength;
			EncodeFixed32(dst, timestamp);
		}
	}

	std::string_view GetValue() {
		return uservalue;
	}

	std::string GetValueToString() {
		return std::string(uservalue.data(), uservalue.size());
	}

	int32_t GetVersion() {
		return version;
	}

	void SetVersion(int32_t v) {
		version = v;
	}

	int32_t GetTimestamp() {
		return timestamp;
	}

	void SetTimestamp(int32_t t) {
		timestamp = t;
		SetTimestampToValue();
	}

	void SetRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
		SetTimestampToValue();
	}

	bool IsPermanentSurvival() {
		return timestamp == 0;
	}

	bool IsStale() {
		if (timestamp == 0) {
			return false;
		}
		return timestamp < time(0);
	}

private:
	std::string* value;
	std::string_view uservalue;
	int32_t version;
	int32_t timestamp;

	static const size_t kStringsValueSuffixLength = sizeof(int32_t);
};

const uint64_t InitalLeftIndex = 9223372036854775807;
const uint64_t InitalRightIndex = 9223372036854775808U;

class ListsValue {
public:
	explicit ListsValue(const std::string_view& uservalue)
		:value(uservalue),
		start(nullptr),
		leftindex(InitalLeftIndex),
		rightindex(InitalRightIndex),
		version(0),
		timestamp(0) {
	}

	~ListsValue() {
		if (start != space) {
			delete[] start;
		}
	}

	size_t AppendTimestampAndVersion() {
		size_t usize = value.size();
		char* dst = start;
		memcpy(dst, value.data(), usize);
		dst += usize;
		EncodeFixed32(dst, version);
		dst += sizeof(int32_t);
		EncodeFixed32(dst, timestamp);
		return usize + 2 * sizeof(int32_t);
	}

	size_t AppendIndex() {
		char* dst = start;
		dst += value.size() + 2 * sizeof(int32_t);
		EncodeFixed64(dst, leftindex);
		dst += sizeof(int64_t);
		EncodeFixed64(dst, leftindex);
		return 2 * sizeof(int64_t);
	}

	static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2 + sizeof(int64_t) * 2;

	const std::string_view Encode() {
		size_t usize = value.size();
		size_t needed = usize + kDefaultValueSuffixLength;
		char* dst;
		if (needed <= sizeof(space)) {
			dst = space;
		}
		else {
			dst = new char[needed];
		}

		start = dst;
		size_t len = AppendTimestampAndVersion() + AppendIndex();
		return std::string_view(start, len);
	}

	int32_t UpdateVersion() {
		int64_t unixTime = time(0);
		if (version >= static_cast<int32_t>(unixTime)) {
			version++;
		}
		else {
			version = static_cast<int32_t>(unixTime);
		}
		return version;
	}

	uint64_t GetLeftIndex() {
		return leftindex;
	}

	void ModifyLeftIndex(uint64_t index) {
		leftindex -= index;
	}

	uint64_t GetRightIndex() {
		return rightindex;
	}

	void ModifyRightIndex(uint64_t index) {
		rightindex += index;
	}

	void SetTimestamp(int32_t t = 0) {
		timestamp = t;
	}

	void SetRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
	}

	void SetVersion(int32_t v = 0) {
		version = v;
	}

private:
	uint64_t leftindex;
	uint64_t rightindex;

	char space[200];
	char* start;
	std::string_view value;
	int32_t version;
	int32_t timestamp;
};
class ParsedListsValue {
public:
	explicit ParsedListsValue(std::string* str)
		:value(str),
		version(0),
		timestamp(0),
		count(0),
		leftindex(0),
		rightindex(0) {
		assert(str->size() >= kListsMetaValueSuffixLength);
		if (str->size() >= kListsMetaValueSuffixLength) {
			uservalue = std::string_view(str->data(),
				str->size() - kListsMetaValueSuffixLength);
			version = DecodeFixed32(str->data() +
				str->size() - sizeof(int32_t) * 2 -
				sizeof(int64_t) * 2);
			timestamp = DecodeFixed32(str->data() +
				str->size() - sizeof(int32_t) -
				sizeof(int64_t) * 2);
			leftindex = DecodeFixed64(str->data() +
				str->size() - sizeof(int64_t) * 2);
			rightindex = DecodeFixed64(str->data() +
				str->size() - sizeof(int64_t));
		}
		count = DecodeFixed64(str->data());
	}
	
	explicit ParsedListsValue(const std::string_view& view)
		:value(nullptr),
		version(0),
		timestamp(0),
		count(0),
		leftindex(0),
		rightindex(0) {
		assert(view.size() >= kListsMetaValueSuffixLength);
		if (view.size() >= kListsMetaValueSuffixLength) {
			uservalue = std::string_view(view.data(),
				view.size() - kListsMetaValueSuffixLength);
			version = DecodeFixed32(view.data() +
				view.size() - sizeof(int32_t) * 2 -
				sizeof(int64_t) * 2);
			timestamp = DecodeFixed32(view.data() +
				view.size() - sizeof(int32_t) -
				sizeof(int64_t) * 2);
			leftindex = DecodeFixed64(view.data() +
				view.size() - sizeof(int64_t) * 2);
			rightindex = DecodeFixed64(view.data() +
				view.size() - sizeof(int64_t));
		}
		count = DecodeFixed64(view.data());
	}

	void StripSuffix() {
		if (value != nullptr) {
			value->erase(value->size() - kListsMetaValueSuffixLength,
				kListsMetaValueSuffixLength);
		}
	}

	void SetVersionToValue() {
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data()) + value->size() -
				kListsMetaValueSuffixLength;
			EncodeFixed32(dst, version);
		}
	}

	void SetTimestampToValue() {
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data()) + value->size() -
				sizeof(int32_t) - 2 * sizeof(int64_t);
			EncodeFixed32(dst, timestamp);
		}
	}

	void SetIndexToValue() {
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data()) + value->size() -
				2 * sizeof(int64_t);
			EncodeFixed64(dst, leftindex);
			dst += sizeof(int64_t);
			EncodeFixed64(dst, rightindex);
		}
	}

	static const size_t kListsMetaValueSuffixLength =
		2 * sizeof(int32_t) + 2 * sizeof(int64_t);

	int32_t InitialMetaValue() {
		this->SetCount(0);
		this->SetLeftIndex(InitalLeftIndex);
		this->SetRightIndex(InitalRightIndex);
		this->SetTimestamp(0);
		return this->UpdateVersion();
	}

	uint64_t GetCount() {
		return count;
	}

	void SetCount(uint64_t count) {
		this->count = count;
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data());
			EncodeFixed64(dst, count);
		}
	}

	void ModifyCount(uint64_t delta) {
		count += delta;
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data());
			EncodeFixed64(dst, count);
		}
	}

	int32_t UpdateVersion() {
		int64_t unixTime = time(0);
		if (version >= static_cast<int32_t>(unixTime)) {
			version++;
		}
		else {
			version = static_cast<int32_t>(unixTime);
		}

		SetVersionToValue();
		return version;
	}

	uint64_t GetLeftIndex() {
		return leftindex;
	}

	void SetLeftIndex(uint64_t index) {
		leftindex = index;
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data()) + value->size() -
				2 * sizeof(int64_t);
			EncodeFixed64(dst, leftindex);
		}
	}

	void ModifyLeftIndex(uint64_t index) {
		leftindex -= index;
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data()) + value->size() -
				2 * sizeof(int64_t);
			EncodeFixed64(dst, leftindex);
		}
	}

	uint64_t GetRightIndex() {
		return rightindex;
	}

	void SetRightIndex(uint64_t index) {
		rightindex = index;
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data()) + value->size() -
				sizeof(int64_t);
			EncodeFixed64(dst, rightindex);
		}
	}

	void ModifyRightIndex(uint64_t index) {
		rightindex += index;
		if (value != nullptr) {
			char* dst = const_cast<char*>(value->data()) + value->size() -
				sizeof(int64_t);
			EncodeFixed64(dst, rightindex);
		}
	}

	std::string_view GetValue() {
		return uservalue;
	}

	std::string GetValueToString() {
		return std::string(uservalue.data(), uservalue.size());
	}

	int32_t GetVersion() {
		return version;
	}

	void SetVersion(int32_t v) {
		version = v;
	}

	int32_t GetTimestamp() {
		return timestamp;
	}

	void SetTimestamp(int32_t t) {
		timestamp = t;
		SetTimestampToValue();
	}

	void SetRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
		SetTimestampToValue();
	}

	bool IsPermanentSurvival() {
		return timestamp == 0;
	}

	bool IsStale() {
		if (timestamp == 0) {
			return false;
		}
		return timestamp < time(0);
	}

private:
	std::string* value;
	std::string_view uservalue;
	int32_t version;
	int32_t timestamp;

	uint64_t count;
	uint64_t leftindex;
	uint64_t rightindex;
};

class BaseMetaValue {
public:
	BaseMetaValue(const std::string_view& value)
		:start(nullptr),
		value(value),
		version(0),
		timestamp(0) {

	}

	void SetTimestamp(int32_t t = 0) {
		timestamp = t;
	}

	void SetRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
	}

	void SetVersion(int32_t v = 0) {
		version = v;
	}

	const std::string_view Encode() {
		size_t usize = value.size();
		size_t needed = usize + kDefaultValueSuffixLength;
		char* dst;

		if (needed <= sizeof(space)) {
			dst = space;
		}
		else {
			dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}

		start = dst;
		size_t len = AppendTimestampAndVersion();
		return std::string_view(start, len);
	}

	int32_t UpdateVersion() {
		int64_t unixTime = time(0);
		if (version >= unixTime) {
			version++;
		}
		else {
			version = unixTime;
		}
		return version;
	}

	size_t AppendTimestampAndVersion() {
		size_t usize = value.size();
		char* dst = start;
		memcpy(dst, value.data(), usize);
		dst += usize;
		EncodeFixed32(dst, version);
		dst += sizeof(int32_t);
		EncodeFixed32(dst, timestamp);
		return usize + 2 * sizeof(int32_t);
	}

private:
	char space[200];
	char* start;
	std::string_view value;
	int32_t version;
	int32_t timestamp;
	static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2;
};

class ParsedBaseMetaValue {
public:
	ParsedBaseMetaValue(std::string* str)
		:value(str),
		version(0),
		timestamp(0) {
		if (str->size() >= kHashMetaValueSuffixLength) {
			uservalue = std::string_view(str->data(), str->size() - kBaseHashMetaValueSuffixLength);
			version = DecodeFixed32(str->data() + str->size() - sizeof(int32_t) * 2);
			timestamp = DecodeFixed32(str->data() + str->size() - sizeof(int32_t));
		}
		count = DecodeFixed32(str->data());
	}

	ParsedBaseMetaValue(const std::string_view& str)
		:value(nullptr),
		version(0),
		timestamp(0) {
		if (str.size() >= kHashMetaValueSuffixLength) {
			uservalue = std::string_view(str.data(), str.size() - kBaseHashMetaValueSuffixLength);
			version = DecodeFixed32(str.data() + str.size() - sizeof(int32_t) * 2);
			timestamp = DecodeFixed32(str.data() + str.size() - sizeof(int32_t));
		}
		count = DecodeFixed32(str.data());
	}

	~ParsedBaseMetaValue() {

	}

	void StripSuffix() {
		if (value != nullptr) {
			value->erase(value->size() - kBaseHashMetaValueSuffixLength, kBaseHashMetaValueSuffixLength);
		}
	}

	void SetVersionToValue() {
		if (value != nullptr) {
			char* dst = value->data() + value->size() - kBaseHashMetaValueSuffixLength;
			EncodeFixed32(dst, version);
		}
	}

	void SetTimestampToValue() {
		if (value != nullptr) {
			char* dst = value->data() + value->size() - sizeof(int32_t);
			EncodeFixed32(dst, timestamp);
		}
	}

	int32_t InitialMetaValue() {
		this->SetCount(0);
		this->SetTimestamp(0);
		return this->UpdateVersion();
	}

	void SetCount(int32_t count) {
		this->count = count;
		if (value != nullptr) {
			char* dst = value->data();
			EncodeFixed32(dst, this->count);
		}
	}

	std::string_view GetValue() {
		return uservalue;
	}

	std::string GetValueToString() {
		return std::string(uservalue.data(), uservalue.size());
	}

	int32_t GetVersion() {
		return version;
	}

	int32_t GetCount() {
		return count;
	}

	void ModifyCount(int32_t delta) {
		count += delta;
		if (value != nullptr) {
			char* dst = value->data();
			EncodeFixed32(dst, count);
		}
	}

	int32_t UpdateVersion() {
		int64_t unixTime = time(0);
		if (version >= unixTime) {
			version++;
		}
		else {
			version = unixTime;
		}

		SetVersionToValue();
		return version;
	}

	void SetVersion(int32_t v) {
		version = v;
	}

	int32_t GetTimestamp() {
		return timestamp;
	}

	void SetTimestamp(int32_t t) {
		timestamp = t;
		SetTimestampToValue();
	}

	void SetRelativeTimestamp(int32_t ttl) {
		timestamp = time(0) + ttl;
		SetTimestampToValue();
	}

	bool IsPermanentSurvival() {
		return timestamp == 0;
	}

	bool IsStale() {
		if (timestamp == 0) {
			return false;
		}
		return timestamp < time(0);
	}

private:
	std::string* value;
	std::string_view uservalue;
	int32_t version;
	int32_t timestamp;
	int32_t count;
	static const size_t kBaseHashMetaValueSuffixLength = 2 * sizeof(int32_t);
	static const size_t kHashMetaValueSuffixLength = sizeof(int32_t);
};

class BaseDataKey {
public:
	BaseDataKey(const std::string_view& key, int32_t v, const std::string_view& data)
		:start(nullptr), key(key), version(v), data(data) {

	}

	~BaseDataKey() {
		if (start != space) {
			delete[] start;
		}
	}

	const std::string_view Encode() {
		size_t usize = key.size() + data.size();
		size_t needed = usize + sizeof(int32_t) * 2;
		char* dst;
		if (needed <= sizeof(space)) {
			dst = space;
		}
		else {
			dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}

		start = dst;
		EncodeFixed32(dst, key.size());
		dst += sizeof(int32_t);
		memcpy(dst, key.data(), key.size());
		dst += key.size();
		EncodeFixed32(dst, version);
		dst += sizeof(int32_t);
		memcpy(dst, data.data(), data.size());
		return std::string_view(start, needed);
	}

private:
	char space[200];
	char* start;
	std::string_view key;
	int32_t version;
	std::string_view data;
};

class ParsedDataKey {
public:
	ParsedDataKey(const std::string* k) {
		const char* ptr = k->data();
		int32_t keyLen = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(k->data(), keyLen);
		ptr += keyLen;
		version = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		data = std::string_view(ptr, k->size() - keyLen - sizeof(int32_t) * 2);
	}

	ParsedDataKey(const std::string_view & k) {
		const char* ptr = k.data();
		int32_t keyLen = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(ptr, keyLen);
		ptr += keyLen;
		version = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		data = std::string_view(ptr, k.size() - keyLen - sizeof(int32_t) * 2);
	}

	~ParsedDataKey() {

	}

	std::string_view GetKey() {
		return key;
	}

	int32_t GetVersion() {
		return version;
	}

	std::string_view GetData() {
		return data;
	}

	std::string GetDataToString() {
		return std::string(data.data(), data.size());
	}

private:
	std::string_view key;
	int32_t version;
	std::string_view data;
};

/*
 * | <Key Size>  |     <Key>      |<Version> | <Score>  |     <Member>      |
 *      4 Bytes      key size Bytes    4 Bytes     8 Bytes    member size Bytes
 */
class ZSetsScoreKey {
public:
	ZSetsScoreKey(const std::string_view& key, int32_t version,
		double score, const std::string_view& member) :
		start(nullptr), key(key),
		version(version), score(score),
		member(member) {}

	~ZSetsScoreKey() {
		if (start != space) {
			delete[] start;
		}
	}

	const std::string_view Encode() {
		size_t needed = key.size() + member.size()
			+ sizeof(int32_t) * 2 + sizeof(uint64_t);
		char* dst = nullptr;
		if (needed <= sizeof(space)) {
			dst = space;
		}
		else {
			dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}

		start = dst;
		EncodeFixed32(dst, key.size());
		dst += sizeof(int32_t);
		memcpy(dst, key.data(), key.size());
		dst += key.size();
		EncodeFixed32(dst, version);
		dst += sizeof(int32_t);
		const void* addrscore = reinterpret_cast<const void*>(&score);
		EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addrscore));
		dst += sizeof(uint64_t);
		memcpy(dst, member.data(), member.size());
		return std::string_view(start, needed);
	}

private:
	char space[200];
	char* start;
	std::string_view key;
	int32_t version;
	double score;
	std::string_view member;
};

class ParsedZSetsScoreKey {
public:
	explicit ParsedZSetsScoreKey(const std::string* k) {
		const char* ptr = k->data();
		int32_t keylen = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(k->data(), keylen);
		ptr += keylen;
		version = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);

		uint64_t tmp = DecodeFixed64(ptr);
		const void* ptrtmp = reinterpret_cast<const void*>(&tmp);
		score = *reinterpret_cast<const double*>(ptrtmp);
		ptr += sizeof(uint64_t);
		member = std::string_view(ptr, k->size() - keylen
			- 2 * sizeof(int32_t) - sizeof(uint64_t));
	}

	explicit ParsedZSetsScoreKey(const std::string_view & k) {
		const char* ptr = k.data();
		int32_t keylen = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(ptr, keylen);
		ptr += keylen;
		version = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);

		uint64_t tmp = DecodeFixed64(ptr);
		const void* ptrtmp = reinterpret_cast<const void*>(&tmp);
		score = *reinterpret_cast<const double*>(ptrtmp);
		ptr += sizeof(uint64_t);
		member = std::string_view(ptr, k.size() - keylen
			- 2 * sizeof(int32_t) - sizeof(uint64_t));
	}

	std::string_view GetKey() {
		return key;
	}

	int32_t GetVersion() const {
		return version;
	}

	double GetScore() const {
		return score;
	}

	std::string_view GetMember() {
		return member;
	}

	std::string GetMemberToString() {
		return std::string(member.data(), member.size());
	}
private:
	std::string_view key;
	int32_t version;
	double score;
	std::string_view member;
};

class ListsDataKey {
public:
	ListsDataKey(const std::string_view& key, int32_t version, uint64_t index) :
		start(nullptr), key(key), version(version), index(index) {
	}

	~ListsDataKey() {
		if (start != space) {
			delete[] start;
		}
	}

	const std::string_view Encode() {
		size_t usize = key.size();
		size_t needed = usize + sizeof(int32_t) * 2 + sizeof(uint64_t);
		char* dst;
		if (needed <= sizeof(space)) {
			dst = space;
		}
		else {
			dst = new char[needed];

			// Need to allocate space, delete previous space
			if (start != space) {
				delete[] start;
			}
		}

		start = dst;
		EncodeFixed32(dst, key.size());
		dst += sizeof(int32_t);
		memcpy(dst, key.data(), key.size());
		dst += key.size();
		EncodeFixed32(dst, version);
		dst += sizeof(int32_t);
		EncodeFixed64(dst, index);
		return std::string_view(start, needed);
	}

private:
	char space[200];
	char* start;
	std::string_view key;
	int32_t version;
	uint64_t index;
};

class ParsedListsDataKey {
public:
	explicit ParsedListsDataKey(const std::string* k) {
		const char* ptr = k->data();
		int32_t keylen = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(ptr, keylen);
		ptr += keylen;
		version = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		index = DecodeFixed64(ptr);
	}

	explicit ParsedListsDataKey(const std::string_view& k) {
		const char* ptr = k.data();
		int32_t keylen = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		key = std::string_view(ptr, keylen);
		ptr += keylen;
		version = DecodeFixed32(ptr);
		ptr += sizeof(int32_t);
		index = DecodeFixed64(ptr);
	}

	~ParsedListsDataKey() {

	}

	std::string_view Getkey() {
		return key;
	}

	int32_t GetVersion() {
		return version;
	}

	uint64_t GetIndex() {
		return index;
	}

private:
	std::string_view key;
	int32_t version;
	uint64_t index;
};

typedef BaseDataKey HashesDataKey;
typedef BaseDataKey SetsMemberKey;
typedef BaseDataKey ZSetsMemberKey;
typedef BaseMetaValue HashesMetaValue;
typedef ParsedBaseMetaValue ParsedHashesMetaValue;
typedef BaseMetaValue SetsMetaValue;
typedef ParsedBaseMetaValue ParsedSetsMetaValue;
typedef BaseMetaValue ZSetsMetaValue;
typedef ParsedBaseMetaValue ParsedZSetsMetaValue;

struct KeyValue {
	std::string key;
	std::string value;

	bool operator == (const KeyValue& kv) const {
		return (kv.key == key && kv.value == value);
	}

	bool operator< (const KeyValue & kv) const {
		return key < kv.key;
	}
};

struct ValueStatus {
	std::string value;
	Status status;
	bool operator == (const ValueStatus& vs) const {
		return (vs.value == value && vs.status == status);
	}
};

struct FieldValue {
	std::string field;
	std::string value;
	bool operator == (const FieldValue& fv) const {
		return (fv.field == field && fv.value == value);
	}
};

struct ScoreMember {
	double score;
	std::string member;
	bool operator == (const ScoreMember& sm) const {
		return (sm.score == score && sm.member == member);
	}
};

struct KeyInfo {
	uint64_t keys;
	uint64_t expires;
	uint64_t avgttl;
	uint64_t invaildkeys;
};

enum DataType {
	kAll,
	kStrings,
	kHashes,
	kLists,
	kZSets,
	kSets
};

enum BitOpType {
	kBitOpAnd = 1,
	kBitOpOr,
	kBitOpXor,
	kBitOpNot,
	kBitOpDefault
};

enum AGGREGATE {
	SUM,
	MIN,
	MAX
};

enum BeforeOrAfter {
  Before,
  After
};
