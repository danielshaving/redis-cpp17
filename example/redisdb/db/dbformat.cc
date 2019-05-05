#include "dbformat.h"
#include "coding.h"
#include "logging.h"

static uint64_t packSequenceAndType(uint64_t seq, ValueType t) {
	assert(seq<= kMaxSequenceNumber);
	assert(t<= kValueTypeForSeek);
	return (seq<< 8) | t;
}

void appendInternalKey(std::string* result, const ParsedInternalKey& key) {
	result->append(key.userKey.data(), key.userKey.size());
	putFixed64(result, packSequenceAndType(key.sequence, key.type));
}

bool parseInternalKey(const std::string_view& internalKey, ParsedInternalKey* result) {
	const size_t n = internalKey.size();
	if (n< 8) return false;
	uint64_t num = decodeFixed64(internalKey.data() + n - 8);
	unsigned char c = num & 0xff;
	result->sequence = num >> 8;
	result->type = static_cast<ValueType>(c);
	result->userKey = std::string_view(internalKey.data(), n - 8);
	return (c<= static_cast<unsigned char>(kTypeValue));
}

std::string ParsedInternalKey::debugString() const {
	char buf[50];
	snprintf(buf, sizeof(buf), "' @ %llu : %d",
		(uint64_t)sequence,
		int(type));
	std::string result = "'";
	result += escapeString(std::string(userKey.data(), userKey.size()));
	result += buf;
	return result;
}

void BytewiseComparatorImpl::findShortestSeparator(
	std::string* start, const std::string_view& limit) const {
	// Find length of common prefix
	size_t minLength;
	if (start->size()< limit.size()) {
		minLength = start->size();
	}
	else {
		minLength = limit.size();
	}

	size_t diffIndex = 0;
	while ((diffIndex< minLength) &&
		((*start)[diffIndex] == limit[diffIndex])) {
		diffIndex++;
	}

	if (diffIndex >= minLength) {
		// Do not shorten if one string is a prefix of the other
	}
	else {
		uint8_t diffByte = static_cast<uint8_t>((*start)[diffIndex]);
		if (diffByte< static_cast<uint8_t>(0xff) &&
			diffByte + 1< static_cast<uint8_t>(limit[diffIndex])) {
			(*start)[diffIndex]++;
			start->resize(diffIndex + 1);
			assert(this->compare(*start, limit)< 0);
		}
	}
}

const char* InternalKeyComparator::name() const {
	return "leveldb.InternalKeyComparator";
}

void BytewiseComparatorImpl::findShortSuccessor(std::string* key) const {
	// Find first character that can be incremented
	size_t n = key->size();
	for (size_t i = 0; i< n; i++) {
		const uint8_t byte = (*key)[i];
		if (byte != static_cast<uint8_t>(0xff)) {
			(*key)[i] = byte + 1;
			key->resize(i + 1);
			return;
		}
	}
}

void InternalKeyComparator::findShortestSeparator(std::string* start, const std::string_view& limit) const {
	// Attempt to shorten the user portion of the key
	std::string_view userStart = extractUserKey(*start);
	std::string_view userLimit = extractUserKey(limit);
	std::string tmp(userStart.data(), userStart.size());
	comparator->findShortestSeparator(&tmp, userLimit);
	if (tmp.size()< userStart.size() &&
		comparator->compare(userStart, tmp)< 0) {
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
		putFixed64(&tmp, packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
		assert(this->compare(*start, tmp)< 0);
		assert(this->compare(tmp, limit)< 0);
		start->swap(tmp);
	}
}

void InternalKeyComparator::findShortSuccessor(std::string* key) const {
	std::string_view userKey = extractUserKey(*key);
	std::string tmp(userKey.data(), userKey.size());
	comparator->findShortSuccessor(&tmp);
	if (tmp.size()< userKey.size() &&
		comparator->compare(userKey, tmp)< 0) {
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
		putFixed64(&tmp, packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
		assert(this->compare(*key, tmp)< 0);
		key->swap(tmp);
	}
}

int InternalKeyComparator::compare(const std::string_view& akey, const std::string_view& bkey) const {
	//    Order by:
	//    increasing user key (according to user-supplied comparator)
	//    decreasing sequence number
	//    decreasing type (though sequence# should be enough to disambiguate)
	int r = comparator->compare(extractUserKey(akey), extractUserKey(bkey));
	if (r == 0) {
		const uint64_t anum = decodeFixed64(akey.data() + akey.size() - 8);
		const uint64_t bnum = decodeFixed64(bkey.data() + bkey.size() - 8);

		if (anum > bnum) {
			r = -1;
		}
		else if (anum< bnum) {
			r = +1;
		}
	}
	return r;
}

std::string InternalKey::debugString() const {
	std::string result;
	ParsedInternalKey parsed;
	if (parseInternalKey(rep, &parsed)) {
		result = parsed.debugString();
	}
	else {
		result = "(bad)";
		result.append(escapeString(rep));
	}
	return result;
}

LookupKey::LookupKey(const std::string_view& userKey, uint64_t s) {
	size_t usize = userKey.size();
	size_t needed = usize + 13;  // A conservative estimate
	char* dst;
	if (needed<= sizeof(space)) {
		dst = space;
	}
	else {
		dst = (char*)malloc(needed);
	}

	start = dst;
	dst = encodeVarint32(dst, usize + 8);
	kstart = dst;
	memcpy(dst, userKey.data(), usize);
	dst += usize;
	encodeFixed64(dst, packSequenceAndType(s, kValueTypeForSeek));
	dst += 8;
	end = dst;
}

