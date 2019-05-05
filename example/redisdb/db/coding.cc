#include "coding.h"

void encodeFixed32(char* buf, uint32_t value) {
	if (!kLittleEndian) {
		memcpy(buf, &value, sizeof(value));
	}
	else {
		buf[0] = value & 0xff;
		buf[1] = (value >> 8) & 0xff;
		buf[2] = (value >> 16) & 0xff;
		buf[3] = (value >> 24) & 0xff;
	}
}

void encodeFixed64(char* buf, uint64_t value) {
	if (!kLittleEndian) {
		memcpy(buf, &value, sizeof(value));
	}
	else {
		buf[0] = value & 0xff;
		buf[1] = (value >> 8) & 0xff;
		buf[2] = (value >> 16) & 0xff;
		buf[3] = (value >> 24) & 0xff;
		buf[4] = (value >> 32) & 0xff;
		buf[5] = (value >> 40) & 0xff;
		buf[6] = (value >> 48) & 0xff;
		buf[7] = (value >> 56) & 0xff;
	}
}

void putFixed32(std::string* dst, uint32_t value) {
	char buf[sizeof(value)];
	encodeFixed32(buf, value);
	dst->append(buf, sizeof(buf));
}

void putFixed64(std::string* dst, uint64_t value) {
	char buf[sizeof(value)];
	encodeFixed64(buf, value);
	dst->append(buf, sizeof(buf));
}

char* encodeVarint32(char* dst, uint32_t v) {
	// Operate on characters as unsigneds
	unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
	static const int B = 128;
	if (v< (1<< 7)) {
		*(ptr++) = v;
	}
	else if (v< (1<< 14)) {
		*(ptr++) = v | B;
		*(ptr++) = v >> 7;
	}
	else if (v< (1<< 21)) {
		*(ptr++) = v | B;
		*(ptr++) = (v >> 7) | B;
		*(ptr++) = v >> 14;
	}
	else if (v< (1<< 28)) {
		*(ptr++) = v | B;
		*(ptr++) = (v >> 7) | B;
		*(ptr++) = (v >> 14) | B;
		*(ptr++) = v >> 21;
	}
	else {
		*(ptr++) = v | B;
		*(ptr++) = (v >> 7) | B;
		*(ptr++) = (v >> 14) | B;
		*(ptr++) = (v >> 21) | B;
		*(ptr++) = v >> 28;
	}
	return reinterpret_cast<char*>(ptr);
}

void putVarint32(std::string* dst, uint32_t v) {
	char buf[5];
	char* ptr = encodeVarint32(buf, v);
	dst->append(buf, ptr - buf);
}

char* encodeVarint64(char* dst, uint64_t v) {
	static const int B = 128;
	unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
	while (v >= B) {
		*(ptr++) = (v & (B - 1)) | B;
		v >>= 7;
	}
	*(ptr++) = static_cast<unsigned char>(v);
	return reinterpret_cast<char*>(ptr);
}

void putVarint64(std::string* dst, uint64_t v) {
	char buf[10];
	char* ptr = encodeVarint64(buf, v);
	dst->append(buf, ptr - buf);
}

void putLengthPrefixedSlice(std::string* dst, const std::string_view & value) {
	putVarint32(dst, value.size());
	dst->append(value.data(), value.size());
}

int varintLength(uint64_t v) {
	int len = 1;
	while (v >= 128) {
		v >>= 7;
		len++;
	}
	return len;
}

const char* getVarint32PtrFallback(const char* p, const char* limit, uint32_t* value) {
	uint32_t result = 0;
	for (uint32_t shift = 0; shift<= 28 && p< limit; shift += 7) {
		uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
		p++;
		if (byte & 128) {
			// More bytes are present
			result |= ((byte & 127)<< shift);
		}
		else {
			result |= (byte<< shift);
			*value = result;
			return reinterpret_cast<const char*>(p);
		}
	}
	return nullptr;
}

bool getVarint32(std::string_view* input, uint32_t* value) {
	const char* p = input->data();
	const char* limit = p + input->size();
	const char* q = getVarint32Ptr(p, limit, value);
	if (q == nullptr) {
		return false;
	}
	else {
		*input = std::string_view(q, limit - q);
		return true;
	}
}

const char* getVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
	uint64_t result = 0;
	for (uint32_t shift = 0; shift<= 63 && p< limit; shift += 7) {
		uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
		p++;
		if (byte & 128) {
			// More bytes are present
			result |= ((byte & 127)<< shift);
		}
		else {
			result |= (byte<< shift);
			*value = result;
			return reinterpret_cast<const char*>(p);
		}
	}
	return nullptr;
}

bool getVarint64(std::string_view* input, uint64_t* value) {
	const char* p = input->data();
	const char* limit = p + input->size();
	const char* q = getVarint64Ptr(p, limit, value);
	if (q == nullptr) {
		return false;
	}
	else {
		*input = std::string_view(q, limit - q);
		return true;
	}
}

const char* getLengthPrefixedSlice(const char* p, const char* limit, std::string_view* result) {
	uint32_t len;
	p = getVarint32Ptr(p, limit, &len);
	if (p == nullptr) return nullptr;
	if (p + len > limit) return nullptr;
	*result = std::string_view(p, len);
	return p + len;
}

std::string_view getLengthPrefixedSlice(const char* data) {
	uint32_t len;
	const char* p = data;
	p = getVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
	return std::string_view(p, len);
}

bool getLengthPrefixedSlice(std::string_view* input, std::string_view* result) {
	uint32_t len;
	if (getVarint32(input, &len) && input->size() >= len) {
		*result = std::string_view(input->data(), len);
		input->remove_prefix(len);
		return true;
	}
	else {
		return false;
	}
}

uint32_t calHash(const char* data, size_t n, uint32_t seed) {
	// Similar to murmur hash
	const uint32_t m = 0xc6a4a793;
	const uint32_t r = 24;
	const char* limit = data + n;
	uint32_t h = seed ^ (n * m);

	// Pick up four bytes at a time
	while (data + 4<= limit) {
		uint32_t w = decodeFixed32(data);
		data += 4;
		h += w;
		h *= m;
		h ^= (h >> 16);
	}

	// Pick up remaining bytes
	switch (limit - data) {
	case 3:
		h += static_cast<unsigned char>(data[2])<< 16;
	case 2:
		h += static_cast<unsigned char>(data[1])<< 8;
	case 1:
		h += static_cast<unsigned char>(data[0]);
		h *= m;
		h ^= (h >> r);
		break;
	}
	return h;
}

