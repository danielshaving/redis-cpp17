#pragma once
#include <stdint.h>
#include <string.h>

#include <string>
#include <string_view>

static const bool kLittleEndian = true /* or some other expression */;

// Standard Put... routines append to a string
void putFixed32(std::string *dst, uint32_t value);
void putFixed64(std::string *dst, uint64_t value);
void putVarint32(std::string *dst, uint32_t value);
void putVarint64(std::string *dst, uint64_t value);
void putLengthPrefixedSlice(std::string *dst, const std::string_view &value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
bool getVarint32(std::string_view *input, uint32_t *value);
bool getVarint64(std::string_view *input, uint64_t *value);
bool getLengthPrefixedSlice(std::string_view *input, std::string_view *result);

std::string_view getLengthPrefixedSlice(const char *data);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
const char *getVarint32Ptr(const char *p, const char *limit, uint32_t *v);
const char *getVarint64Ptr(const char *p, const char *limit, uint64_t *v);

// Returns the length of the varint32 or varint64 encoding of "v"
int varintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
void encodeFixed32(char *dst, uint32_t value);
void encodeFixed64(char *dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
char *encodeVarint32(char *dst, uint32_t value);
char *encodeVarint64(char *dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint32_t decodeFixed32(const char *ptr)
{
	if (!kLittleEndian)
	{
		uint32_t result;
		memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
		return result;
	}
	else
	{
		return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
	}
}

inline uint64_t decodeFixed64(const char *ptr)
{
	if (!kLittleEndian)
	{
		uint64_t result;
		memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
		return result;
	}
	else
	{
		uint64_t lo = decodeFixed32(ptr);
		uint64_t hi = decodeFixed32(ptr + 4);
		return (hi << 32) | lo;
	}
}

// Internal routine for use by fallback path of GetVarint32Ptr
const char *getVarint32PtrFallback(const char *p, const char *limit, uint32_t *value);
inline const char *getVarint32Ptr(const char *p, const char *limit, uint32_t *value)
{
	if (p < limit)
	{
		uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
		if ((result & 128) == 0)
		{
			*value = result;
			return p + 1;
		}
	}
	return getVarint32PtrFallback(p, limit, value);
}

uint32_t calHash(const char *data, size_t n, uint32_t seed);
