#pragma once

#include <string>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <random>
#include <string>
#include <cstring>
#include <cmath>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef HAVE_SNAPPY
#include <snappy.h>
#endif

bool IsTailWildcard(const std::string& pattern);

int32_t StringMatchLen(const char *pattern, int32_t patternlen,
	const char *string, int32_t stringlen, int32_t nocase);

std::string ToString(const std::string_view &view);

int32_t StringMatch(const char *pattern,
	const char *string, int32_t nocase);

bool StartsWith(const std::string_view& x,
	const std::string_view& y);

int StrToLongDouble(const char* s,
	size_t slen, long double* ldval);

int LongDoubleToStr(long double ldval, 
	std::string* value);

std::string_view RandomString(std::default_random_engine* rnd, 
	int len, std::string* dst);
	
std::string RandomKey(std::default_random_engine* rnd, int len);

std::string_view CompressibleString(std::default_random_engine* rnd,
	double compressedfraction, size_t len, std::string* dst);
	
int CalculateMetaStartAndEndKey(const std::string& key,
	std::string* metastartkey, std::string* metaendkey);

int CalculateDataStartAndEndKey(const std::string& key, 
	std::string* datastartkey, std::string* dataendkey);
  
inline bool Snappy_Compress(const char* input, size_t length,
                            std::string* output) {
#if HAVE_SNAPPY
	output->resize(snappy::MaxCompressedLength(length));
	size_t outlen;
	snappy::RawCompress(input, length, &(*output)[0], &outlen);
	output->resize(outlen);
	return true;
#else
	// Silence compiler warnings about unused arguments.
	(void)input;
	(void)length;
	(void)output;
#endif  // HAVE_SNAPPY
	return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#if HAVE_SNAPPY
	return snappy::GetUncompressedLength(input, length, result);
#else
	// Silence compiler warnings about unused arguments.
	(void)input;
	(void)length;
	(void)result;
	return false;
#endif  // HAVE_SNAPPY
}


inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#if HAVE_SNAPPY
	return snappy::RawUncompress(input, length, output);
#else
	// Silence compiler warnings about unused arguments.
	(void)input;
	(void)length;
	(void)output;
	return false;
#endif  // HAVE_SNAPPY

}