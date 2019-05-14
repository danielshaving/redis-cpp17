#pragma once

#include <string>
#include <iostream>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <cstring>
#include <cmath>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

bool IsTailWildcard(const std::string& pattern);

int32_t StringMatchLen(const char *pattern, int32_t patternlen,
	const char *string, int32_t stringlen, int32_t nocase);
	
int32_t StringMatch(const char *pattern,
	const char *string, int32_t nocase);

bool StartsWith(const std::string_view& x,
	const std::string_view& y);

int StrToLongDouble(const char* s,
	size_t slen, long double* ldval);

int LongDoubleToStr(long double ldval, 
	std::string* value);