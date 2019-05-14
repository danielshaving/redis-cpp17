#include "util.h"

#include <ctype.h>
#include <stdint.h>
#include <limits.h>

bool IsTailWildcard(const std::string& pattern) {
	if (pattern.size() < 2) {
		return false;
	}
	else {
		if (pattern.back() != '*') {
			return false;
		}
		else {
			for (uint32_t idx = 0; idx < pattern.size() - 1; ++idx) {
				if (pattern[idx] == '*' || pattern[idx] == '?'
					|| pattern[idx] == '[' || pattern[idx] == ']') {
					return false;
				}
			}
		}
	}
	return true;
}

int32_t StringMatchLen(const char* pattern, int32_t patternlen,
	const char* string, int32_t stringlen, int32_t nocase) {
	while (patternlen) {
		switch (pattern[0]) {
		case '*':
			while (pattern[1] == '*') {
				pattern++;
				patternlen--;
			}
			if (patternlen == 1)
				return 1; /* match */
			while (stringlen) {
				if (StringMatchLen(pattern + 1, patternlen - 1,
					string, stringlen, nocase))
					return 1; /* match */
				string++;
				stringlen--;
			}
			return 0; /* no match */
			break;
		case '?':
			if (stringlen == 0)
				return 0; /* no match */
			string++;
			stringlen--;
			break;
		case '[': {
			int no, match;
			pattern++;
			patternlen--;
			no = pattern[0] == '^';
			if (no) {
				pattern++;
				patternlen--;
			}
			match = 0;
			while (1) {
				if (pattern[0] == '\\' && patternlen >= 2) {
					pattern++;
					patternlen--;
					if (pattern[0] == string[0])
						match = 1;
				}
				else if (pattern[0] == ']') {
					break;
				}
				else if (patternlen == 0) {
					pattern--;
					patternlen++;
					break;
				}
				else if (pattern[1] == '-' && patternlen >= 3) {
					int start = pattern[0];
					int end = pattern[2];
					int c = string[0];
					if (start > end) {
						int t = start;
						start = end;
						end = t;
					}
					if (nocase) {
						start = tolower(start);
						end = tolower(end);
						c = tolower(c);
					}
					pattern += 2;
					patternlen -= 2;
					if (c >= start && c <= end)
						match = 1;
				}
				else {
					if (!nocase) {
						if (pattern[0] == string[0])
							match = 1;
					}
					else {
						if (tolower((int)pattern[0]) == tolower((int)string[0]))
							match = 1;
					}
				}
				pattern++;
				patternlen--;
			}
			if (no)
				match = !match;
			if (!match)
				return 0; /* no match */
			string++;
			stringlen--;
			break;
		}
		case '\\':
			if (patternlen >= 2) {
				pattern++;
				patternlen--;
			}
			/* fall through */
		default:
			if (!nocase) {
				if (pattern[0] != string[0])
					return 0; /* no match */
			}
			else {
				if (tolower((int)pattern[0]) != tolower((int)string[0]))
					return 0; /* no match */
			}
			string++;
			stringlen--;
			break;
		}
		pattern++;
		patternlen--;
		if (stringlen == 0) {
			while (*pattern == '*') {
				pattern++;
				patternlen--;
			}
			break;
		}
	}

	if (patternlen == 0 && stringlen == 0)
		return 1;
	return 0;
}

int32_t StringMatch(const char* pattern, const char* string, int32_t nocase) {
	return StringMatchLen(pattern, strlen(pattern), string, strlen(string), nocase);
}

bool StartsWith(const std::string_view & x, const std::string_view & y) {
	return ((x.size() >= y.size()) && (memcmp(x.data(), y.data(), y.size()) == 0));
}

int StrToLongDouble(const char* s, size_t slen, long double* ldval) {
	char* pEnd;
	std::string t(s, slen);
	if (t.find(" ") != std::string::npos) {
		return -1;
	}
	long double d = strtold(s, &pEnd);
	if (pEnd != s + slen)
		return -1;

	if (ldval != NULL) * ldval = d;
	return 0;
}

int LongDoubleToStr(long double ldval, std::string* value) {
	char buf[256];
	int len;
	if (std::isnan(ldval)) {
		return -1;
	}
	else if (std::isinf(ldval)) {
		/* Libc in odd systems (Hi Solaris!) will format infinite in a
		* different way, so better to handle it in an explicit way. */
		if (ldval > 0) {
			memcpy(buf, "inf", 3);
			len = 3;
		}
		else {
			memcpy(buf, "-inf", 4);
			len = 4;
		}
		return -1;
	}
	else {
		/* We use 17 digits precision since with 128 bit floats that precision
		 * after rounding is able to represent most small decimal numbers in a
		 * way that is "non surprising" for the user (that is, most small
		 * decimal numbers will be represented in a way that when converted
		 * back into a string are exactly the same as what the user typed.) */
		len = snprintf(buf, sizeof(buf), "%.17Lf", ldval);
		/* Now remove trailing zeroes after the '.' */
		if (strchr(buf, '.') != NULL) {
			char* p = buf + len - 1;
			while (*p == '0') {
				p--;
				len--;
			}
			if (*p == '.') len--;
		}
		value->assign(buf, len);
		return 0;
	}
}

