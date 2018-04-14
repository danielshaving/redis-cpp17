#pragma once
#include "xAll.h"
#include "xSha1.h"


#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)

int64_t ustime(void);
int64_t mstime(void);
int64_t setime(void);


uint32_t dictGenHashFunction(const void *key, int32_t len) ;
uint32_t dictGenCaseHashFunction(const  char *buf, int32_t len);

int32_t ll2string(char *s, size_t len, int64_t value);
int32_t string2ll(const char *s,size_t slen,int64_t  *value);
int32_t stringmatchlen(const char *p, int32_t plen, const char *s, int32_t slen, int32_t nocase);
int32_t stringmatch(const char *p, const char *s, int32_t nocase);
void getRandomHexChars(char *p, uint32_t len);
void memrev64(void *p);
void bytesToHuman(char *s, uint64_t n);

std::string base64Encode(unsigned char const*, unsigned int len);
std::string base64Decode(std::string const& s);


static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}
