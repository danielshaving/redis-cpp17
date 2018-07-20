//
// Created by zhanghao on 2018/6/17.
//
#pragma once
#include "all.h"
#include "sha1.h"

void memrev16(void *p);
void memrev32(void *p);
void memrev64(void *p);
uint16_t intrev16(uint16_t v);
uint32_t intrev32(uint32_t v);
uint64_t intrev64(uint64_t v);

/* variants of the function doing the actual convertion only if the target
 * host is big endian */
#if (BYTE_ORDER == LITTLE_ENDIAN)
#define memrev16ifbe(p)
#define memrev32ifbe(p)
#define memrev64ifbe(p)
#define intrev16ifbe(v) (v)
#define intrev32ifbe(v) (v)
#define intrev64ifbe(v) (v)
#else
#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)
#define intrev16ifbe(v) intrev16(v)
#define intrev32ifbe(v) intrev32(v)
#define intrev64ifbe(v) intrev64(v)
#endif

/* The functions htonu64() and ntohu64() convert the specified value to
 * network byte ordering and back. In big endian systems they are no-ops. */
#if (BYTE_ORDER == BIG_ENDIAN)
#define htonu64(v) (v)
#define ntohu64(v) (v)
#else
#define htonu64(v) intrev64(v)
#define ntohu64(v) intrev64(v)
#endif

#ifdef REDIS_TEST
int endianconvTest(int argc, char *argv[]);
#endif


int64_t ustime(void);
int64_t mstime(void);
int64_t setime(void);

uint32_t dictGenHashFunction(const void *key,int32_t len) ;
uint32_t dictGenCaseHashFunction(const char *buf,int32_t len);

int32_t ll2string(char *s,size_t len, int64_t value);
int32_t string2ll(const char *s,size_t slen,int64_t *value);
int32_t stringmatchlen(const char *p,int32_t plen,
		const char *s,int32_t slen,int32_t nocase);
int32_t stringmatch(const char *p,const char *s,int32_t nocase);
void getRandomHexChars(char *p,uint32_t len);
void memrev64(void *p);
void bytesToHuman(char *s,uint64_t n);

std::string base64Encode(unsigned char const*,unsigned int len);
std::string base64Decode(std::string const& s);

static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); \
	else { printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

