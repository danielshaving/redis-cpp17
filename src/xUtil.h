#pragma once
#include "all.h"
#include "xSha1.h"


#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)

long long ustime(void);
long long mstime(void);
long long setime(void);


uint32_t dictGenHashFunction(const void *key, int32_t len) ;
uint32_t dictGenCaseHashFunction(const  char *buf, int32_t len);

int32_t ll2string(char *s, size_t len, int64_t value);
int32_t string2ll(const char *s,size_t slen,int64_t  *value);
int32_t stringmatchlen(const char *p, int32_t plen, const char *s, int32_t slen, int32_t nocase);
int32_t stringmatch(const char *p, const char *s, int32_t nocase);
void getRandomHexChars(char *p, uint32_t len);
void memrev64(void *p);
void bytesToHuman(char *s, uint64_t n);

std::string base64_encode(unsigned char const*, unsigned int len);
std::string base64_decode(std::string const& s);



