#pragma once
#include "all.h"
#include "xSha1.h"


#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)

long long ustime(void);
long long mstime(void);
long long setime(void) ;


unsigned int dictGenHashFunction(const void *key, int len) ;
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len);

int ll2string(char *s, size_t len, long long value);
int string2ll(const char * s,size_t slen, long long * value);
int stringmatchlen(const char *p, int plen, const char *s, int slen, int nocase);
int stringmatch(const char *p, const char *s, int nocase);
void getRandomHexChars(char *p, unsigned int len);
void memrev64(void *p);
void bytesToHuman(char *s, unsigned long long n);



