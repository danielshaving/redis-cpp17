#pragma once
#include "all.h"
#include "sha1.h"

class Random
{
private:
	uint32_t seed;
public:
	Random()
	:seed(1)
	{

	}
	Random(uint32_t s)
	:seed(s & 0x7fffffffu)
	{
		if (seed == 0 || seed == 2147483647L)
		{
			seed = 1;
		}
	}

	void setSeed(uint32_t s)
	{
		seed = (s & 0x7fffffffu);
		// Avoid bad seeds.
		if (seed == 0 || seed == 2147483647L)
		{
			seed = 1;
		}
	}

	uint32_t next()
	{
		static const uint32_t M = 2147483647L;   // 2^31-1
		static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
		// We are computing
		//       seed = (seed * A) % M,    where M = 2^31-1
		//
		// seed must not be zero or M, or else all subsequent computed values
		// will be zero or M respectively.  For all other values, seed will end
		// up cycling through every number in [1,M-1]
		uint64_t product = seed * A;

		// Compute (product % M) using the fact that ((x << 31) % M) == x.
		seed = static_cast<uint32_t>((product >> 31) + (product & M));
		// The first reduction may overflow by 1 bit, so we may need to
		// repeat.  mod == M is not possible; using > allows the faster
		// sign-bit-based test.
		if (seed > M)
		{
			seed -= M;
		}

		return seed;
	}
	// Returns a uniformly distributed value in the range [0..n-1]
	// REQUIRES: n > 0
	uint32_t uniform(int n) { return next() % n; }

	// Randomly returns true ~"1/n" of the time, and false otherwise.
	// REQUIRES: n > 0
	bool oneIn(int n) { return (next() % n) == 0; }

	// Skewed: pick "base" uniformly from range [0,max_log] and then
	// return "base" random bits.  The effect is to pick a number in the
	// range [0,2^max_log-1] with exponential bias towards smaller numbers.
	uint32_t skewed(int maxLog) { return uniform(1 << uniform(maxLog + 1)); }
};

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
int32_t stringmatchlen(const char *p,int32_t plen,const char *s,int32_t slen,int32_t nocase);
int32_t stringmatch(const char *p,const char *s,int32_t nocase);
void getRandomHexChars(char *p,uint32_t len);
void memrev64(void *p);
void bytesToHuman(char *s,uint64_t n);

std::string base64Encode(unsigned char const*,unsigned int len);
std::string base64Decode(std::string const& s);

static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

