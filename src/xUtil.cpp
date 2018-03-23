#include "xUtil.h"



const uint32_t dict_hash_function_seed = 5381;

/* And a case insensitive hash function (based on djb hash) */
uint32_t dictGenCaseHashFunction(const char *buf, int32_t len)
{
	uint32_t hash = (uint32_t)dict_hash_function_seed;

	while (len--)
	    hash = ((hash << 5) + hash) + (tolower(*buf++)); /* hash * 33 + c */
	return hash;
}

uint32_t dictGenHashFunction(const void *key, int32_t len)
{
	/* 'm' and 'r' are mixing constants generated offline.
	 They're no really 'magic', they just happen to work well.  */
	uint32_t seed = dict_hash_function_seed;
	const uint32_t m = 0x5bd1e995;
	const int32_t r = 24;

	/* Initialize the hash to a 'random' value */
	uint32_t h = seed ^ len;

	/* Mix 4 bytes at a time into the hash */
	const char *data = (const char *)key;

	while(len >= 4)
	{
		uint32_t k = *(uint32_t*)data;

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;

		data += 4;
		len -= 4;
	}

	/* Handle the last few bytes of the input array  */
	switch(len) 
	{
		case 3: h ^= data[2] << 16;
		case 2: h ^= data[1] << 8;
		case 1: h ^= data[0]; h *= m;
	};

	/* Do a few final mixes of the hash to ensure the last few
	 * bytes are well-incorporated. */
	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}


int32_t string2ll(const char * s,size_t slen, int64_t *value)
{
	const char *p = s;
    size_t plen = 0;
    int32_t negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    if (slen == 1 && p[0] == '0')
    {
        if (value != nullptr) *value = 0;
        return 1;
    }

    if (p[0] == '-')
    {
        negative = 1;
        p++; plen++;

        if (plen == slen)
            return 0;
    }

    if (p[0] >= '1' && p[0] <= '9')
    {
        v = p[0]-'0';
        p++; plen++;
    }
    else if (p[0] == '0' && slen == 1)
    {
        *value = 0;
        return 1;
    }
    else
    {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9')
    {
        if (v > (ULLONG_MAX / 10))
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0')))
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    if (plen < slen)
        return 0;

    if (negative)
    {
        if (v > ((unsigned long long)(-(LLONG_MIN+1))+1))
            return 0;
        if (value != nullptr) *value = -v;
    }
    else
    {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;
        if (value != nullptr) *value = v;
    }
    return 1;

}

int32_t ll2string(char *s, size_t len, int64_t value)
{
    char buf[32], *p;
    uint64_t v;
    size_t l;

    if (len == 0) return 0;
    v = (value < 0) ? -value : value;
    p = buf+31; /* point to the last character */
    do
    {
        *p-- = '0'+(v%10);
        v /= 10;
    } while(v);
    if (value < 0) *p-- = '-';
    p++;
    l = 32-(p-buf);
    if (l+1 > len) l = len-1; /* Make sure it fits, including the nul term */
    memcpy(s,p,l);
    s[l] = '\0';
    return l;
}

/* Glob-style pattern matching. */
int32_t stringmatchlen(const char *pattern, int32_t patternLen,
        const char *string, int32_t stringLen, int32_t nocase)
{
    while(patternLen)
    {
        switch(pattern[0])
        {
        case '*':
            while (pattern[1] == '*')
            {
                pattern++;
                patternLen--;
            }
            if (patternLen == 1)
                return 1; /* match */
            while(stringLen)
            {
                if (stringmatchlen(pattern+1, patternLen-1,
                            string, stringLen, nocase))
                    return 1; /* match */
                string++;
                stringLen--;
            }
            return 0; /* no match */
            break;
        case '?':
            if (stringLen == 0)
                return 0; /* no match */
            string++;
            stringLen--;
            break;
        case '[':
        {
            int no, match;
            pattern++;
            patternLen--;
            no = pattern[0] == '^';
            if (no)
            {
                pattern++;
                patternLen--;
            }
            match = 0;
            while(1)
            {
                if (pattern[0] == '\\' && patternLen >= 2)
                {
                    pattern++;
                    patternLen--;
                    if (pattern[0] == string[0])
                        match = 1;
                }
                else if (pattern[0] == ']')
                {
                    break;
                }
                else if (patternLen == 0)
                {
                    pattern--;
                    patternLen++;
                    break;
                }
                else if (pattern[1] == '-' && patternLen >= 3)
                {
                    int start = pattern[0];
                    int end = pattern[2];
                    int c = string[0];
                    if (start > end)
                    {
                        int t = start;
                        start = end;
                        end = t;
                    }
                    if (nocase)
                    {
                        start = tolower(start);
                        end = tolower(end);
                        c = tolower(c);
                    }
                    pattern += 2;
                    patternLen -= 2;
                    if (c >= start && c <= end)
                        match = 1;
                }
                else
                {
                    if (!nocase)
                    {
                        if (pattern[0] == string[0])
                            match = 1;
                    }
                    else
                    {
                        if (tolower((int)pattern[0]) == tolower((int)string[0]))
                            match = 1;
                    }
                }
                pattern++;
                patternLen--;
            }
            if (no)
                match = !match;
            if (!match)
                return 0; /* no match */
            string++;
            stringLen--;
            break;
        }
        case '\\':
            if (patternLen >= 2)
            {
                pattern++;
                patternLen--;
            }
            /* fall through */
        default:
            if (!nocase)
            {
                if (pattern[0] != string[0])
                    return 0; /* no match */
            } else
            {
                if (tolower((int)pattern[0]) != tolower((int)string[0]))
                    return 0; /* no match */
            }
            string++;
            stringLen--;
            break;
        }
        pattern++;
        patternLen--;
        if (stringLen == 0)
        {
            while(*pattern == '*')
            {
                pattern++;
                patternLen--;
            }
            break;
        }
    }

    if (patternLen == 0 && stringLen == 0)
        return 1;
    return 0;
}

int32_t stringmatch(const char *pattern, const char *string, int32_t nocase)
{
    return stringmatchlen(pattern,strlen(pattern),string,strlen(string),nocase);
}

void getRandomHexChars(char *p, uint32_t len)
{
	char *charset = "0123456789abcdef";
	uint32_t j;

	/* Global state. */
	static int seedInitialized = 0;
	static unsigned char seed[20]; /* The SHA1 seed, from /dev/urandom. */
	static uint64_t counter = 0; /* The counter we hash with the seed. */

	if (!seedInitialized)
	{
		/* Initialize a seed and use SHA1 in counter mode, where we hash
		* the same seed with a progressive counter. For the goals of this
		* function we just need non-colliding strings, there are no
		* cryptographic security needs. */
		FILE *fp = fopen("/dev/urandom", "r");
		if (fp && fread(seed, sizeof(seed), 1, fp) == 1)
			seedInitialized = 1;
		if (fp) fclose(fp);
	}

	if (seedInitialized)
	{
		while (len)
		{
			unsigned char digest[20];
			SHA1_CTX ctx;
			uint32_t copylen = len > 20 ? 20 : len;

			SHA1Init(&ctx);
			SHA1Update(&ctx, seed, sizeof(seed));
			SHA1Update(&ctx, (unsigned char*)&counter, sizeof(counter));
			SHA1Final(digest, &ctx);
			counter++;

			memcpy(p, digest, copylen);
			/* Convert to hex digits. */
			for (j = 0; j < copylen; j++) p[j] = charset[p[j] & 0x0F];
			len -= copylen;
			p += copylen;
		}
	}
	else
	{
		/* If we can't read from /dev/urandom, do some reasonable effort
		* in order to create some entropy, since this function is used to
		* generate run_id and cluster instance IDs */
		char *x = p;
		uint32_t l = len;
		struct timeval tv;
		pid_t pid = getpid();

		/* Use time and PID to fill the initial array. */
		gettimeofday(&tv, NULL);
		if (l >= sizeof(tv.tv_usec))
		{
			memcpy(x, &tv.tv_usec, sizeof(tv.tv_usec));
			l -= sizeof(tv.tv_usec);
			x += sizeof(tv.tv_usec);
		}
		if (l >= sizeof(tv.tv_sec))
		{
			memcpy(x, &tv.tv_sec, sizeof(tv.tv_sec));
			l -= sizeof(tv.tv_sec);
			x += sizeof(tv.tv_sec);
		}
		if (l >= sizeof(pid))
		{
			memcpy(x, &pid, sizeof(pid));
			l -= sizeof(pid);
			x += sizeof(pid);
		}
		/* Finally xor it with rand() output, that was already seeded with
		* time() at startup, and convert to hex digits. */
		for (j = 0; j < len; j++)
		{
			p[j] ^= rand();
			p[j] = charset[p[j] & 0x0F];
		}
	}
}


long long ustime(void)
{
	struct timeval tv;
	long long ust;

	gettimeofday(&tv, nullptr);
	ust = ((long long)tv.tv_sec)*1000000;
	ust += tv.tv_usec;
	return ust;
}

/* Return the UNIX time in milliseconds */
long long mstime(void)
{
	return ustime()/1000;
}


/* Return the UNIX time in seconds */
long long setime(void)
{
	return ustime()/1000/1000;
}




/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n)
{
	double d;

	if (n < 1024) {
	    /* Bytes */
	    sprintf(s,"%lluB",n);
	    return;
	}
	else if (n < (1024*1024))
	{
	    d = (double)n/(1024);
	    sprintf(s,"%.2fK",d);
	}
	else if (n < (1024LL*1024*1024))
	{
	    d = (double)n/(1024*1024);
	    sprintf(s,"%.2fM",d);
	}
	else if (n < (1024LL*1024*1024*1024))
	{
	    d = (double)n/(1024LL*1024*1024);
	    sprintf(s,"%.2fG",d);
	}
	else if (n < (1024LL*1024*1024*1024*1024))
	{
	    d = (double)n/(1024LL*1024*1024*1024);
	    sprintf(s,"%.2fT",d);
	}
	else if (n < (1024LL*1024*1024*1024*1024*1024))
	{
	    d = (double)n/(1024LL*1024*1024*1024*1024);
	    sprintf(s,"%.2fP",d);
	}
	else
	{
	    /* Let's hope we never need this */
	    sprintf(s,"%lluB",n);
	}
}


/* Toggle the 16 bit uint32_teger pointed by *p from little endian to
 * big endian */
void memrev16(void *p)
{
	unsigned char *x = (unsigned char *)p, t;

	t = x[0];
	x[0] = x[1];
	x[1] = t;
}

/* Toggle the 32 bit uint32_teger pointed by *p from little endian to
 * big endian */
void memrev32(void *p)
{
	unsigned char *x = (unsigned char *)p, t;

	t = x[0];
	x[0] = x[3];
	x[3] = t;
	t = x[1];
	x[1] = x[2];
	x[2] = t;
}

/* Toggle the 64 bit uint32_teger pointed by *p from little endian to
 * big endian */
void memrev64(void *p)
{
	unsigned char *x = (unsigned char *)p, t;

	t = x[0];
	x[0] = x[7];
	x[7] = t;
	t = x[1];
	x[1] = x[6];
	x[6] = t;
	t = x[2];
	x[2] = x[5];
	x[5] = t;
	t = x[3];
	x[3] = x[4];
	x[4] = t;
}


static const std::string base64_chars =
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "abcdefghijklmnopqrstuvwxyz"
             "0123456789+/";


static inline bool is_base64(unsigned char c)
{
	return (isalnum(c) || (c == '+') || (c == '/'));
}

std::string base64Encode(unsigned char const* bytes_to_encode, unsigned int in_len)
{
	std::string ret;
	int i = 0;
	int j = 0;
	unsigned char char_array_3[3];
	unsigned char char_array_4[4];

	while (in_len--)
	{
		char_array_3[i++] = *(bytes_to_encode++);
		if (i == 3) {
		char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
		char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
		char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
		char_array_4[3] = char_array_3[2] & 0x3f;
	
		for(i = 0; (i <4) ; i++)
		ret += base64_chars[char_array_4[i]];
		i = 0;
		}
	}

	if (i)
	{
		for(j = i; j < 3; j++)
		char_array_3[j] = '\0';

		char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
		char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
		char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
		char_array_4[3] = char_array_3[2] & 0x3f;

		for (j = 0; (j < i + 1); j++)
			ret += base64_chars[char_array_4[j]];

		while((i++ < 3))
		ret += '=';

	}

	return ret;

}

std::string base64Decode(std::string const& encoded_string)
{
  size_t in_len = encoded_string.size();
  int i = 0;
  int j = 0;
  int in_ = 0;
  unsigned char char_array_4[4], char_array_3[3];
  std::string ret;

  while (in_len-- && ( encoded_string[in_] != '=') && is_base64(encoded_string[in_]))
  {
    char_array_4[i++] = encoded_string[in_]; in_++;
    if (i ==4) {
      for (i = 0; i <4; i++)
        char_array_4[i] = base64_chars.find(char_array_4[i]);

      char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (i = 0; (i < 3); i++)
        ret += char_array_3[i];
      i = 0;
    }
  }

  if (i)
  {
    for (j = i; j <4; j++)
      char_array_4[j] = 0;

    for (j = 0; j <4; j++)
      char_array_4[j] = base64_chars.find(char_array_4[j]);

    char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
    char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
    char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

    for (j = 0; (j < i - 1); j++) ret += char_array_3[j];
  }

  return ret;
}









