#include "xUtil.h"

int string2ll(const char * s,size_t slen, long long * value)
{
	const char *p = s;
    size_t plen = 0;
    int negative = 0;
    unsigned long long v;

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


int ll2string(char *s, size_t len, long long value)
{
    char buf[32], *p;
    unsigned long long v;
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
int stringmatchlen(const char *pattern, int patternLen,
        const char *string, int stringLen, int nocase)
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

int stringmatch(const char *pattern, const char *string, int nocase)
{
    return stringmatchlen(pattern,strlen(pattern),string,strlen(string),nocase);
}

void getRandomHexChars(char *p, unsigned int len)
{
	char *charset = "0123456789abcdef";
	unsigned int j;

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
			unsigned int copylen = len > 20 ? 20 : len;

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
		unsigned int l = len;
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


/* Toggle the 16 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev16(void *p)
{
	unsigned char *x = (unsigned char *)p, t;

	t = x[0];
	x[0] = x[1];
	x[1] = t;
}

/* Toggle the 32 bit unsigned integer pointed by *p from little endian to
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

/* Toggle the 64 bit unsigned integer pointed by *p from little endian to
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







