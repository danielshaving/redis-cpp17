#include "sds.h"

sds sdsnewlen(const void *init,size_t initlen)
{
    struct sdshdr *sh;
    // T = O(N)
    if (init)
    {
		sh = (sdshdr*)zmalloc(sizeof(struct sdshdr)+initlen+1);
    }
    else
    {
        sh = (sdshdr*)zcalloc(sizeof(struct sdshdr)+initlen+1);
    }

    if (sh == nullptr) return nullptr;
    sh->len = initlen;
    sh->free = 0;
    // T = O(N)
    if (initlen && init)
    memcpy(sh->buf,init,initlen);
    sh->buf[initlen] = '\0';
    return (char*)sh->buf;
}

/* Create an empty (zero length) sds string. Even in this case the string
 * always has an implicit null term. */
sds sdsempty(void)
{
    return sdsnewlen("",0);
}

/* Create a new sds string starting from a null termined C string. */
sds sdsnew(const char *init)
{
    size_t initlen = (init == nullptr) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

sds sdsdup(const sds s)
{
    return sdsnewlen(s, sdslen(s));
}

void sdsfree(sds s)
{
    if (s == nullptr) return;
    zfree(s-sizeof(struct sdshdr));
}

void sdsupdatelen(sds s)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    int reallen = strlen(s);
    sh->free += (sh->len-reallen);
    sh->len = reallen;
}

void sdsclear(sds s)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    sh->free += sh->len;
    sh->len = 0;
    sh->buf[0] = '\0';
}

sds sdsMakeRoomFor(sds s, size_t addlen)
{
    struct sdshdr *sh, *newsh;
    size_t free = sdsavail(s);
    size_t len, newlen;
    if (free >= addlen) return s;
    len = sdslen(s);
    sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    newlen = (len+addlen);
    if (newlen < SDS_MAX_PREALLOC)
        newlen *= 2;
    else
        newlen += SDS_MAX_PREALLOC;
    // T = O(N)
    newsh = (sdshdr*)zrealloc(sh, sizeof(struct sdshdr)+newlen+1);
    if (newsh == nullptr) return nullptr;
    newsh->free = newlen - len;
    return newsh->buf;
}

/* Reallocate the sds string so that it has no free space at the end. The
 * contained string remains not altered, but next concatenation operations
 * will require a reallocation.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdsRemoveFreeSpace(sds s)
{
    struct sdshdr *sh;
    sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    // T = O(N)
    sh = (sdshdr*)zrealloc(sh, sizeof(struct sdshdr)+sh->len+1);
    sh->free = 0;
    return sh->buf;
}
/* Return the total size of the allocation of the specifed sds string,
 * including:
 * 1) The sds header before the pointer.
 * 2) The string.
 * 3) The free buffer at the end if any.
 * 4) The implicit null term.
 */
size_t sdsAllocSize(sds s)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    return sizeof(*sh)+sh->len+sh->free+1;
}

void sdsIncrLen(sds s, int incr)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    assert(sh->free >= incr);
    sh->len += incr;
    sh->free -= incr;
    assert(sh->free >= 0);
    s[sh->len] = '\0';
}

/* Grow the sds to have the specified length. Bytes that were not part of
 * the original length of the sds will be set to zero.
 *
 * if the specified length is smaller than the current length, no operation
*/
sds sdsgrowzero(sds s, size_t len)
{
    struct sdshdr *sh = (sdshdr*)(s-(sizeof(struct sdshdr)));
    size_t totlen, curlen = sh->len;
    if (len <= curlen) return s;
    // T = O(N)
    s = sdsMakeRoomFor(s,len-curlen);
    if (s == nullptr) return nullptr;

    sh = (sdshdr*)(s-(sizeof(struct sdshdr)));
    memset(s+curlen,0,(len-curlen+1)); /* also set trailing \0 byte */
    totlen = sh->len+sh->free;
    sh->len = len;
    sh->free = totlen-sh->len;
    return s;
}

/* Append the specified binary-safe string pointed by 't' of 'len' bytes to the
 * end of the specified sds string 's'.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatlen(sds s, const void *t, size_t len)
{
    struct sdshdr *sh;
    size_t curlen = sdslen(s);
    // T = O(N)
    s = sdsMakeRoomFor(s,len);
    if (s == nullptr) return nullptr;
    // T = O(N)
    sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    memcpy(s+curlen, t, len);
    sh->len = curlen+len;
    sh->free = sh->free-len;
    s[curlen+len] = '\0';
    return s;
}

/* Append the specified null termianted C string to the sds string 's'.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscat(sds s, const char *t)
{
    return sdscatlen(s, t, strlen(t));
}

/* Append the specified sds 't' to the existing sds 's'.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatsds(sds s, const sds t)
{
    return sdscatlen(s, t, sdslen(t));
}

/* Destructively modify the sds string 's' to hold the specified binary
 * safe string pointed by 't' of length 'len' bytes. */
sds sdscpylen(sds s, const char *t, size_t len)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    size_t totlen = sh->free+sh->len;
   
    if (totlen < len)
    {
        // T = O(N)
        s = sdsMakeRoomFor(s,len-sh->len);
        if (s == nullptr) return nullptr;
        sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
        totlen = sh->free+sh->len;
    }

    memcpy(s,t,len);
    s[len] = '\0';
    sh->len = len;
    sh->free = totlen-len;
    return s;
}

/* Like sdscpylen() but 't' must be a null-termined string so that the length
 * of the string is obtained with strlen(). */
sds sdscpy(sds s, const char *t)
{
    return sdscpylen(s, t, strlen(t));
}

/* Helper for sdscatlonglong() doing the actual number -> string
 * conversion. 's' must point to a string with room for at least
 * SDS_LLSTR_SIZE bytes.
 *
 * The function returns the lenght of the null-terminated string
 * representation stored at 's'. */
#define SDS_LLSTR_SIZE 21
int sdsll2str(char *s, long long value)
{
    char *p, aux;
    unsigned long long v;
    size_t l;

    /* Generate the string representation, this method produces
     * an reversed string. */
    v = (value < 0) ? -value : value;
    p = s;
    do
    {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);
    if (value < 0) *p++ = '-';

    /* Compute length and add null term. */
    l = p-s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while(s < p)
    {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Identical sdsll2str(), but for unsigned long long type. */
int sdsull2str(char *s, unsigned long long v)
{
    char *p, aux;
    size_t l;

    /* Generate the string representation, this method produces
     * an reversed string. */
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);

    /* Compute length and add null term. */
    l = p-s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while(s < p)
    {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Create an sds string from a long long value. It is much faster than:
 *
 * sdscatprintf(sdsempty(),"%lld\n", value);
 */
sds sdsfromlonglong(long long value)
{
    char buf[SDS_LLSTR_SIZE];
    int len = sdsll2str(buf,value);
    return sdsnewlen(buf,len);
}

/* Like sdscatpritf() but gets va_list instead of being variadic. */
sds sdscatvprintf(sds s, const char *fmt, va_list ap)
{
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf, *t;
    size_t buflen = strlen(fmt)*2;

    /* We try to start using a static buffer for speed.
     * If not possible we revert to heap allocation. */
    if (buflen > sizeof(staticbuf))
    {
        buf = (char*)zmalloc(buflen);
        if (buf == nullptr) return nullptr;
    }
    else
    {
        buflen = sizeof(staticbuf);
    }

    /* Try with buffers two times bigger every time we fail to
     * fit the string in the current buffer size. */
    while(1)
    {
        buf[buflen-2] = '\0';
        va_copy(cpy,ap);
        // T = O(N)
        vsnprintf(buf, buflen, fmt, cpy);
        if (buf[buflen-2] != '\0')
        {
            if (buf != staticbuf) zfree(buf);
            buflen *= 2;
            buf = (char*)zmalloc(buflen);
            if (buf == nullptr) return nullptr;
            continue;
        }
        break;
    }

    /* Finally concat the obtained string to the SDS string and return it. */
    t = sdscat(s, buf);
    if (buf != staticbuf) zfree(buf);
    return t;
}

/* Append to the sds string 's' a string obtained using printf-alike format
 * specifier.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsempty("Sum is: ");
 * s = sdscatprintf(s,"%d+%d = %d",a,b,a+b).
 *
 * Often you need to create a string from scratch with the printf-alike
 * format. When this is the need, just use sdsempty() as the target string:
 *
 * s = sdscatprintf(sdsempty(), "... your format ...", args);
 */
sds sdscatprintf(sds s, const char *fmt, ...)
{
    va_list ap;
    char *t;
    va_start(ap, fmt);
    // T = O(N^2)
    t = sdscatvprintf(s,fmt,ap);
    va_end(ap);
    return t;
}

/* This function is similar to sdscatprintf, but much faster as it does
 * not rely on sprintf() family functions implemented by the libc that
 * are often very slow. Moreover directly handling the sds string as
 * new data is concatenated provides a performance improvement.
 *
 * However this function only handles an incompatible subset of printf-alike
 * format specifiers:
 *
 * %s - C String
 * %S - SDS string
 * %i - signed int
 * %I - 64 bit signed integer (long long, int64_t)
 * %u - unsigned int
 * %U - 64 bit unsigned integer (unsigned long long, uint64_t)
 * %% - Verbatim "%" character.
 */
sds sdscatfmt(sds s, char const *fmt, ...)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    size_t initlen = sdslen(s);
    const char *f = fmt;
    int i;
    va_list ap;

    va_start(ap,fmt);
    f = fmt;    /* Next format specifier byte to process. */
    i = initlen; /* Position of the next byte to write to dest str. */
    while(*f)
    {
        char next, *str;
        size_t l;
        long long num;
        unsigned long long unum;

        /* Make sure there is always space for at least 1 char. */
        if (sh->free == 0)
        {
            s = sdsMakeRoomFor(s,1);
            sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
        }

        switch(*f)
        {
        case '%':
            next = *(f+1);
            f++;
            switch(next)
            {
            case 's':
            case 'S':
                str = va_arg(ap,char*);
                l = (next == 's') ? strlen(str) : sdslen(str);
                if (sh->free < l)
                {
                    s = sdsMakeRoomFor(s,l);
                    sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
                }
                memcpy(s+i,str,l);
                sh->len += l;
                sh->free -= l;
                i += l;
                break;
            case 'i':
            case 'I':
                if (next == 'i')
                    num = va_arg(ap,int);
                else
                    num = va_arg(ap,long long);
                {
                    char buf[SDS_LLSTR_SIZE];
                    l = sdsll2str(buf,num);
                    if (sh->free < l)
                    {
                        s = sdsMakeRoomFor(s,l);
                        sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
                    }
                    memcpy(s+i,buf,l);
                    sh->len += l;
                    sh->free -= l;
                    i += l;
                }
                break;
            case 'u':
            case 'U':
                if (next == 'u')
                    unum = va_arg(ap,unsigned int);
                else
                    unum = va_arg(ap,unsigned long long);
                {
                    char buf[SDS_LLSTR_SIZE];
                    l = sdsull2str(buf,unum);
                    if (sh->free < l)
                    {
                        s = sdsMakeRoomFor(s,l);
                        sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
                    }
                    memcpy(s+i,buf,l);
                    sh->len += l;
                    sh->free -= l;
                    i += l;
                }
                break;
            default: /* Handle %% and generally %<unknown>. */
                s[i++] = next;
                sh->len += 1;
                sh->free -= 1;
                break;
            }
            break;
        default:
            s[i++] = *f;
            sh->len += 1;
            sh->free -= 1;
            break;
        }
        f++;
    }
    va_end(ap);

    /* Add null-term */
    s[i] = '\0';
    return s;
}

/* Remove the part of the string from left and from right composed just of
 * contiguous characters found in 'cset', that is a null terminted C string.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("AA...AA.a.aa.aHelloWorld     :::");
 * s = sdstrim(s,"A. :");
 * printf("%s\n", s);
 *
 * Output will be just "Hello World".
 */
sds sdstrim(sds s, const char *cset)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    char *start, *end, *sp, *ep;
    size_t len;
    sp = start = s;
    ep = end = s+sdslen(s)-1;


    while(sp <= end && strchr(cset, *sp)) sp++;
    while(ep > start && strchr(cset, *ep)) ep--;

 
    len = (sp > ep) ? 0 : ((ep-sp)+1);
    
    // T = O(N)
    if (sh->buf != sp) memmove(sh->buf, sp, len);

   
    sh->buf[len] = '\0';

 
    sh->free = sh->free+(sh->len-len);
    sh->len = len;

   
    return s;
}

/* Turn the string into a smaller (or equal) string containing only the
 * substring specified by the 'start' and 'end' indexes.
 *
 * start and end can be negative, where -1 means the last character of the
 * string, -2 the penultimate character, and so forth.
 *
 * The interval is inclusive, so the start and end characters will be part
 * of the resulting string.
 *
 * The string is modified in-place.
 *
 * Example:
 *
 * s = sdsnew("Hello World");
 * sdsrange(s,1,-1); => "ello World"
 */
void sdsrange(sds s, int start, int end)
{
    struct sdshdr *sh = (sdshdr*) (s-(sizeof(struct sdshdr)));
    size_t newlen, len = sdslen(s);

    if (len == 0) return;
    if (start < 0)
    {
        start = len+start;
        if (start < 0) start = 0;
    }
    if (end < 0)
    {
        end = len+end;
        if (end < 0) end = 0;
    }
    newlen = (start > end) ? 0 : (end-start)+1;
    if (newlen != 0)
    {
        if (start >= (signed)len)
        {
            newlen = 0;
        }
        else if (end >= (signed)len)
        {
            end = len-1;
            newlen = (start > end) ? 0 : (end-start)+1;
        }
    }
    else
    {
        start = 0;
    }

   
    if (start && newlen) memmove(sh->buf, sh->buf+start, newlen);

 
    sh->buf[newlen] = 0;

    sh->free = sh->free+(sh->len-newlen);
    sh->len = newlen;
}

/* Apply tolower() to every character of the sds string 's'. */
void sdstolower(sds s)
{
    int len = sdslen(s), j;
    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}


/* Apply toupper() to every character of the sds string 's'. */
void sdstoupper(sds s)
{
    int len = sdslen(s), j;
    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}

/* Compare two sds strings s1 and s2 with memcmp().
 *
 * Return value:
 *
 *     1 if s1 > s2.
 *    -1 if s1 < s2.
 *     0 if s1 and s2 are exactly the same binary string.
 *
 * If two strings share exactly the same prefix, but one of the two has
 * additional characters, the longer string is considered to be greater than
 * the smaller one. */
int sdscmp(const sds s1, const sds s2)
{
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1,s2,minlen);
    if (cmp == 0) return l1-l2;
    return cmp;
}

sds *sdssplitlen(const char *s, int len, const char *sep, int seplen, int *count)
{
    int elements = 0, slots = 5, start = 0, j;
    sds *tokens;
    if (seplen < 1 || len < 0) return nullptr;
    tokens = (sds*)zmalloc(sizeof(sds)*slots);
    if (tokens == nullptr) return nullptr;
    if (len == 0)
    {
        *count = 0;
        return tokens;
    }

    // T = O(N^2)
    for (j = 0; j < (len-(seplen-1)); j++)
    {
        /* make sure there is room for the next element and the final one */
        if (slots < elements+2)
        {
            sds *newtokens;

            slots *= 2;
            newtokens = (sds*)zrealloc(tokens,sizeof(sds)*slots);
            if (newtokens == nullptr) goto cleanup;
            tokens = newtokens;
        }
        /* search the separator */
        // T = O(N)
        if ((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0))
        {
            tokens[elements] = sdsnewlen(s+start,j-start);
            if (tokens[elements] == nullptr) goto cleanup;
            elements++;
            start = j+seplen;
            j = j+seplen-1; /* skip the separator */
        }
    }
    /* Add the final element. We are sure there is room in the tokens array. */
    tokens[elements] = sdsnewlen(s+start,len-start);
    if (tokens[elements] == nullptr) goto cleanup;
    elements++;
    *count = elements;
    return tokens;

cleanup:
    {
        int i;
        for (i = 0; i < elements; i++) sdsfree(tokens[i]);
        zfree(tokens);
        *count = 0;
        return nullptr;
    }
}


/* Free the result returned by sdssplitlen(), or do nothing if 'tokens' is nullptr. */
void sdsfreesplitres(sds *tokens, int count)
{
    if (!tokens) return;
    while(count--)
        sdsfree(tokens[count]);
    zfree(tokens);
}


/* Append to the sds string "s" an escaped string representation where
 * all the non-printable characters (tested with isprint()) are turned into
 * escapes in the form "\n\r\a...." or "\x<hex-number>".
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatrepr(sds s, const char *p, size_t len)
{
    s = sdscatlen(s,"\"",1);
    while(len--)
    {
        switch(*p)
        {
			case '\\':
			case '"':
				s = sdscatprintf(s,"\\%c",*p);
				break;
			case '\n': s = sdscatlen(s,"\\n",2); break;
			case '\r': s = sdscatlen(s,"\\r",2); break;
			case '\t': s = sdscatlen(s,"\\t",2); break;
			case '\a': s = sdscatlen(s,"\\a",2); break;
			case '\b': s = sdscatlen(s,"\\b",2); break;
			default:
				if (isprint(*p))
					s = sdscatprintf(s,"%c",*p);
				else
					s = sdscatprintf(s,"\\x%02x",(unsigned char)*p);
				break;
        }
        p++;
    }
    return sdscatlen(s,"\"",1);
}

/* Helper function for sdssplitargs() that returns non zero if 'c'
 * is a valid hex digit. */

int is_hex_digit(char c)
{
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* Helper function for sdssplitargs() that converts a hex digit into an
 * integer from 0 to 15 */

int hex_digit_to_int(char c)
{
    switch(c)
    {
		case '0': return 0;
		case '1': return 1;
		case '2': return 2;
		case '3': return 3;
		case '4': return 4;
		case '5': return 5;
		case '6': return 6;
		case '7': return 7;
		case '8': return 8;
		case '9': return 9;
		case 'a': case 'A': return 10;
		case 'b': case 'B': return 11;
		case 'c': case 'C': return 12;
		case 'd': case 'D': return 13;
		case 'e': case 'E': return 14;
		case 'f': case 'F': return 15;
		default: return 0;
    }
}


sds *sdssplitargs(const char *line,int *argc)
{
    const char *p = line;
    char *current = nullptr;
    char **vector = nullptr;

    *argc = 0;
    while(1)
    {
        // T = O(N)
        while(*p && isspace(*p)) p++;

        if (*p) {
            /* get a token */
            int inq=0;  /* set to 1 if we are in "quotes" */
            int insq=0; /* set to 1 if we are in 'single quotes' */
            int done=0;

            if (current == nullptr) current = sdsempty();

            // T = O(N)
            while(!done) {
                if (inq) {
                    if (*p == '\\' && *(p+1) == 'x' &&
                                             is_hex_digit(*(p+2)) &&
                                             is_hex_digit(*(p+3)))
                    {
                        unsigned char byte;

                        byte = (hex_digit_to_int(*(p+2))*16)+
                                hex_digit_to_int(*(p+3));
                        current = sdscatlen(current,(char*)&byte,1);
                        p += 3;
                    } else if (*p == '\\' && *(p+1)) {
                        char c;

                        p++;
                        switch(*p) {
                        case 'n': c = '\n'; break;
                        case 'r': c = '\r'; break;
                        case 't': c = '\t'; break;
                        case 'b': c = '\b'; break;
                        case 'a': c = '\a'; break;
                        default: c = *p; break;
                        }
                        current = sdscatlen(current,&c,1);
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else if (insq) {
                    if (*p == '\\' && *(p+1) == '\'') {
                        p++;
                        current = sdscatlen(current,"'",1);
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else {
                    switch(*p) {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                        done=1;
                        break;
                    case '"':
                        inq=1;
                        break;
                    case '\'':
                        insq=1;
                        break;
                    default:
                        current = sdscatlen(current,p,1);
                        break;
                    }
                }
                if (*p) p++;
            }
            /* add the token to the vector */
            // T = O(N)
            vector = (char **)zrealloc(vector,((*argc)+1)*sizeof(char*));
            vector[*argc] = current;
            (*argc)++;
            current = nullptr;
        }
        else
        {
            /* Even on empty input string return something not nullptr. */
            if (vector == nullptr) vector = (char **)zmalloc(sizeof(void*));
            return vector;
        }
    }

err:
    while((*argc)--)
        sdsfree(vector[*argc]);
    zfree(vector);
    if (current) sdsfree(current);
    *argc = 0;
    return nullptr;
}

sds sdsmapchars(sds s,const char *from,const char *to,size_t setlen)
{
    size_t j, i, l = sdslen(s);
    for (j = 0; j < l; j++)
    {
        for (i = 0; i < setlen; i++)
        {
            if (s[j] == from[i])
            {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

/* Join an array of C strings using the specified separator (also a C string).
 * Returns the result as an sds string. */
sds sdsjoin(char **argv,int argc,char *sep)
{
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscat(join, argv[j]);
        if (j != argc-1) join = sdscat(join,sep);
    }
    return join;
}


