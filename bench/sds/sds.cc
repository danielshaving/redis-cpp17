#pragma once

#include "sds.h"

int __failed_tests = 0;
int __test_num = 0;
#define test_cond(descr, _c) do { \
    __test_num++; printf("%d - %s: ", __test_num, descr); \
    if(_c) printf("PASSED\n"); else {printf("FAILED\n"); __failed_tests++;} \
} while(0);
#define test_report() do { \
    printf("%d tests, %d passed, %d failed\n", __test_num, \
                    __test_num-__failed_tests, __failed_tests); \
    if (__failed_tests) { \
        printf("=== WARNING === We have failed tests here...\n"); \
        exit(1); \
    } \
} while(0);

int main(void) {
    struct sdshdr *sh;
    sds x = sdsnew("foo"), y;

    test_cond("Create a string and obtain the length", sdslen(x) == 3 &&
                                                       memcmp(x, "foo\0", 4) == 0)

    sdsfree(x);
    x = sdsnewlen("foo", 2);
    test_cond("Create a string with specified length",
              sdslen(x) == 2 && memcmp(x, "fo\0", 3) == 0)

    x = sdscat(x, "bar");
    test_cond("Strings concatenation",
              sdslen(x) == 5 && memcmp(x, "fobar\0", 6) == 0);

    x = sdscpy(x, "a");
    test_cond("sdscpy() against an originally longer string",
              sdslen(x) == 1 && memcmp(x, "a\0", 2) == 0)

    x = sdscpy(x, "xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk");
    test_cond("sdscpy() against an originally shorter string",
              sdslen(x) == 33 &&
              memcmp(x, "xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk\0", 33) == 0)

    sdsfree(x);
    x = sdscatprintf(sdsempty(), "%d", 123);
    test_cond("sdscatprintf() seems working in the base case",
              sdslen(x) == 3 && memcmp(x, "123\0", 4) == 0)

    sdsfree(x);
    x = sdsnew("--");
    x = sdscatfmt(x, "Hello %s World %I,%I--", "Hi!", LLONG_MIN, LLONG_MAX);
    test_cond("sdscatfmt() seems working in the base case",
              sdslen(x) == 60 &&
              memcmp(x, "--Hello Hi! World -9223372036854775808,"
                        "9223372036854775807--", 60) == 0)

    sdsfree(x);
    x = sdsnew("--");
    x = sdscatfmt(x, "%u,%U--", UINT_MAX, ULLONG_MAX);
    test_cond("sdscatfmt() seems working with unsigned numbers",
              sdslen(x) == 35 &&
              memcmp(x, "--4294967295,18446744073709551615--", 35) == 0)

    sdsfree(x);
    x = sdsnew("xxciaoyyy");
    sdstrim(x, "xy");
    test_cond("sdstrim() correctly trims characters",
              sdslen(x) == 4 && memcmp(x, "ciao\0", 5) == 0)

    y = sdsdup(x);
    sdsrange(y, 1, 1);
    test_cond("sdsrange(...,1,1)",
              sdslen(y) == 1 && memcmp(y, "i\0", 2) == 0)

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 1, -1);
    test_cond("sdsrange(...,1,-1)",
              sdslen(y) == 3 && memcmp(y, "iao\0", 4) == 0)

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, -2, -1);
    test_cond("sdsrange(...,-2,-1)",
              sdslen(y) == 2 && memcmp(y, "ao\0", 3) == 0)

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 2, 1);
    test_cond("sdsrange(...,2,1)",
              sdslen(y) == 0 && memcmp(y, "\0", 1) == 0)

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 1, 100);
    test_cond("sdsrange(...,1,100)",
              sdslen(y) == 3 && memcmp(y, "iao\0", 4) == 0)

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 100, 100);
    test_cond("sdsrange(...,100,100)",
              sdslen(y) == 0 && memcmp(y, "\0", 1) == 0)

    sdsfree(y);
    sdsfree(x);
    x = sdsnew("foo");
    y = sdsnew("foa");
    test_cond("sdscmp(foo,foa)", sdscmp(x, y) > 0)

    sdsfree(y);
    sdsfree(x);
    x = sdsnew("bar");
    y = sdsnew("bar");
    test_cond("sdscmp(bar,bar)", sdscmp(x, y) == 0)

    sdsfree(y);
    sdsfree(x);
    x = sdsnew("aar");
    y = sdsnew("bar");
    test_cond("sdscmp(bar,bar)", sdscmp(x, y) < 0)

    sdsfree(y);
    sdsfree(x);
    x = sdsnewlen("\a\n\0foo\r", 7);
    y = sdscatrepr(sdsempty(), x, sdslen(x));
    test_cond("sdscatrepr(...data...)",
              memcmp(y, "\"\\a\\n\\x00foo\\r\"", 15) == 0)

    {
        int oldfree;

        sdsfree(x);
        x = sdsnew("0");
        sh = (sdshdr *) (x - (sizeof(struct sdshdr)));
        test_cond("sdsnew() free/len buffers", sh->len == 1 && sh->free == 0);
        x = sdsMakeRoomFor(x, 1);
        sh = (sdshdr *) (x - (sizeof(struct sdshdr)));
        test_cond("sdsMakeRoomFor()", sh->len == 1 && sh->free > 0);
        oldfree = sh->free;
        x[1] = '1';
        sdsIncrLen(x, 1);
        test_cond("sdsIncrLen() -- content", x[0] == '0' && x[1] == '1');
        test_cond("sdsIncrLen() -- len", sh->len == 2);
        test_cond("sdsIncrLen() -- free", sh->free == oldfree - 1);
    }

    test_report();
    return 0;
}
