#include "util.h"
#include "arena.h"

int main(int argc, char **argv) {
    std::vector <std::pair<size_t, char *>> allocated;
    Arena arena;
    const int N = 100000;
    size_t bytes = 0;
    Random rnd(301);
    for (int i = 0; i < N; i++) {
        size_t s;
        if (i % (N / 10) == 0) {
            s = i;
        } else {
            s = rnd.oneIn(4000) ? rnd.uniform(6000) :
                (rnd.oneIn(10) ? rnd.uniform(100) : rnd.uniform(20));
        }
        if (s == 0) {
            // Our arena disallows size 0 allocations.
            s = 1;
        }

        char *r;
        if (rnd.oneIn(10)) {
            r = arena.allocateAligned(s);
        } else {
            r = arena.allocate(s);
        }

        for (size_t b = 0; b < s; b++) {
            // Fill the "i"th allocation with a known bit pattern
            r[b] = i % 256;
        }

        bytes += s;
        allocated.push_back(std::make_pair(s, r));
        assert(arena.getMemoryUsage() >= bytes);
        if (i > N / 10) {
            assert(arena.getMemoryUsage() <= bytes * 1.10);
        }
    }

    for (size_t i = 0; i < allocated.size(); i++) {
        size_t numBytes = allocated[i].first;
        const char *p = allocated[i].second;
        for (size_t b = 0; b < numBytes; b++) {
            // Check the "i"th allocation for the known bit pattern
            assert((int(p[b]) & 0xff) == (i % 256));
        }
    }

    return 0;
}
