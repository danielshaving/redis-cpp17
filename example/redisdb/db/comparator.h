#pragma once
#include "dbformat.h"
#include "coding.h"

class ListsDataKeyComparatorImpl : public Comparator {
public:
    ListsDataKeyComparatorImpl() { }

    const char* Name() const override {
        return "redisdb.ListsDataKeyComparator";
    }

    int Compare(const std::string_view& a, const std::string_view& b) const override {
        assert(!a.empty() && !b.empty());
        const char* ptra = a.data();
        const char* ptrb = b.data();

        int32_t asize = static_cast<int32_t>(a.size());
        int32_t bsize = static_cast<int32_t>(b.size());
        
        int32_t keyalen = DecodeFixed32(ptra);
        int32_t keyblen = DecodeFixed32(ptrb);

        ptra += sizeof(int32_t);
        ptrb += sizeof(int32_t);

        std::string_view setskeya(ptra,  keyalen);
        std::string_view setskeyb(ptrb,  keyblen);

        ptra += keyalen;
        ptrb += keyblen;
        
        if (setskeya != setskeyb) {
            return setskeya.compare(setskeyb);
        }

        if (ptra - a.data() == asize &&
            ptrb - b.data() == bsize) {
            return 0;
        } else if (ptra - a.data() == asize) {
            return -1;
        } else if (ptrb - b.data() == bsize) {
            return 1;
        }

        int32_t versiona = DecodeFixed32(ptra);
        int32_t versionb = DecodeFixed32(ptrb);

        ptra += sizeof(int32_t);
        ptrb += sizeof(int32_t);
        if (versiona != versionb) {
            return versiona < versionb ? -1 : 1;
        }
        
        if (ptra - a.data() == asize &&
            ptrb - b.data() == bsize) {
            return 0;
        } else if (ptra - a.data() == asize) {
            return -1;
        } else if (ptrb - b.data() == bsize) {
            return 1;
        }

        uint64_t indexa = DecodeFixed64(ptra);
        uint64_t indexb = DecodeFixed64(ptrb);

        ptra += sizeof(uint64_t);
        ptrb += sizeof(uint64_t);
        if (indexa != indexb) {
            return indexa < indexb ? -1 : 1;
        } else {
            return 0;
        }
    }

    bool Equal(const std::string_view& a, const std::string_view& b) const override {
        return !Compare(a, b);
    }

    void FindShortestSeparator(std::string* start,
                                const std::string_view& limit) const override {
    }

    void FindShortSuccessor(std::string* key) const override {
    }
};

class ZSetsScoreKeyComparatorImpl : public Comparator {
public:
    const char* Name() const override {
        return "redisdb.ZSetsScoreKeyComparator";
    }

    int Compare(const std::string_view& a, const std::string_view& b) const override {
        assert(a.size() > sizeof(int32_t));
        assert(a.size() >= DecodeFixed32(a.data())
                + 2 * sizeof(int32_t) + sizeof(uint64_t));
        assert(b.size() > sizeof(int32_t));
        assert(b.size() >= DecodeFixed32(b.data())
                + 2 * sizeof(int32_t) + sizeof(uint64_t));

        const char* ptra = a.data();
        const char* ptrb = b.data();

        int32_t asize = static_cast<int32_t>(a.size());
        int32_t bsize = static_cast<int32_t>(b.size());

        int32_t keyalen = DecodeFixed32(ptra);
        int32_t keyblen = DecodeFixed32(ptrb);

        std::string_view keyaprefix(ptra,  keyalen + 2 * sizeof(int32_t));
        std::string_view kkeybprefix(ptrb,  keyblen + 2 * sizeof(int32_t));

        ptra += keyalen + 2 * sizeof(int32_t);
        ptrb += keyblen + 2 * sizeof(int32_t);
        int ret = keyaprefix.compare(kkeybprefix);
        if (ret) {
            return ret;
        }

        uint64_t ai = DecodeFixed64(ptra);
        uint64_t bi = DecodeFixed64(ptrb);

        const void* ptrascore = reinterpret_cast<const void*>(&ai);
        const void* ptrbscore = reinterpret_cast<const void*>(&bi);

        double ascore = *reinterpret_cast<const double*>(ptrascore);
        double bscore = *reinterpret_cast<const double*>(ptrbscore);

        ptra += sizeof(uint64_t);
        ptrb += sizeof(uint64_t);
        if (ascore != bscore) {
            return ascore < bscore  ? -1 : 1;
        } else {
            if (ptra - a.data() == asize && ptrb - b.data() == bsize) {
                return 0;
            } else if (ptra - a.data() == asize) {
                return -1;
            } else if (ptrb - b.data() == bsize) {
             return 1;
            } else {
                std::string_view keyamember(ptra, asize - (ptra - a.data()));
                std::string_view keybmember(ptrb, bsize - (ptrb - b.data()));
                ret = keyamember.compare(keybmember);
                if (ret) {
                    return ret;
                }
            }
        }
        return 0;
    }

    bool Equal(const std::string_view& a, const std::string_view& b) const override {
        return !Compare(a, b);
    }

    void ParseAndPrintZSetsScoreKey(const std::string& from, const std::string& str) {
        const char* ptr = str.data();

        int32_t keylen = DecodeFixed32(ptr);
        ptr += sizeof(int32_t);

        std::string key(ptr, keylen);
        ptr += keylen;

        int32_t version = DecodeFixed32(ptr);
        ptr += sizeof(int32_t);

        uint64_t keyscorei = DecodeFixed64(ptr);
        const void* ptrkeyscore = reinterpret_cast<const void*>(&keyscorei);
        double score = *reinterpret_cast<const double*>(ptrkeyscore);
        ptr += sizeof(uint64_t);


        std::string member(ptr, str.size() - (keylen + 2 * sizeof(int32_t) + sizeof(uint64_t)));
        printf("%s: total_len[%lu], keylen[%d], key[%s], version[%d], score[%lf], member[%s]\n",
                from.data(), str.size(), keylen, key.data(), version, score, member.data());
    }

    // Advanced functions: these are used to reduce the space requirements
    // for internal data structures like index blocks.

    // If *start < limit, changes *start to a short string in [start,limit).
    // Simple comparator implementations may return with *start unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    void FindShortestSeparator(std::string* start,
                                const std::string_view& limit) const override {
    }

    // Changes *key to a short string >= *key.
    // Simple comparator implementations may return with *key unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    void FindShortSuccessor(std::string* key) const override {
    }
};