#pragma once

#include "all.h"
#include "xZmalloc.h"


template< typename KeyType,
          typename HashFn = std::hash<KeyType>,
          typename EqualKey = std::equal_to<KeyType>>
class xSortedSet
{
private:
    typedef typename std::unordered_map<KeyType, double, HashFn, EqualKey> DictType;
    typedef typename DictType::iterator DictTypeIterator;
    typedef typename DictType::const_iterator DictTypeConstIterator;
    typedef typename std::vector<KeyType> KeyVecType;
    typedef typename std::pair<KeyType, double> KeyScorePairType;
    typedef typename std::vector<KeyScorePairType> KeyScoreVecType;

private:
    static const int SKIPLIST_MAXLEVEL = 32;
    class xSkipListNode;
    class xSkipListLevel;
    class xRangeSpec;
private:
    unsigned long length()
    {
        return slength;
    }

    int randoslevel()
    {
        int level = 1;
        while ((random()&0xFFFF) < (0.25/*SKIPLIST_P*/ * 0xFFFF))
            level += 1;
        return (level<SKIPLIST_MAXLEVEL) ? level : SKIPLIST_MAXLEVEL;
    }

    xSkipListNode* insert(double score, KeyType key)
    {
        xSkipListNode *update[SKIPLIST_MAXLEVEL], *x;
        unsigned int rank[SKIPLIST_MAXLEVEL];
        int i, level;

        x = header;
        for (i = slevel-1; i >= 0; i--)
        {
            /* store rank that is crossed to reach the insert position */
            rank[i] = i == (slevel-1) ? 0 : rank[i+1];
            while (x->slevel[i].forward && x->slevel[i].forward->score < score)
            {
                rank[i] += x->slevel[i].span;
                x = x->slevel[i].forward;
            }
            update[i] = x;
        }
        /* For skiplist self, 'key' stands
         * we assume the key is not already inside, since we allow duplicated
         * scores, and the re-insertion of score and redis object should never
         * happen since the caller of zslInsert() should test in the hash table
         * if the element is already inside or not. */
        level = randoslevel();
        if (level > slevel)
        {
            for (i = slevel; i < level; i++)
            {
                rank[i] = 0;
                update[i] = header;
                update[i]->slevel[i].span = slength;
            }
            slevel = level;
        }

        x = (xSkipListNode*)zmalloc(sizeof(xSkipListNode));
        new(x) xSkipListNode(level, score, key);
        //x = new xSkipListNode(level, score, key);
        for (i = 0; i < level; i++)
         {
            x->slevel[i].forward = update[i]->slevel[i].forward;
            update[i]->slevel[i].forward = x;

            /* update span covered by update[i] as x is
             * inserted here */
            x->slevel[i].span = update[i]->slevel[i].span - (rank[0] - rank[i]);
            update[i]->slevel[i].span = (rank[0] - rank[i]) + 1;
        }

        /* increment span for untouched levels */
        for (i = level; i < slevel; i++)
        {
            update[i]->slevel[i].span++;
        }

        x->backward = (update[0] == header) ? nullptr : update[0];
        if (x->slevel[0].forward)
            x->slevel[0].forward->backward = x;
        else
            tail = x;
        slength++;
        return x;
    }

    void deleteNode(xSkipListNode *x, xSkipListNode **update)
    {
        int i;
        for (i = 0; i < slevel; i++)
        {
            if (update[i]->slevel[i].forward == x)
            {
                update[i]->slevel[i].span += x->slevel[i].span - 1;
                update[i]->slevel[i].forward = x->slevel[i].forward;
            }
            else
            {
                update[i]->slevel[i].span -= 1;
            }
        }

        if (x->slevel[0].forward)
        {
            x->slevel[0].forward->backward = x->backward;
        }
        else
        {
            tail = x->backward;
        }
        while(slevel > 1 && header->slevel[slevel-1].forward == nullptr)
            slevel--;
        slength--;
    }

    /* Delete an element with matching score/key from the skiplist. */
    bool erase(double score, KeyType key)
    {
        xSkipListNode *update[SKIPLIST_MAXLEVEL], *x;
        int i;
        x = header;
        for (i = slevel-1; i >= 0; i--)
        {
            while (x->slevel[i].forward && x->slevel[i].forward->score < score)
                x = x->slevel[i].forward;
            update[i] = x;
        }
        /* We may have multiple elements with the same score, what we need
         * is to find the element with both the right score and key. */
        x = x->slevel[0].forward;
        if (x && score == x->score && x->mKey == key)
        {
            deleteNode(x, update);
            zfree(x);
            //delete x;
            return true;
        }
        else
        {
            return false; /* not found */
        }
        return false; /* not found */
    }

    static bool scoreGtemin(double score, const xRangeSpec &spec)
    {
        return spec.minex ? (score > spec.min) : (score >= spec.min);
    }

    static bool scorelteMax(double score, const xRangeSpec &spec)
    {
        return spec.maxex ? (score < spec.max) : (score <= spec.max);
    }

    /* Returns if there is a part of the skiplist is in range. */
    bool isInRange(const xRangeSpec &range)
    {
        xSkipListNode *x;
        /* Test for ranges that will always be empty. */
        if (range.min > range.max || (range.min == range.max && (range.minex || range.maxex)))
            return false;
        x = tail;
        if (x == nullptr || !scoreGtemin(x->score, range))
            return false;
        x = header->slevel[0].forward;
        if (x == nullptr || !scorelteMax(x->score, range))
            return false;
        return true;
    }

    /* Find the first node that is contained in the specified range.
     * Returns nullptr when no element is contained in the range. */
    xSkipListNode* firstInRange(const xRangeSpec &range)
    {
        xSkipListNode *x;
        int i;

        /* If everything is out of range, return early. */
        if (!isInRange(range)) return nullptr;

        x = header;
        for (i = slevel-1; i >= 0; i--)
        {
            /* Go forward while *OUT* of range. */
            while (x->slevel[i].forward && !scoreGtemin(x->slevel[i].forward->score, range))
                x = x->slevel[i].forward;
        }

        /* This is an inner range, so the next node cannot be nullptr. */
        x = x->slevel[0].forward;
        assert(x != nullptr);

        /* Check if score <= max. */
        if (!scorelteMax(x->score, range)) return nullptr;
        return x;
    }

    /* Find the last node that is contained in the specified range.
     * Returns nullptr when no element is contained in the range. */
    xSkipListNode* lastInRange(const xRangeSpec &range)
    {
        xSkipListNode *x;
        int i;

        /* If everything is out of range, return early. */
        if (!isInRange(range)) return nullptr;

        x = header;
        for (i = slevel-1; i >= 0; i--)
        {
            /* Go forward while *IN* range. */
            while (x->slevel[i].forward && scorelteMax(x->slevel[i].forward->score, range))
                x = x->slevel[i].forward;
        }

        /* This is an inner range, so this node cannot be nullptr. */
        assert(x != nullptr);

        /* Check if score >= min. */
        if (!scoreGtemin(x->score, range)) return nullptr;
        return x;
    }

    /* Delete all the elements with score between min and max from the skiplist.
     * Min and max are inclusive, so a score >= min || score <= max is deleted.
     * Note that this function takes the reference to the hash table view of the
     * ordered set, in order to remove the elements from the hash table too. */
    unsigned long eraseRangeByScore(const xRangeSpec &range)
    {
        xSkipListNode *update[SKIPLIST_MAXLEVEL], *x;
        unsigned long removed = 0;
        int i;

        x = header;
        for (i = slevel-1; i >= 0; i--)
        {
            while (x->slevel[i].forward &&
                (range.minex ? x->slevel[i].forward->score <= range.min
                              : x->slevel[i].forward->score < range.min))
                x = x->slevel[i].forward;
            update[i] = x;
        }

        /* Current node is the last with score < or <= min. */
        x = x->slevel[0].forward;

        /* Delete nodes while in range. */
        while (x && (range.maxex ? x->score < range.max : x->score <= range.max))
        {
            xSkipListNode *next = x->slevel[0].forward;
            deleteNode(x, update);
            dict.erase(x->mKey);
            zfree(x);
            //delete x;
            removed++;
            x = next;
        }
        return removed;
    }

    /* Delete all the elements with rank between start and end from the skiplist.
     * Start and end are inclusive. Note that start and end need to be 1-based */
    unsigned long eraseRangeByRank(unsigned int start, unsigned int end)
    {
        xSkipListNode *update[SKIPLIST_MAXLEVEL], *x;
        unsigned long traversed = 0, removed = 0;
        int i;

        x = header;
        for (i = slevel-1; i >= 0; i--)
        {
            while (x->slevel[i].forward && (traversed + x->slevel[i].span) < start)
            {
                traversed += x->slevel[i].span;
                x = x->slevel[i].forward;
            }
            update[i] = x;
        }

        traversed++;
        x = x->slevel[0].forward;
        while (x && traversed <= end)
        {
            xSkipListNode *next = x->slevel[0].forward;
            deleteNode(x, update);
            dict.erase(x->mKey);
            zfree(x);
            removed++;
            traversed++;
            x = next;
        }
        return removed;
    }

    /* Find the rank for an element by both score and key.
     * Returns 0 when the element cannot be found, rank otherwise.
     * Note that the rank is 1-based due to the span of header to the
     * first element. */
    unsigned long getRank(double score, KeyType key)
    {
        xSkipListNode *x;
        unsigned long rank = 0;
        int i;

        x = header;
        for (i = slevel-1; i >= 0; i--) {
            while (x->slevel[i].forward && x->slevel[i].forward->score <= score)
            {
                rank += x->slevel[i].span;
                x = x->slevel[i].forward;
            }

            /* x might be equal to header, so test if is header */
            if (not x->Iheader && x->mKey == key)
            {
                return rank;
            }
        }
        return 0;
    }

    /* Finds an element by its rank. The rank argument needs to be 1-based. */
    xSkipListNode* getElementByRank(unsigned long rank)
	{
        xSkipListNode *x;
        unsigned long traversed = 0;
        int i;

        x = header;
        for (i = slevel-1; i >= 0; i--)
        {
            while (x->slevel[i].forward && (traversed + x->slevel[i].span) <= rank)
            {
                traversed += x->slevel[i].span;
                x = x->slevel[i].forward;
            }
            if (traversed == rank)
            {
                return x;
            }
        }
        return nullptr;
    }

    void zaddGeneric(KeyType key, double score, bool incr)
    {
        DictTypeIterator it = dict.find(key);
        if (it != dict.end())
        {
            double curscore = it->second;
            if (incr)
            {
                score += curscore;
            }
            if (score != curscore)
            {
                erase(curscore, key);
                insert(score, key);
                dict[key] = score;
            }
        }
        else
        {
            insert(score, key);
            dict[key] = score;
        }
    }

    void zaddGeneric(long start, long end, bool reverse, KeyVecType &result)
    {
        result.clear();

        /* Sanitize indexes. */
        long llen = length();
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen)
        {
            return;
        }
        if (end >= llen) end = llen-1;
        unsigned long rangelen = (end-start)+1;

        xSkipListNode *ln;
        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (reverse)
        {
            ln = tail;
            if (start > 0)
                ln = getElementByRank(llen-start);
        }
        else
        {
            ln = header->slevel[0].forward;
            if (start > 0)
                ln = getElementByRank(start+1);
        }

        while(rangelen--)
        {
            assert(ln != nullptr);
            result.push_back(ln->mKey);
            ln = reverse ? ln->backward : ln->slevel[0].forward;
        }
    }

    void zrangeWithscoresGeneric(long start, long end, bool reverse, KeyScoreVecType &result)
    {
        result.clear();

        /* Sanitize indexes. */
        long llen = length();
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen)
        {
            return;
        }
        if (end >= llen) end = llen-1;
        unsigned long rangelen = (end-start)+1;

        xSkipListNode *ln;
        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (reverse)
        {
            ln = tail;
            if (start > 0)
                ln = getElementByRank(llen-start);
        }
        else
        {
            ln = header->slevel[0].forward;
            if (start > 0)
                ln = getElementByRank(start+1);
        }

        while(rangelen--)
        {
            assert(ln != nullptr);
            result.push_back(std::make_pair(ln->mKey, ln->score));
            ln = reverse ? ln->backward : ln->slevel[0].forward;
        }
    }

    void zrangebyscoreGeneric(double min, double max, bool reverse, KeyVecType &result,
                               bool minex = false, bool maxex = false)
    {
        result.clear();
        xRangeSpec range((reverse?max:min), (reverse?min:max), (reverse?maxex:minex), (reverse?minex:maxex));
        xSkipListNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse)
        {
            ln = lastInRange(range);
        }
        else
        {
            ln = firstInRange(range);
        }

        /* No "first" element in the specified interval. */
        if (ln == nullptr)
        {
            return;
        }

        while (ln)
        {
            /* Abort when the node is no longer in range. */
            if (reverse)
            {
                if (!scoreGtemin(ln->score,range)) break;
            }
            else
            {
                if (!scorelteMax(ln->score,range)) break;
            }

            result.push_back(ln->mKey);

            /* Move to next node */
            if (reverse)
            {
                ln = ln->backward;
            }
            else
            {
                ln = ln->slevel[0].forward;
            }
        }
    }

    void zrangebyscoreWithscoresGeneric(double min, double max, bool reverse, KeyScoreVecType &result,
                                          bool minex = false, bool maxex = false)
    {
        result.clear();
        xRangeSpec range((reverse?max:min), (reverse?min:max), (reverse?maxex:minex), (reverse?minex:maxex));
        xSkipListNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = lastInRange(range);
        } else {
            ln = firstInRange(range);
        }

        /* No "first" element in the specified interval. */
        if (ln == nullptr)
        {
            return;
        }

        while (ln)
        {
            /* Abort when the node is no longer in range. */
            if (reverse)
            {
                if (!scoreGtemin(ln->score,range)) break;
            }
            else
            {
                if (!scorelteMax(ln->score,range)) break;
            }

            result.push_back(std::make_pair(ln->mKey, ln->score));

            /* Move to next node */
            if (reverse)
            {
                ln = ln->backward;
            }
            else
            {
                ln = ln->slevel[0].forward;
            }
        }
    }

    bool zrankGeneric(KeyType key, bool reverse, unsigned long &rank)
    {
        unsigned long llen = length();
        DictTypeIterator it = dict.find(key);
        if (it != dict.end())
        {
            double score = it->second;
            rank = getRank(score, key);
            if (reverse)
            {
                rank = llen - rank;
            }
            else
            {
                rank -= 1;
            }
            return true;
        }
        else
        {
            return false;
        }
    }

public:
    void zadd(KeyType key, double score)
    {
        zaddGeneric(key, score, false);
    }

    void zincrby(KeyType key, double score)
    {
        zaddGeneric(key, score, true);
    }

    void zrem(KeyType key)
    {
        DictTypeIterator it = dict.find(key);
        if (it != dict.end())
        {
            double score = it->second;
            erase(score, key);
            dict.erase(it);
        }
    }

    void zremrangebyscore(double min, double max, bool minex = false, bool maxex = false)
    {
        xRangeSpec range(min, max, minex, maxex);
        eraseRangeByScore(range);
    }

    void zremrangebyrank(long start, long end)
    {
        long llen = length();
        if (start < 0) start = llen + start;
        if (end < 0) end = llen + end;
        if (start < 0) start = 0;
        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen)
        {
            return;
        }
        if (end >= llen) end = llen - 1;
        /* Correct for 1-based rank. */
        eraseRangeByRank(start+1, end+1);
    }

    void zrange(long start, long end, KeyVecType &result)
    {
        zaddGeneric(start, end, false, result);
    }

    void zrevrange(long start, long end, KeyVecType &result)
    {
        zaddGeneric(start, end, true, result);
    }

    void zrangeWithscores(long start, long end, KeyScoreVecType &result)
    {
        zrangeWithscoresGeneric(start, end, false, result);
    }

    void zrevrangeWithscores(long start, long end, KeyScoreVecType &result)
    {
        zrangeWithscoresGeneric(start, end, true, result);
    }

    void zrangebyscore(double min, double max, KeyVecType &result, bool minex = false, bool maxex = false)
    {
        zrangebyscoreGeneric(min, max, false, result, minex, maxex);
    }

    void zrevrangebyscore(double min, double max, KeyVecType &result, bool minex = false, bool maxex = false)
    {
        zrangebyscoreGeneric(min, max, true, result, minex, maxex);
    }

    void zrangebyscoreWithscores(double min, double max, KeyScoreVecType &result, bool minex = false, bool maxex = false)
    {
    	zrangebyscoreWithscoresGeneric(min, max, false, result, minex, maxex);
    }

    void zrevrangebyscoreWithscores(double min, double max, KeyScoreVecType &result, bool minex = false, bool maxex = false)
    {
    	zrangebyscoreWithscoresGeneric(min, max, true, result, minex, maxex);
    }

    unsigned long zcount(double min, double max, bool minex = false, bool maxex = false)
    {
        xRangeSpec range(min, max, minex, maxex);
        xSkipListNode *zn;
        unsigned long rank;
        unsigned long count = 0;

        /* Find first element in range */
        zn = firstInRange(range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != nullptr)
        {
            rank = getRank(zn->score, zn->mKey);
            count = (slength - (rank - 1));

            /* Find last element in range */
            zn = lastInRange(range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != nullptr)
            {
                rank = getRank(zn->score, zn->mKey);
                count -= (slength - rank);
            }
        }
        return count;
    }

    unsigned long zcard()
    {
        return length();
    }

    bool zscore(KeyType key, double &score)
    {
        DictTypeConstIterator it = dict.find(key);
        if (it != dict.end())
        {
            score = it->second;
            return true;
        }
        else
        {
            return false;
        }
    }

    bool zrank(KeyType key, unsigned long &rank)
    {
        return zrankGeneric(key, false, rank);
    }

    bool zrevrank(KeyType key, unsigned long &rank)
    {
        return zrankGeneric(key, true, rank);
    }

public:
    xSortedSet():tail(nullptr), slength(0), slevel(1), dict()
    {

        header = (xSkipListNode*)zmalloc(sizeof(xSkipListNode));
        new(header)xSkipListNode(SKIPLIST_MAXLEVEL);

    }

    ~xSortedSet()
    {
        xSkipListNode *node = header->slevel[0].forward, *next;
        zfree(header);
        while (node)
        {
            next = node->slevel[0].forward;
            zfree(node);
            node = next;
        }
    }

private:
    class xSkipListNode
    {
    public:
        xSkipListNode(int level): Iheader(true), score(0), backward(nullptr)
        {
            slevel = (xSkipListLevel*)zmalloc(sizeof(xSkipListLevel) * level);
            for(int i = 0 ;i < level; i++)
            {
                new(&slevel[i])xSkipListLevel();
        	}

        }

        xSkipListNode(int level, double score, KeyType key): Iheader(false), score(score), mKey(key), backward(nullptr)
        {
			slevel = (xSkipListLevel*)zmalloc(sizeof(xSkipListLevel) * level);
			for(int i = 0 ;i < level; i++)
			{
				new(&slevel[i])xSkipListLevel();
			}
        }

        ~xSkipListNode()
        {
            if (slevel)
            {
                zfree(slevel);
            }
        }

    public:
        /* xSkipListNode maybe used as a SkipList's 'header' node, which will never take a real key/score.
         * So the key/score field of a 'header' xSkipListNode is always invalid,
         * and we need this flag to judge 'header' node. */
        bool Iheader;
        double score;
        KeyType mKey;
        xSkipListNode *backward;
        xSkipListLevel *slevel;
    };

    class xSkipListLevel
    {
    public:
        xSkipListLevel():forward(nullptr), span(0) {}
        xSkipListNode *forward;
        unsigned int span;
    };

    class xRangeSpec
    {
    public:
        xRangeSpec(double min, double max, bool minex, bool maxex):min(min), max(max), minex(minex), maxex(maxex) {}
    public:
        double min, max;
        bool minex, maxex;
    };

private:
    /* Data structure for the SkipList */
    xSkipListNode *header, *tail;
    unsigned long slength;
    int slevel;
    /* Data structure for the Dict */
    DictType dict;
};

