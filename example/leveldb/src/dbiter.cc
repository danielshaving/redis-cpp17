#include "dbiter.h"
#include "filename.h"
#include "dbformat.h"
#include "dbimpl.h"
#include "status.h"

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator 
{
public:
	// Which direction is the iterator currently moving?
	// (1) When moving forward, the internal iterator is positioned at
	//     the exact entry that yields this->key(), this->value()
	// (2) When moving backwards, the internal iterator is positioned
	//     just before all entries whose user key == this->key().
	enum Direction 
	{
		kForward,
		kReverse
	};

	DBIter(DBImpl *db, const Comparator *cmp, 
		const std::shared_ptr<Iterator> &iter, uint64_t s,
	     uint32_t seed)
	  : db(db),
	    comparator(cmp),
	    iter(iter),
	    sequence(s),
	    direction(kForward),
	    vali(false)
	{

	}
		
	virtual ~DBIter()
	{

	}
	
	virtual bool valid() const { return vali; }
	virtual std::string_view key() const 
	{
		assert(vali);
		return (direction == kForward) ? extractUserKey(iter->key()) : savedKey;
	}
	
	virtual std::string_view value() const 
	{
		assert(vali);
		return (direction == kForward) ? iter->value() : savedValue;
	}
	
	virtual Status status() const
	{
		if (s.ok())
		{
			return iter->status();
		} 
		else 
		{
		    return s;
		}
	}

	virtual void next();
	virtual void prev();
	virtual void seek(const std::string_view &target);
	virtual void seekToFirst();
	virtual void seekToLast();
    virtual void registerCleanup(const std::any &arg) { }

private:
	void findNextUserEntry(bool skipping, std::string *skip);
	void findPrevUserEntry();
	bool parseKey(ParsedInternalKey *key);

	inline void saveKey(const std::string_view &k, std::string *dst) 
	{
		dst->assign(k.data(), k.size());
	}

	inline void clearSavedValue() 
	{
		if (savedValue.capacity() > 1048576) 
		{
			std::string empty;
			std::swap(empty, savedValue);
		} 
		else 
		{
			savedValue.clear();
		}
	}

	DBImpl *db;
	const Comparator *const comparator;
	std::shared_ptr<Iterator> const iter;
	uint64_t const sequence;

	Status s;
	std::string savedKey;     // == curren-t key when direction_==kReverse
	std::string savedValue;   // == current raw value when direction_==kReverse
	Direction direction;
	bool vali;

	std::default_random_engine random;

	// No copying allowed
	DBIter(const DBIter&);
	void operator=(const DBIter&);
};


inline bool DBIter::parseKey(ParsedInternalKey *ikey)
{
    std::string_view k = iter->key();
    if (!parseInternalKey(k, ikey))
    {
        s = Status::corruption("corrupted internal key in DBIter");
        return false;
    }
    else
    {
        return true;
    }
}

void DBIter::next()
{
    assert(vali);

    if (direction == kReverse)
    {
        // Switch directions?
        direction = kForward;
        // iter_ is pointing just before the entries for this->key(),
        // so advance into the range of entries for this->key() and then
        // use the normal skipping code below.
        if (!iter->valid())
        {
            iter->seekToFirst();
        }
        else
        {
            iter->next();
        }

        if (!iter->valid())
        {
            vali = false;
            savedKey.clear();
            return;
        }
        // saved_key_ already contains the key to skip past.
    }
    else
    {
        // Store in saved_key_ the current key so we skip it below.
        saveKey(extractUserKey(iter->key()), &savedKey);
    }

    findNextUserEntry(true, &savedKey);
}

void DBIter::findNextUserEntry(bool skipping, std::string *skip)
{
    // Loop until we hit an acceptable entry to yield
    assert(iter->valid());
    assert(direction == kForward);
    do
    {
        ParsedInternalKey ikey;
        if (parseKey(&ikey) && ikey.sequence <= sequence)
        {
            switch (ikey.type)
            {
                case kTypeDeletion:
                    // Arrange to skip all upcoming entries for this key since
                    // they are hidden by this deletion.
                    saveKey(ikey.userKey, skip);
                    skipping = true;
                    break;
                case kTypeValue:
                    if (skipping &&
                        comparator->compare(ikey.userKey, *skip) <= 0)
                    {
                        // Entry hidden
                    }
                    else
                    {
                        vali = true;
                        savedKey.clear();
                        return;
                    }
                    break;
            }
        }
        iter->next();
    } while (iter->valid());

    savedKey.clear();
    vali = false;
}

void DBIter::prev()
{
    assert(vali);

    if (direction == kForward)
    {  // Switch directions?
        // iter_ is pointing at the current entry.  Scan backwards until
        // the key changes so we can use the normal reverse scanning code.
        assert(iter->valid());  // Otherwise valid_ would have been false
        saveKey(extractUserKey(iter->key()), &savedKey);
        while (true)
        {
            iter->prev();
            if (!iter->valid())
            {
                vali = false;
                savedKey.clear();
                clearSavedValue();
                return;
            }
            if (comparator->compare(extractUserKey(iter->key()),
                                          savedKey) < 0)
            {
                break;
            }
        }
        direction = kReverse;
    }

    findPrevUserEntry();
}

void DBIter::findPrevUserEntry()
{
    assert(direction == kReverse);

    ValueType valueType = kTypeDeletion;
    if (iter->valid())
    {
        do
        {
            ParsedInternalKey ikey;
            if (parseKey(&ikey) && ikey.sequence <= sequence)
            {
                if ((valueType != kTypeDeletion) &&
                    comparator->compare(ikey.userKey, savedKey) < 0)
                {
                    // We encountered a non-deleted value in entries for previous keys,
                    break;
                }

                valueType = ikey.type;
                if (valueType == kTypeDeletion)
                {
                    savedKey.clear();
                    clearSavedValue();
                }
                else
                {
                    std::string_view rawValue = iter->value();
                    if (savedValue.capacity() > rawValue.size() + 1048576)
                    {
                        std::string empty;
                        std::swap(empty, savedValue);
                    }

                    saveKey(extractUserKey(iter->key()), &savedKey);
                    savedValue.assign(rawValue.data(), rawValue.size());
                }
            }
            iter->prev();
        } while (iter->valid());
    }

    if (valueType == kTypeDeletion)
    {
        // End
        vali = false;
        savedKey.clear();
        clearSavedValue();
        direction = kForward;
    }
    else
    {
        vali = true;
    }
}

void DBIter::seek(const std::string_view &target)
{
    direction = kForward;
    clearSavedValue();
    savedKey.clear();
    appendInternalKey(
            &savedKey, ParsedInternalKey(target, sequence, kValueTypeForSeek));
    iter->seek(savedKey);
    if (iter->valid())
    {
        findNextUserEntry(false, &savedKey /* temporary storage */);
    }
    else
    {
        vali = false;
    }
}

void DBIter::seekToFirst()
{
    direction = kForward;
    clearSavedValue();
    iter->seekToFirst();
    if (iter->valid())
    {
        findNextUserEntry(false, &savedKey /* temporary storage */);
    }
    else
    {
        vali = false;
    }
}

void DBIter::seekToLast()
{
    direction = kReverse;
    clearSavedValue();
    iter->seekToLast();
    findPrevUserEntry();
}


std::shared_ptr<Iterator> newDBIterator(DBImpl *db,
                        const Comparator *userCmp,
                        std::shared_ptr<Iterator> internalIter,
                        uint64_t sequence,
                        uint32_t seed)
{
	std::shared_ptr<Iterator> iter(new DBIter(db, userCmp, internalIter, sequence, seed));
	return iter;
}

