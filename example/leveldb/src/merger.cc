#include "merger.h"
#include "iterator.h"

class MergingIterator : public Iterator
{
public:
    MergingIterator(const Comparator *comparator, std::vector<std::shared_ptr<Iterator>> &child, int n)
            : comparator(comparator),
              n(n),
              current(nullptr),
              direction(kForward)
    {
        children.reserve(n);
        for (int i = 0; i < n; i++)
        {
            children[i]->set(child[i]);
        }
    }

    virtual ~MergingIterator()
    {

    }

    virtual bool valid() const
    {
        return (current != nullptr);
    }

    virtual void seekToFirst()
    {
        for (int i = 0; i < n; i++)
        {
            children[i]->seekToFirst();
        }

        findSmallest();
        direction = kForward;
    }

    virtual void seekToLast()
    {
        for (int i = 0; i < n; i++)
        {
            children[i]->seekToLast();
        }

        findLargest();
        direction = kReverse;
    }

    virtual void seek(const std::string_view &target)
    {
        for (int i = 0; i < n; i++)
        {
            children[i]->seek(target);
        }

        findSmallest();
        direction = kForward;
    }

    virtual void next()
    {
        assert(valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction != kForward)
        {
            for (int i = 0; i < n; i++)
            {
                auto child = children[i];
                if (child != current)
                {
                    child->seek(key());
                    if (child->valid() &&
                        comparator->compare(key(), child->key()) == 0)
                    {
                        child->next();
                    }
                }
            }
            direction = kForward;
        }

        current->next();
        findSmallest();
    }

    virtual void prev()
    {
        assert(valid());

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction != kReverse)
        {
            for (int i = 0; i < n; i++)
            {
                auto child = children[i];
                if (child != current)
                {
                    child->seek(key());
                    if (child->valid())
                    {
                        // Child is at first entry >= key().  Step back one to be < key()
                        child->prev();
                    }
                    else
                    {
                        // Child has no entries >= key().  Position at last entry.
                        child->seekToLast();
                    }
                }
            }
            direction = kReverse;
        }

        current->prev();
        findLargest();
    }

    virtual std::string_view key() const
    {
        assert(valid());
        return current->key();
    }

    virtual std::string_view value() const
    {
        assert(valid());
        return current->value();
    }

    virtual Status status() const
    {
        Status status;
        for (int i = 0; i < n; i++)
        {
            status = children[i]->status();
            if (!status.ok())
            {
                break;
            }
        }
        return status;
    }

    virtual void registerCleanup(const std::any &arg)
    {
    	clearnups.push_back(arg);
    }
private:
    void findSmallest();
    void findLargest();

    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in leveldb.
    const Comparator *comparator;
    std::vector<std::shared_ptr<IteratorWrapper>> children;
    std::shared_ptr<IteratorWrapper> current;

    int n;
    // Which direction is the iterator moving?
    enum Direction
    {
        kForward,
        kReverse
    };
    Direction direction;
    std::list<std::any> clearnups;
};

void MergingIterator::findSmallest()
{
	std::shared_ptr<IteratorWrapper> smallest = nullptr;
	for (int i = 0; i < n; i++)
	{
		std::shared_ptr<IteratorWrapper> child = children[i];
		if (child->valid())
		{
			if (smallest == nullptr)
			{
				smallest = child;
			}
			else if (comparator->compare(child->key(), smallest->key()) < 0)
			{
				smallest = child;
			}
		}
	}
	current = smallest;
}

void MergingIterator::findLargest()
{
	std::shared_ptr<IteratorWrapper> largest = nullptr;
	for (int i = n - 1; i >= 0; i--)
	{
		std::shared_ptr<IteratorWrapper> child = children[i];
		if (child->valid())
		{
			if (largest == nullptr)
			{
				largest = child;
			}
			else if (comparator->compare(child->key(), largest->key()) > 0)
			{
				largest = child;
			}
		}
	}
	current = largest;
}

std::shared_ptr<Iterator> newMergingIterator(
        const Comparator *cmp, std::vector<std::shared_ptr<Iterator>> &list, int n)
{
    assert(n >= 0);
    if (n == 0)
    {
        return newEmptyIterator();
    }
    else if (n == 1)
    {
        return list[0];
    }
    else
    {
    	std::shared_ptr<MergingIterator> iter(new MergingIterator(cmp, list, n));
        return iter;
    }
}
