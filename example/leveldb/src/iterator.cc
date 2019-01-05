#include "iterator.h"

class EmptyIterator : public Iterator 
{
public:
	EmptyIterator(const Status &s): s(s) { }
	virtual ~EmptyIterator() = default;

	virtual bool valid() const  { return false; }
	virtual void seek(const std::string_view &target) { }
	virtual void seekToFirst() { }
	virtual void seekToLast() { }
	virtual void next() { assert(false); }
	virtual void prev() { assert(false); }
	virtual std::string_view key() const  { assert(false); return std::string_view(); }
	virtual std::string_view value() const  { assert(false); return std::string_view(); }
	virtual Status status() const { return s; }
	virtual void registerCleanup(const std::any &block) { assert(false);  } 

private:
	Status s;
};

std::shared_ptr<Iterator> newEmptyIterator()
{
	std::shared_ptr<Iterator> iter(new EmptyIterator(Status::OK()));
	return iter;
}

std::shared_ptr<Iterator> newErrorIterator(const Status &status)
{
	std::shared_ptr<Iterator> iter(new EmptyIterator(status));
	return iter;
}

std::shared_ptr<Iterator> newTwoLevelIterator(const std::shared_ptr<Iterator> &indexIter,
	const Callback &callback, const std::any &arg, const ReadOptions &options)
{
	std::shared_ptr<Iterator> iter(new TwoLevelIterator(indexIter, callback, arg, options));
	return iter;
}
	
