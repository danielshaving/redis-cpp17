#include "iterator.h"

Iterator::Iterator() {
	
}

Iterator::~Iterator() {
	for (size_t i = 0; i < functors.size(); ++i) {
		functors[i]();
	}
}

class EmptyIterator : public Iterator {
public:
	EmptyIterator(const Status& s) : s(s) {}

	virtual ~EmptyIterator() = default;

	virtual bool Valid() const { return false; }

	virtual void Seek(const std::string_view& target) {}

	virtual void SeekToFirst() {}

	virtual void SeekToLast() {}

	virtual void Next() { assert(false); }

	virtual void Prev() { assert(false); }

	virtual std::string_view key() const {
		assert(false);
		return std::string_view();
	}

	virtual std::string_view value() const {
		assert(false);
		return std::string_view();
	}

	virtual Status status() const { return s; }

private:
	Status s;
};

std::shared_ptr<Iterator> NewEmptyIterator() {
	std::shared_ptr<Iterator> iter(new EmptyIterator(Status::OK()));
	return iter;
}

std::shared_ptr<Iterator> NewErrorIterator(const Status& status) {
	std::shared_ptr<Iterator> iter(new EmptyIterator(status));
	return iter;
}

std::shared_ptr<Iterator> NewTwoLevelIterator(const std::shared_ptr<Iterator>& indexIter,
	const ReadOptions& options, const Callback& callback) {
	std::shared_ptr<Iterator> iter(new TwoLevelIterator(options, indexIter, callback));
	return iter;
}

