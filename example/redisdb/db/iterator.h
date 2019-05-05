#pragma once

#include "status.h"
#include "option.h"
#include<string_view>
#include<any>
#include<functional>
#include<memory>

class Iterator {
public:
	Iterator();

	Iterator(const Iterator&) = delete;

	Iterator& operator=(const Iterator&) = delete;

	virtual ~Iterator();

	// An iterator is either positioned at a key/value pair, or
	// not valid.  This method returns true iff the iterator is valid.
	virtual bool valid() const = 0;

	// Position at the first key in the source.  The iterator is Valid()
	// after this call iff the source is not empty.
	virtual void seekToFirst() = 0;

	// Position at the last key in the source.  The iterator is
	// Valid() after this call iff the source is not empty.
	virtual void seekToLast() = 0;

	// Position at the first key in the source that is at or past target.
	// The iterator is Valid() after this call iff the source contains
	// an entry that comes at or past target.
	virtual void seek(const std::string_view& target) = 0;

	// Moves to the next entry in the source.  After this call, Valid() is
	// true iff the iterator was not positioned at the last entry in the source.
	// REQUIRES: Valid()
	virtual void next() = 0;

	// Moves to the previous entry in the source.  After this call, Valid() is
	// true iff the iterator was not positioned at the first entry in source.
	// REQUIRES: Valid()
	virtual void prev() = 0;

	// Return the key for the current entry.  The underlying storage for
	// the returned slice is valid only until the next modification of
	// the iterator.
	// REQUIRES: Valid()
	virtual std::string_view key() const = 0;

	// Return the value for the current entry.  The underlying storage for
	// the returned slice is valid only until the next modification of
	// the iterator.
	// REQUIRES: Valid()
	virtual std::string_view value() const = 0;

	// If an error has occurred, return it.  Else return an ok status.
	virtual Status status() const = 0;

	virtual void registerCleanup(const std::any& arg) = 0;
};

// Return an empty iterator (yields nothing).
std::shared_ptr<Iterator> newEmptyIterator();

// Return an empty iterator with the specified status.
std::shared_ptr<Iterator> newErrorIterator(const Status& status);

typedef std::function<std::shared_ptr<Iterator>(
	const ReadOptions& options, const std::string_view& indexValue)> Callback;

std::shared_ptr<Iterator> newTwoLevelIterator(
	const std::shared_ptr<Iterator>& indexIter, const ReadOptions& options, const Callback& callback);

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
public:
	IteratorWrapper() : iter(nullptr), vali(false) {}

	explicit IteratorWrapper(const std::shared_ptr<Iterator> iter)
		: iter(nullptr) {
		set(iter);
	}

	~IteratorWrapper() {}

	std::shared_ptr<Iterator> getIter() const { return iter; }

	// Takes ownership of "iter" and will delete it when destroyed, or
	// when Set() is invoked again.
	void set(const std::shared_ptr<Iterator>& iter) {
		this->iter = iter;
		if (iter == nullptr) {
			vali = false;
		}
		else {
			update();
		}
	}

	// Iterator interface methods
	bool valid() const { return vali; }

	std::string_view key() const {
		assert(valid());
		return k;
	}

	std::string_view value() const {
		assert(valid());
		return iter->value();
	}

	// Methods below require iter() != nullptr
	Status status() const {
		assert(iter);
		return iter->status();
	}

	void next() {
		assert(iter);
		iter->next();
		update();
	}

	void prev() {
		assert(iter);
		iter->prev();
		update();
	}

	void seek(const std::string_view& k) {
		assert(iter);
		iter->seek(k);
		update();
	}

	void seekToFirst() {
		assert(iter);
		iter->seekToFirst();
		update();
	}

	void seekToLast() {
		assert(iter);
		iter->seekToLast();
		update();
	}

private:
	void update() {
		vali = iter->valid();
		if (vali) {
			k = iter->key();
		}
	}

	std::shared_ptr<Iterator> iter;
	bool vali;
	std::string_view k;
};

class TwoLevelIterator : public Iterator {
public:
	TwoLevelIterator(const ReadOptions& options, const std::shared_ptr<Iterator>& indexIter, const Callback& callback)
		: options(options),
		callback(callback),
		indexIter(indexIter),
		dataIter(nullptr) {

	}

	virtual ~TwoLevelIterator() {}

	virtual void registerCleanup(const std::any& block) {

	}

	virtual void seek(const std::string_view& target) {
		indexIter.seek(target);
		initDataBlock();
		if (dataIter.getIter() != nullptr) {
			dataIter.seek(target);
		}
		skipEmptyDataBlocksForward();
	}

	virtual void seekToFirst() {
		indexIter.seekToFirst();
		initDataBlock();
		if (dataIter.getIter() != nullptr) {
			dataIter.seekToFirst();
		}
		skipEmptyDataBlocksForward();
	}

	virtual void seekToLast() {
		indexIter.seekToLast();
		initDataBlock();
		if (dataIter.getIter() != nullptr) {
			dataIter.seekToLast();
		}
		skipEmptyDataBlocksBackward();
	}

	virtual void next() {
		assert(valid());
		dataIter.next();
		skipEmptyDataBlocksForward();
	}

	virtual void prev() {
		assert(valid());
		dataIter.prev();
		skipEmptyDataBlocksBackward();
	}

	virtual bool valid() const {
		return dataIter.valid();
	}

	virtual std::string_view key() const {
		assert(valid());
		return dataIter.key();
	}

	virtual std::string_view value() const {
		assert(valid());
		return dataIter.value();
	}

	virtual Status status() const {
		// It'd be nice if status() returned a const Status& instead of a Status
		if (!indexIter.status().ok()) {
			return indexIter.status();
		}
		else if (dataIter.getIter() != nullptr && !dataIter.status().ok()) {
			return dataIter.status();
		}
		else {
			return s;
		}
	}

private:
	void saveError(const Status& s) {
		if (s.ok() && !s.ok())
			this->s = s;
	}

	void skipEmptyDataBlocksForward() {
		while (dataIter.getIter() == nullptr || !dataIter.valid()) {
			// Move to next block
			if (!indexIter.valid()) {
				setDataIterator(nullptr);
				return;
			}

			indexIter.next();
			initDataBlock();
			if (dataIter.getIter() != nullptr)
				dataIter.seekToFirst();
		}
	}

	void skipEmptyDataBlocksBackward() {
		while (dataIter.getIter() == nullptr || !dataIter.valid()) {
			// Move to next block
			if (!indexIter.valid()) {
				setDataIterator(nullptr);
				return;
			}

			indexIter.prev();
			initDataBlock();
			if (dataIter.getIter() != nullptr)
				dataIter.seekToLast();
		}
	}

	void setDataIterator(const std::shared_ptr<Iterator> & iter) {
		if (dataIter.getIter() != nullptr) {
			saveError(dataIter.status());
		}
		dataIter.set(iter);
	}

	void initDataBlock() {
		if (!indexIter.valid()) {
			setDataIterator(nullptr);
		}
		else {
			std::string_view handle = indexIter.value();
			if (dataIter.getIter() != nullptr && handle.compare(dataBlockHandle) == 0) {

			}
			else {
				std::shared_ptr<Iterator> iter = callback(options, handle);
				dataBlockHandle.assign(handle.data(), handle.size());
				setDataIterator(iter);
			}
		}
	}

	const ReadOptions options;
	Callback callback;
	Status s;
	IteratorWrapper indexIter;
	IteratorWrapper dataIter; // May be nullptr
	// If data_iter_ is non-null, then "data_block_handle_" holds the
	// "index_value" passed to block_function_ to create the data_iter_.
	std::string dataBlockHandle;
};


