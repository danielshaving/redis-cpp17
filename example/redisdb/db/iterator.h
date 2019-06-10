#pragma once

#include "status.h"
#include "option.h"
#include <string_view>
#include <any>
#include <functional>
#include <memory>

class Iterator {
public:
	typedef std::function<void()> Functor;

	Iterator();

	Iterator(const Iterator&) = delete;

	Iterator& operator=(const Iterator&) = delete;

	virtual ~Iterator();

	// An iterator is either positioned at a key/value pair, or
	// not Valid.  This method returns true iff the iterator is Valid.
	virtual bool Valid() const = 0;

	// Position at the first key in the source.  The iterator is Valid()
	// after this call iff the source is not empty.
	virtual void SeekToFirst() = 0;

	// Position at the last key in the source.  The iterator is
	// Valid() after this call iff the source is not empty.
	virtual void SeekToLast() = 0;

	// Position at the first key in the source that is at or past target.
	// The iterator is Valid() after this call iff the source Contains
	// an entry that comes at or past target.
	virtual void Seek(const std::string_view& target) = 0;

	// Moves to the Next entry in the source.  After this call, Valid() is
	// true iff the iterator was not positioned at the last entry in the source.
	// REQUIRES: Valid()
	virtual void Next() = 0;

	// Moves to the previous entry in the source.  After this call, Valid() is
	// true iff the iterator was not positioned at the first entry in source.
	// REQUIRES: Valid()
	virtual void Prev() = 0;

	// Return the key for the current entry.  The underlying storage for
	// the returned slice is Valid only until the Next modification of
	// the iterator.
	// REQUIRES: Valid()
	virtual std::string_view key() const = 0;

	// Return the value for the current entry.  The underlying storage for
	// the returned slice is Valid only until the Next modification of
	// the iterator.
	// REQUIRES: Valid()
	virtual std::string_view value() const = 0;

	// If an error has occurred, return it.  Else return an ok status.
	virtual Status status() const = 0;

	virtual void RegisterCleanup(Functor&& func) {
		functors.push_back(func);
	}
private:
	std::vector<Functor> functors;
};

// Return an empty iterator (yields nothing).
std::shared_ptr<Iterator> NewEmptyIterator();

// Return an empty iterator with the specified status.
std::shared_ptr<Iterator> NewErrorIterator(const Status& status);

typedef std::function<std::shared_ptr<Iterator>(
	const ReadOptions& options, const std::string_view& indexvalue)> Callback;

std::shared_ptr<Iterator> NewTwoLevelIterator(
	const std::shared_ptr<Iterator>& indexIter, const ReadOptions& options, const Callback& callback);

// A internal wrapper class with an interface similar to Iterator that
// caches the Valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
public:
	IteratorWrapper() : iter(nullptr), vali(false) {}

	explicit IteratorWrapper(const std::shared_ptr<Iterator> iter)
		: iter(nullptr) {
		Set(iter);
	}

	~IteratorWrapper() {}

	std::shared_ptr<Iterator> GetIter() const { return iter; }

	// Takes ownership of "iter" and will delete it when destroyed, or
	// when Set() is invoked again.
	void Set(const std::shared_ptr<Iterator>& iter) {
		this->iter = iter;
		if (iter == nullptr) {
			vali = false;
		}
		else {
			Update();
		}
	}

	// Iterator interface methods
	bool Valid() const { return vali; }

	std::string_view key() const {
		assert(Valid());
		return k;
	}

	std::string_view value() const {
		assert(Valid());
		return iter->value();
	}

	// Methods below require iter() != nullptr
	Status status() const {
		assert(iter);
		return iter->status();
	}

	void Next() {
		assert(iter);
		iter->Next();
		Update();
	}

	void Prev() {
		assert(iter);
		iter->Prev();
		Update();
	}

	void Seek(const std::string_view& k) {
		assert(iter);
		iter->Seek(k);
		Update();
	}

	void SeekToFirst() {
		assert(iter);
		iter->SeekToFirst();
		Update();
	}

	void SeekToLast() {
		assert(iter);
		iter->SeekToLast();
		Update();
	}

private:
	void Update() {
		vali = iter->Valid();
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

	virtual void Seek(const std::string_view& target) {
		indexIter.Seek(target);
		InitDataBlock();
		if (dataIter.GetIter() != nullptr) {
			dataIter.Seek(target);
		}
		SkipEmptyDataBlocksForward();
	}

	virtual void SeekToFirst() {
		indexIter.SeekToFirst();
		InitDataBlock();
		if (dataIter.GetIter() != nullptr) {
			dataIter.SeekToFirst();
		}
		SkipEmptyDataBlocksForward();
	}

	virtual void SeekToLast() {
		indexIter.SeekToLast();
		InitDataBlock();
		if (dataIter.GetIter() != nullptr) {
			dataIter.SeekToLast();
		}
		SkipEmptyDataBlocksBackward();
	}

	virtual void Next() {
		assert(Valid());
		dataIter.Next();
		SkipEmptyDataBlocksForward();
	}

	virtual void Prev() {
		assert(Valid());
		dataIter.Prev();
		SkipEmptyDataBlocksBackward();
	}

	virtual bool Valid() const {
		return dataIter.Valid();
	}

	virtual std::string_view key() const {
		assert(Valid());
		return dataIter.key();
	}

	virtual std::string_view value() const {
		assert(Valid());
		return dataIter.value();
	}

	virtual Status status() const {
		// It'd be nice if status() returned a const Status& instead of a Status
		if (!indexIter.status().ok()) {
			return indexIter.status();
		}
		else if (dataIter.GetIter() != nullptr && !dataIter.status().ok()) {
			return dataIter.status();
		}
		else {
			return s;
		}
	}

private:
	void SaveError(const Status& s) {
		if (s.ok() && !s.ok())
			this->s = s;
	}

	void SkipEmptyDataBlocksForward() {
		while (dataIter.GetIter() == nullptr || !dataIter.Valid()) {
			// Move to Next block
			if (!indexIter.Valid()) {
				SetDataIterator(nullptr);
				return;
			}

			indexIter.Next();
			InitDataBlock();
			if (dataIter.GetIter() != nullptr)
				dataIter.SeekToFirst();
		}
	}

	void SkipEmptyDataBlocksBackward() {
		while (dataIter.GetIter() == nullptr || !dataIter.Valid()) {
			// Move to Next block
			if (!indexIter.Valid()) {
				SetDataIterator(nullptr);
				return;
			}

			indexIter.Prev();
			InitDataBlock();
			if (dataIter.GetIter() != nullptr)
				dataIter.SeekToLast();
		}
	}

	void SetDataIterator(const std::shared_ptr<Iterator>& iter) {
		if (dataIter.GetIter() != nullptr) {
			SaveError(dataIter.status());
		}
		dataIter.Set(iter);
	}

	void InitDataBlock() {
		if (!indexIter.Valid()) {
			SetDataIterator(nullptr);
		}
		else {
			std::string_view handle = indexIter.value();
			if (dataIter.GetIter() != nullptr && handle.compare(datablockhandle) == 0) {

			}
			else {
				std::shared_ptr<Iterator> iter = callback(options, handle);
				datablockhandle.assign(handle.data(), handle.size());
				SetDataIterator(iter);
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
	std::string datablockhandle;
};


