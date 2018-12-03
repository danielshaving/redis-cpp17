#pragma once
#include "status.h"
#include "option.h"
#include <string_view>
#include <any>
#include <functional>
#include <memory>

class Iterator
{
public:
	Iterator() { }

	Iterator(const Iterator&) = delete;
	Iterator& operator=(const Iterator&) = delete;

	virtual ~Iterator() { } 

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
	virtual void seek(const std::string_view &target) = 0;

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
	
	virtual void registerCleanup(const std::any &arg) = 0;
};

// Return an empty iterator (yields nothing).
std::shared_ptr<Iterator> newEmptyIterator();

// Return an empty iterator with the specified status.
std::shared_ptr<Iterator> newErrorIterator(const Status &status);

typedef std::function<std::shared_ptr<Iterator>(const std::any &arg,
		 const ReadOptions &options,
		 const std::string_view &indexValue)> BlockCallback;

std::shared_ptr<Iterator> newTwoLevelIterator(const std::shared_ptr<Iterator> &indexIter,
	const BlockCallback &callback , const std::any &arg, const ReadOptions &options);

