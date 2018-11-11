#pragma once
#include <stdint.h>
#include <string_view>
#include <any>
#include <functional>
#include <memory>
#include "status.h"

class Block;
class BlockHandle;
class Footer;
struct Options;
class PosixMmapReadableFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class Table
{
public:
	// Attempt to open the table that is stored in bytes [0..file_size)
	// of "file", and read the metadata entries necessary to allow
	// retrieving data from the table.
	//
	// If successful, returns ok and sets "*table" to the newly opened
	// table.  The client should delete "*table" when no longer needed.
	// If there was an error while initializing the table, sets "*table"
	// to nullptr and returns a non-ok status.  Does not take ownership of
	// "*source", but the client must ensure that "source" remains live
	// for the duration of the returned table's lifetime.
	//
	// *file must remain live while this Table is in use.
	static Status open(const Options &options,
		std::shared_ptr<PosixMmapReadableFile> &file,
		uint64_t fileSize,
		std::shared_ptr<Table> &table);

	Table(const Table&) = delete;
	void operator=(const Table&) = delete;

	~Table();

	// Given a key, return an approximate byte offset in the file where
	// the data for that key begins (or would begin if the key were
	// present in the file).  The returned value is in terms of file
	// bytes, and so includes effects like compression of the underlying data.
	// E.g., the approximate offset of the last key in the table will
	// be close to the file length.
	uint64_t approximateOffsetOf(const std::string_view &key) const;
	
	// Calls (*handle_result)(arg, ...) with the entry found after a call
	// to Seek(key).  May not make such a call if filter policy says
	// that key is not present.
	Status internalGet(
		const ReadOptions &options, 
		const std::string_view &key,
		const std::any &arg,
		std::function<void(const std::any &arg,
		const std::string_view &k, const std::string_view &v)> &callback);
		
private:
	struct Rep;
	std::shared_ptr<Rep> rep;

	explicit Table(const std::shared_ptr<Rep> &rep)
	:rep(rep)
	{
		
	}

	void readMeta(const Footer &footer);
	void readFilter(const std::string_view &filterHandleValue);
};
