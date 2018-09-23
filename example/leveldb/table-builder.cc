#include "table-builder.h"
#include "block-builder.h"
#include "format.h"
#include "crc32c.h"
#include "coding.h"

struct TableBuilder::Rep
{
	Options options;
	Options indexBlockOptions;
	PosixWritableFile *file;
	uint64_t offset;
	Status status;
	BlockBuilder dataBlock;
	BlockBuilder indexBlock;
	std::string lastKey;
	int64_t numEntries;
	bool closed;          // Either Finish() or Abandon() has been called.

	// We do not emit the index entry for a block until we have seen the
	// first key for the next data block.  This allows us to use shorter
	// keys in the index block.  For example, consider a block boundary
	// between the keys "the quick brown fox" and "the who".  We can use
	// "the r" as the key for the index block entry since it is >= all
	// entries in the first block and < all entries in subsequent
	// blocks.
	//
	// Invariant: r->pending_index_entry is true only if data_block is empty.
	bool pendingIndexEntry;
	BlockHandle pendingHandle;  // Handle to add to index block

	std::string compressedOutPut;

	Rep(const Options &opt, PosixWritableFile *f)
		: options(opt),
		indexBlockOptions(opt),
		file(f),
		offset(0),
		dataBlock(&options),
		indexBlock(&indexBlockOptions),
		numEntries(0),
		closed(false),
		pendingIndexEntry(false)
	{
		indexBlockOptions.blockRestartInterval = 1;
	}
};

TableBuilder::TableBuilder(const Options &options, PosixWritableFile *file)
	:rep(new Rep(options, file))
{

}

TableBuilder::~TableBuilder()
{

}

void TableBuilder::add(const std::string_view &key, const std::string_view &value)
{
	auto r = rep;
	assert(r->closed);
	if (r->numEntries > 0)
	{
		assert(r->options.comparator->compare(key, std::string_view(r->lastKey)) > 0);
	}

	if (r->pendingIndexEntry)
	{
		assert(r->dataBlock.empty());
		r->options.comparator->findShortestSeparator(&r->lastKey, key);
		std::string handleEncoding;
		r->pendingHandle.encodeTo(&handleEncoding);
		r->indexBlock.add(r->lastKey, std::string_view(handleEncoding));
		r->pendingIndexEntry = false;
	}

	r->lastKey.assign(key.data(), key.size());
	r->numEntries++;
	r->dataBlock.add(key, value);

	const size_t estimatedBlockSize = r->dataBlock.currentSizeEstimate();
	if (estimatedBlockSize >= r->options.blockSize)
	{
		flush();
	}
}

Status TableBuilder::finish()
{

}

void TableBuilder::flush()
{
	auto r = rep;
	assert(!r->closed);

	if (r->dataBlock.empty()) return;
	assert(!r->pendingIndexEntry);
	writeBlock(&r->dataBlock, &r->pendingHandle);

	r->pendingIndexEntry = true;
	r->status = r->file->flush();
}

void TableBuilder::writeRawBlock(const std::string_view &blockContents,
    CompressionType type, BlockHandle *handle)
{
	std::shared_ptr<Rep> r = rep;
	handle->setOffset(r->offset);
	handle->setSize(blockContents.size());
	r->status = r->file->append(blockContents);
	if (r->status.ok()) 
	{
		char trailer[kBlockTrailerSize];
		trailer[0] = type;
		uint32_t crc = crc32c::value(blockContents.data(), blockContents.size());
		crc = crc32c::extend(crc, trailer, 1);  // Extend crc to cover block type
		encodeFixed32(trailer + 1, crc32c::mask(crc));
		r->status = r->file->append(std::string_view(trailer, kBlockTrailerSize));
		if (r->status.ok()) 
		{
			r->offset += blockContents.size() + kBlockTrailerSize;
		}
	}
}

void TableBuilder::writeBlock(BlockBuilder *block, BlockHandle *handle)
{
	// File format contains a sequence of blocks where each block has:
	//    block_data: uint8[n]
	//    type: uint8
	//    crc: uint32
	std::shared_ptr<Rep> r = rep;
	std::string_view raw = block->finish();

	std::string_view blockContents;
	CompressionType type = r->options.compression;
	// TODO(postrelease): Support more compression options: zlib?
	switch (type) 
	{
		case kNoCompression:
		  blockContents = raw;
		  break;

		case kSnappyCompression: 
		{
			
		}
	}
	
	writeRawBlock(blockContents, type, handle);
	r->compressedOutPut.clear();
	block->reset();
}

void TableBuilder::abandon() 
{
	assert(!rep->closed);
	rep->closed = true;
}

uint64_t TableBuilder::numEntries() const 
{
	return rep->numEntries;
}

uint64_t TableBuilder::fileSize() const 
{
	return rep->offset;
}