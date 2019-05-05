#include "tablebuilder.h"
#include "crc32c.h"
#include "coding.h"
#include "blockbuilder.h"
#include "env.h"
#include "format.h"

struct TableBuilder::Rep {
	Options options;
	Options indexBlockOptions;
	std::shared_ptr<WritableFile> file;
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
	// "the rep" as the key for the index block entry since it is >= all
	// entries in the first block and< all entries in subsequent
	// blocks.
	//
	// Invariant: rep->pending_index_entry is true only if data_block is empty.
	bool pendingIndexEntry;
	BlockHandle pendingHandle;  // Handle to add to index block
	std::string compressedOutPut;

	Rep(const Options& opt, const std::shared_ptr<WritableFile>& f)
		: options(opt),
		indexBlockOptions(opt),
		file(f),
		offset(0),
		dataBlock(&options),
		indexBlock(&indexBlockOptions),
		numEntries(0),
		closed(false),
		pendingIndexEntry(false) {
		indexBlockOptions.blockrestartinterval = 1;
	}
};

TableBuilder::TableBuilder(const Options& options, const std::shared_ptr<WritableFile>& file)
	: rep(new Rep(options, file)) {

}

TableBuilder::~TableBuilder() {
	assert(rep->closed);  // Catch errors where caller forgot to call Finish()
}

void TableBuilder::add(const std::string_view& key, const std::string_view& value) {
	assert(!rep->closed);
	if (!ok()) return;

	if (rep->numEntries > 0) {
		std::string_view lastKey(rep->lastKey.data(), rep->lastKey.size());
		assert(rep->options.comparator->compare(key, lastKey) > 0);
	}

	if (rep->pendingIndexEntry) {
		assert(rep->dataBlock.empty());
		//rep->options.comparator->findShortestSeparator(&rep->lastKey, key);
		std::string handleEncoding;
		rep->pendingHandle.encodeTo(&handleEncoding);
		rep->indexBlock.add(rep->lastKey, std::string_view(handleEncoding));
		rep->pendingIndexEntry = false;
	}

	rep->lastKey.assign(key.data(), key.size());
	rep->numEntries++;
	rep->dataBlock.add(key, value);

	const size_t estimatedBlockSize = rep->dataBlock.currentSizeEstimate();
	if (estimatedBlockSize >= rep->options.blocksize) {
		flush();
	}
}

Status TableBuilder::finish() {
	flush();
	assert(!rep->closed);
	rep->closed = true;

	BlockHandle metaindexBlockHandle, indexBlockHandle;
	// Write metaindex block
	if (ok()) {
		BlockBuilder metaIndexBlock(&rep->options);
		writeBlock(&metaIndexBlock, &metaindexBlockHandle);
	}

	// Write index block
	if (ok()) {
		if (rep->pendingIndexEntry) {
			//rep->options.comparator->findShortSuccessor(&rep->lastKey);
			std::string handleEncoding;
			rep->pendingHandle.encodeTo(&handleEncoding);
			rep->indexBlock.add(rep->lastKey, std::string_view(handleEncoding));
			rep->pendingIndexEntry = false;
		}
		writeBlock(&rep->indexBlock, &indexBlockHandle);
	}

	// Write footer
	if (ok()) {
		Footer footer;
		footer.setMetaindexHandle(metaindexBlockHandle);
		footer.setIndexHandle(indexBlockHandle);
		std::string footerEncoding;
		footer.encodeTo(&footerEncoding);
		rep->status = rep->file->append(footerEncoding);
		if (rep->status.ok()) {
			rep->offset += footerEncoding.size();
		}
	}
	return rep->status;
}

void TableBuilder::flush() {
	assert(!rep->closed);

	if (rep->dataBlock.empty()) return;
	assert(!rep->pendingIndexEntry);
	writeBlock(&rep->dataBlock, &rep->pendingHandle);

	rep->pendingIndexEntry = true;
	rep->status = rep->file->flush();
}

void TableBuilder::writeRawBlock(const std::string_view& blockContents,
	CompressionType type, BlockHandle* handle) {
	handle->setOffset(rep->offset);
	handle->setSize(blockContents.size());
	rep->status = rep->file->append(blockContents);
	if (rep->status.ok()) {
		char trailer[kBlockTrailerSize];
		trailer[0] = type;
		uint32_t crc = crc32c::value(blockContents.data(), blockContents.size());
		crc = crc32c::extend(crc, trailer, 1);  // Extend crc to cover block type
		encodeFixed32(trailer + 1, crc32c::mask(crc));
		rep->status = rep->file->append(std::string_view(trailer, kBlockTrailerSize));
		if (rep->status.ok()) {
			rep->offset += blockContents.size() + kBlockTrailerSize;
		}
	}
}

void TableBuilder::writeBlock(BlockBuilder* block, BlockHandle* handle) {
	// File format contains a sequence of blocks where each block has:
	//    block_data: uint8[n]
	//    type: uint8
	//    crc: uint32
	std::string_view raw = block->finish();

	std::string_view blockContents;
	CompressionType type = rep->options.compression;
	// TODO(postrelease): Support more compression options: zlib?
	switch (type) {
	case kNoCompression:
		blockContents = raw;
		break;

	case kSnappyCompression: {

	}
	}

	writeRawBlock(blockContents, type, handle);
	rep->compressedOutPut.clear();
	block->reset();
}

void TableBuilder::abandon() {
	assert(!rep->closed);
	rep->closed = true;
}

uint64_t TableBuilder::numEntries() const {
	return rep->numEntries;
}

uint64_t TableBuilder::fileSize() const {
	return rep->offset;
}

Status TableBuilder::status() const {
	return rep->status;
}
