#include "tablebuilder.h"
#include "crc32c.h"
#include "coding.h"
#include "blockbuilder.h"
#include "env.h"
#include "format.h"

struct TableBuilder::Rep {
	Options options;
	Options indexblockoptions;
	std::shared_ptr<WritableFile> file;
	uint64_t offset;
	Status status;
	BlockBuilder datablock;
	BlockBuilder indexblock;
	std::string lastkey;
	int64_t pendinghandle;
	bool closed;          // Either Finish() or Abandon() has been called.

	// We do not emit the index entry for a block until we have seen the
	// first key for the Next data block.  This allows us to use shorter
	// keys in the index block.  For example, consider a block boundary
	// between the keys "the quick brown fox" and "the who".  We can use
	// "the rep" as the key for the index block entry since it is >= all
	// entries in the first block and< all entries in subsequent
	// blocks.
	//
	// Invariant: rep->pending_index_entry is true only if data_block is empty.
	bool pendingindexentry;
	BlockHandle blockhandle;  // Handle to Add to index block
	std::string compressedoutput;

	Rep(const Options& opt, const std::shared_ptr<WritableFile>& f)
		: options(opt),
		indexblockoptions(opt),
		file(f),
		offset(0),
		datablock(&options),
		indexblock(&indexblockoptions),
		pendinghandle(0),
		closed(false),
		pendingindexentry(false) {
		indexblockoptions.blockrestartinterval = 1;
	}
};

TableBuilder::TableBuilder(const Options& options, const std::shared_ptr<WritableFile>& file)
	: rep(new Rep(options, file)) {

}

TableBuilder::~TableBuilder() {
	assert(rep->closed);  // Catch errors where caller forgot to call Finish()
}

void TableBuilder::Add(const std::string_view& key, const std::string_view& value) {
	assert(!rep->closed);
	if (!ok()) return;

	if (rep->pendinghandle > 0) {
		std::string_view lastkey(rep->lastkey.data(), rep->lastkey.size());
		assert(rep->options.comparator->Compare(key, lastkey) > 0);
	}

	if (rep->pendingindexentry) {
		assert(rep->datablock.empty());
		rep->options.comparator->FindShortestSeparator(&rep->lastkey, key);
		std::string handleencoding;
		rep->blockhandle.EncodeTo(&handleencoding);
		rep->indexblock.Add(rep->lastkey, std::string_view(handleencoding));
		rep->pendingindexentry = false;
	}

	rep->lastkey.assign(key.data(), key.size());
	rep->pendinghandle++;
	rep->datablock.Add(key, value);

	const size_t estimatedBlockSize = rep->datablock.CurrentSizeEstimate();
	if (estimatedBlockSize >= rep->options.blocksize) {
		flush();
	}
}

Status TableBuilder::Finish() {
	flush();
	assert(!rep->closed);
	rep->closed = true;

	BlockHandle metaindexBlockHandle, indexBlockHandle;
	// Write metaindex block
	if (ok()) {
		BlockBuilder metaIndexBlock(&rep->options);
		WriteBlock(&metaIndexBlock, &metaindexBlockHandle);
	}

	// Write index block
	if (ok()) {
		if (rep->pendingindexentry) {
			rep->options.comparator->FindShortSuccessor(&rep->lastkey);
			std::string handleEncoding;
			rep->blockhandle.EncodeTo(&handleEncoding);
			rep->indexblock.Add(rep->lastkey, std::string_view(handleEncoding));
			rep->pendingindexentry = false;
		}
		WriteBlock(&rep->indexblock, &indexBlockHandle);
	}

	// Write footer
	if (ok()) {
		Footer footer;
		footer.SetMetaindexHandle(metaindexBlockHandle);
		footer.SetIndexHandle(indexBlockHandle);
		std::string footerEncoding;
		footer.EncodeTo(&footerEncoding);
		rep->status = rep->file->append(footerEncoding);
		if (rep->status.ok()) {
			rep->offset += footerEncoding.size();
		}
	}
	return rep->status;
}

void TableBuilder::flush() {
	assert(!rep->closed);

	if (rep->datablock.empty()) return;
	assert(!rep->pendingindexentry);
	WriteBlock(&rep->datablock, &rep->blockhandle);

	rep->pendingindexentry = true;
	rep->status = rep->file->flush();
}

void TableBuilder::WriteRawBlock(const std::string_view& blockContents,
	CompressionType type, BlockHandle* handle) {
	handle->SetOffset(rep->offset);
	handle->SetSize(blockContents.size());
	rep->status = rep->file->append(blockContents);
	if (rep->status.ok()) {
		char trailer[kBlockTrailerSize];
		trailer[0] = type;
		uint32_t crc = crc32c::Value(blockContents.data(), blockContents.size());
		crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
		EncodeFixed32(trailer + 1, crc32c::Mask(crc));
		rep->status = rep->file->append(std::string_view(trailer, kBlockTrailerSize));
		if (rep->status.ok()) {
			rep->offset += blockContents.size() + kBlockTrailerSize;
		}
	}
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
	// File format Contains a sequence of blocks where each block has:
	//    block_data: uint8[n]
	//    type: uint8
	//    crc: uint32
	std::string_view raw = block->Finish();
	std::string_view blockcontents;
	CompressionType type = rep->options.compression;
	// TODO(postrelease): Support more compression options: zlib?
	switch (type) {
		case kNoCompression:
			blockcontents = raw;
			break;

		case kSnappyCompression: {
			std::string* compressed = &rep->compressedoutput;
			if (Snappy_Compress(raw.data(), raw.size(), compressed) &&
				compressed->size() < raw.size() - (raw.size() / 8u)) {
				blockcontents = *compressed;
			} else {
				// Snappy not supported, or compressed less than 12.5%, so just
				// store uncompressed form
				blockcontents = raw;
				type = kNoCompression;
			}
		}
	}

	WriteRawBlock(blockcontents, type, handle);
	rep->compressedoutput.clear();
	block->reset();
}

void TableBuilder::Abandon() {
	assert(!rep->closed);
	rep->closed = true;
}

uint64_t TableBuilder::pendinghandle() const {
	return rep->pendinghandle;
}

uint64_t TableBuilder::filesize() const {
	return rep->offset;
}

Status TableBuilder::status() const {
	return rep->status;
}
