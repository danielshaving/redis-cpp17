#include "table.h"
#include "format.h"
#include "coding.h"
#include "option.h"
#include "cache.h"

struct Table::Rep {
	~Rep() {

	}

	Options options;
	Status status;
	std::shared_ptr<RandomAccessFile> file;
	uint64_t cacheId;
	BlockHandle metaindexHandle;  // Handle to metaindex_block: saved from footer
	std::shared_ptr<Block> indexBlock;
};

Table::~Table() {

}

Status Table::Open(const Options& options,
	const std::shared_ptr<RandomAccessFile>& file,
	uint64_t size,
	std::shared_ptr<Table>& table) {
	if (size < Footer::kEncodedLength) {
		return Status::Corruption("file is too short to be an sstable");
	}

	char footerSpace[Footer::kEncodedLength];
	std::string_view footerInput;
	Status s = file->read(size - Footer::kEncodedLength, Footer::kEncodedLength,
		&footerInput, footerSpace);
	if (!s.ok()) return s;

	Footer footer;
	s = footer.DecodeFrom(&footerInput);
	if (!s.ok()) return s;

	// Read the index block
	BlockContents indexBlockContents;
	if (s.ok()) {
		ReadOptions opt;
		if (options.paranoidchecks) {
			opt.verifyChecksums = true;
		}
		s = ReadBlock(file, opt, footer.GetIndexHandle(), &indexBlockContents);
	}

	if (s.ok()) {
		// We've successfully read the footer and the index block: we're
		// ready to serve requests.
		std::shared_ptr<Block> indexBlock(new Block(indexBlockContents));
		std::shared_ptr<Rep> rep(new Table::Rep);
		rep->options = options;
		rep->file = file;
		rep->metaindexHandle = footer.GetMetaindexHandle();
		rep->indexBlock = indexBlock;
		table = std::shared_ptr<Table>(new Table(rep));
	}
	return s;
}

std::shared_ptr<Iterator> Table::NewIterator(const ReadOptions& options) {
	std::shared_ptr<Iterator> indexIter = rep->indexBlock->NewIterator(rep->options.comparator);
	return NewTwoLevelIterator(indexIter, options, std::bind(&Table::BlockReader,
		shared_from_this(), std::placeholders::_1, std::placeholders::_2));
}

static void DeleteBlock(const std::any &arg) {

}

static void ReleaseBlock(const std::any& arg1, const std::any& arg2) {
	std::shared_ptr<ShardedLRUCache> cache = std::any_cast<std::shared_ptr<ShardedLRUCache>>(arg1);
	std::shared_ptr<LRUHandle> handle = std::any_cast<std::shared_ptr<LRUHandle>>(arg2);
	cache->Release(handle);
}

std::shared_ptr<Iterator> Table::BlockReader(const ReadOptions& options, const std::string_view& indexValue) {
	auto blockcache = rep->options.blockcache;

	std::shared_ptr<Block> block;
	BlockHandle handle;
	std::string_view input = indexValue;
	Status s = handle.DecodeFrom(&input);
	std::shared_ptr<LRUHandle> cachehandle;

	if (s.ok()) {
		BlockContents contents;
		if (blockcache != nullptr) {
			char cachekeybuffer[8];
			EncodeFixed64(cachekeybuffer, handle.GetOffset());
			std::string_view key(cachekeybuffer, sizeof(cachekeybuffer));
			cachehandle = blockcache->Lookup(key);
			if (cachehandle != nullptr) {
				block = std::any_cast<std::shared_ptr<Block>>(cachehandle->value);
			}
			else {
				s = ReadBlock(rep->file, options, handle, &contents);
				if (s.ok()) {
					block.reset(new Block(contents));
					if (contents.cachable && options.fillcache) {
						cachehandle = blockcache->Insert(key, block, block->GetSize(), nullptr);
					}
				}
			}
		}
		else {
			s = ReadBlock(rep->file, options, handle, &contents);
			if (s.ok()) {
				block.reset(new Block(contents));
			}
		}
	}

	std::shared_ptr<Iterator> iter;
	if (block != nullptr) {
		iter = block->NewIterator(rep->options.comparator);
		if (cachehandle == nullptr) {
			iter->RegisterCleanup(std::bind(DeleteBlock, block));
		}
		else {
			iter->RegisterCleanup(std::bind(ReleaseBlock, blockcache, cachehandle));
		}
	}
	else {
		iter = NewErrorIterator(s);
	}
	return iter;
}

Status Table::InternalGet(
	const ReadOptions& options,
	const std::string_view& key,
	const std::any& arg,
	std::function<void(const std::any& arg,
		const std::string_view& k, const std::string_view& v)>& callback) {
	Status s;
	std::shared_ptr<Iterator> iter = rep->indexBlock->NewIterator(rep->options.comparator);
	iter->Seek(key);
	if (iter->Valid()) {
		std::shared_ptr<Iterator> blockIter = BlockReader(options, iter->value());
		blockIter->Seek(key);
		if (blockIter->Valid()) {
			callback(arg, blockIter->key(), blockIter->value());
		}
		s = blockIter->status();
}

	if (s.ok()) {
		s = iter->status();
	}
	return s;
}

uint64_t Table::ApproximateOffsetOf(const std::string_view& key) const {
	std::shared_ptr<Iterator> indexIter = rep->indexBlock->NewIterator(rep->options.comparator);
	indexIter->Seek(key);
	uint64_t result;
	if (indexIter->Valid()) {
		BlockHandle handle;
		std::string_view input = indexIter->value();
		Status s = handle.DecodeFrom(&input);
		if (s.ok()) {
			result = handle.GetOffset();
		}
		else {
			// Strange: we can't decode the block handle in the index block.
			// We'll just return the offset of the metaindex block, which is
			// close to the whole file size for this case.
			result = rep->metaindexHandle.GetOffset();
		}
	}
	else {
		result = rep->metaindexHandle.GetOffset();
	}
	return result;
}

