#include "table.h"
#include "format.h"
#include "coding.h"
#include "option.h"

struct Table::Rep
{
	~Rep()
	{

	}

	Options options;
	Status status;
	std::shared_ptr<RandomAccessFile> file;
	uint64_t cacheId;
	BlockHandle metaindexHandle;  // Handle to metaindex_block: saved from footer
	std::shared_ptr<Block> indexBlock;
};

Table::~Table()
{

}

Status Table::open(const Options &options,
	const std::shared_ptr<RandomAccessFile> &file,
	uint64_t size,
	std::shared_ptr<Table> &table)
{
	if (size < Footer::kEncodedLength)
	{
		return Status::corruption("file is too short to be an sstable");
	}
	
	char footerSpace[Footer::kEncodedLength];
	std::string_view footerInput;
	Status s = file->read(size - Footer::kEncodedLength, Footer::kEncodedLength,
					&footerInput, footerSpace);
	if (!s.ok()) return s;
	
	Footer footer;
	s = footer.decodeFrom(&footerInput);
	if (!s.ok()) return s;

	// Read the index block
	BlockContents indexBlockContents;
	if (s.ok()) 
	{
		ReadOptions opt;
		if (options.paranoidChecks) 
		{
			opt.verifyChecksums = true;
		}
		s = readBlock(file, opt, footer.getIndexHandle(), &indexBlockContents);
	}
  
	if (s.ok()) 
	{
		// We've successfully read the footer and the index block: we're
		// ready to serve requests.
		std::shared_ptr<Block> indexBlock(new Block(indexBlockContents));
		std::shared_ptr<Rep> rep(new Table::Rep);
		rep->options = options;
		rep->file = file;
		rep->metaindexHandle = footer.getMetaindexHandle();
		rep->indexBlock = indexBlock;
		table = std::shared_ptr<Table>(new Table(rep));
	}
	return s;
}

std::shared_ptr<Iterator> Table::newIterator(const ReadOptions &options) const 
{
	std::shared_ptr<Iterator> indexIter = rep->indexBlock->newIterator(rep->options.comparator.get());
	return newTwoLevelIterator(indexIter,
		std::bind(blockReader, std::placeholders::_1, 
			std::placeholders::_2, std::placeholders::_3), shared_from_this(), options);
}

std::shared_ptr<Iterator> Table::blockReader(const std::any &arg,
		 const ReadOptions &options,
		 const std::string_view &indexValue)
{
	assert(arg.has_value());
	const std::shared_ptr<const Table> &table = std::any_cast<std::shared_ptr<const Table>>(arg);
	std::shared_ptr<Block> block = nullptr;
	BlockHandle handle;
	std::string_view input = indexValue;
	Status s = handle.decodeFrom(&input);
    if (s.ok()) 
	{
		BlockContents contents;
		s = readBlock(table->rep->file, options, handle, &contents);
		if (s.ok()) 
		{
			block = std::shared_ptr<Block>(new Block(contents));
		}
	}

	std::shared_ptr<Iterator> iter;
	if (block != nullptr)
	{
		iter = block->newIterator(table->rep->options.comparator.get());
		iter->registerCleanup(block);
	}
	else
	{
		//iter = std::shared_ptr<Iterator>(new Iterator(s));
	}
	return iter;
}
								 
Status Table::internalGet(
	const ReadOptions &options, 
	const std::string_view &key,
	const std::any &arg,
	std::function<void(const std::any &arg, 
	const std::string_view &k, const std::string_view &v)> &callback)
{
	Status s;
	std::shared_ptr<Iterator> iter = rep->indexBlock->newIterator(rep->options.comparator.get());
	iter->seek(key);
	if (iter->valid())
	{
		std::shared_ptr<Iterator> blockIter = blockReader(shared_from_this(), options, iter->value());
		blockIter->seek(key);
		if (blockIter->valid()) 
		{
			callback(arg, blockIter->key(), blockIter->value());
		}
		s = blockIter->status();
	}
	
	if (s.ok()) 
	{
		s = iter->status();
	}
	return s;
}

uint64_t Table::approximateOffsetOf(const std::string_view &key) const
{
	std::shared_ptr<Iterator> indexIter = rep->indexBlock->newIterator(rep->options.comparator.get());
	indexIter->seek(key);
	uint64_t result;
	if (indexIter->valid())
	{
		BlockHandle handle;
		std::string_view input = indexIter->value();
		Status s = handle.decodeFrom(&input);
		if (s.ok())
		{
			result = handle.getOffset();
		}
		else
		{
			// Strange: we can't decode the block handle in the index block.
			// We'll just return the offset of the metaindex block, which is
			// close to the whole file size for this case.
			result = rep->metaindexHandle.getOffset();
		}
	}
	else
	{
		result = rep->metaindexHandle.getOffset();
	}
	return result;
}

