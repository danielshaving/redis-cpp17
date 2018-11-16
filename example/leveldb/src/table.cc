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
	std::shared_ptr<PosixMmapReadableFile> file;
	uint64_t cacheId;
	const char *filterData;

	BlockHandle metaindexHandle;  // Handle to metaindex_block: saved from footer
	std::shared_ptr<Block> indexBlock;
};

Table::~Table()
{

}

Status Table::open(const Options &options,
	std::shared_ptr<PosixMmapReadableFile> &file,
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
		s = readBlock(file.get(), opt, footer.getIndexHandle(), &indexBlockContents);
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
		rep->filterData = nullptr;
		std::shared_ptr<Table> t (new Table(rep));
		table = t;
	}
	return s;
}

std::shared_ptr<BlockIterator> Table::blockReader(const std::any &arg,
	 const ReadOptions &options,
	 const std::string_view &indexValue)
{
	Table *table = std::any_cast<Table*>(arg);
	std::shared_ptr<Block> block = nullptr;
	BlockHandle handle;
	std::string_view input = indexValue;
	Status s = handle.decodeFrom(&input);
    if (s.ok()) 
	{
		BlockContents contents;
		s = readBlock(table->rep->file.get(), options, handle, &contents);
		if (s.ok()) 
		{
			block = std::shared_ptr<new Block(contents)>();
		}
	}
}
								 
Status Table::internalGet(
	const ReadOptions &options, 
	const std::string_view &key,
	const std::any &arg,
	std::function<void(const std::any &arg, 
	const std::string_view &k, const std::string_view &v)> &callback)
{
	Status s;
	std::shared_ptr<BlockIterator> iter = rep->indexBlock->newIterator(rep->options.comparator);
	iter->seek(key);
	if (iter->valid())
	{
		std::shared_ptr<BlockIterator> blockIter = blockReader(this, options, iter->getValue());
	}
}

