#include "table.h"
#include "format.h"
#include "coding.h"
#include "option.h"
#include "block.h"

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
		//rep->cacheId = (options.blockCache ? options.blockCache->newId() : 0);
		rep->filterData = nullptr;
		std::shared_ptr<Table> t (new Table(rep));
		table = t;
	}
	return s;
}

Status Table::internalGet(
	const ReadOptions &options, 
	const std::string_view &key,
	const std::any &arg,
	std::function<void(const std::any &arg, 
	const std::string_view &k, const std::string_view &v)> &callback)
{
	Status s;
}

