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
	std::shared_ptr<PosixRandomAccessFile> file;
	uint64_t cacheId;
	const char *filterData;

	BlockHandle metaindexHandle;  // Handle to metaindex_block: saved from footer
	std::shared_ptr<Block> indexBlock;
};

Status Table::open(const Options &options,
					 PosixRandomAccessFile *file,
					 uint64_t fileSize,
					 Table **table)
{
	*table = nullptr;
	if (fileSize < Footer::kEncodedLength) 
	{
		return Status::corruption("file is too short to be an sstable");
	}	
}
