#include "table-builder.h"
#include "block-builder.h"
#include "format.h"

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

    Rep(const Options &opt,PosixWritableFile *f)
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

TableBuilder::TableBuilder(const Options &options,PosixWritableFile *file)
:rep(new Rep(options,file))
{

}

TableBuilder::~TableBuilder()
{

}

void TableBuilder::add(const std::string_view &key,const std::string_view &value)
{

}