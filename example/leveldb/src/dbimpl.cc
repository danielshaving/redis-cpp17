#include "dbimpl.h"

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer 
{
	Status status;
	WriteBatch *batch;
	bool sync;
	bool done;
};

Status DBImpl::put(const WriteOptions&,const std::string_view &key,const std::string_view &value)
{

}

Status DBImpl::erase(const WriteOptions&,const std::string_view &key)
{

}

Status DBImpl::write(const WriteOptions &options,WriteBatch *updates)
{

}

Status DBImpl::get(const ReadOptions &options,const std::string_view &key,std::string *value)
{

}

Status DBImpl::open(const Options& options,const std::string& dbname)
{

}