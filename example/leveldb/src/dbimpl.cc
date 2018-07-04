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

Status DBImpl::put(const WriteOptions &opt,const std::string_view &key,const std::string_view &value)
{
    WriteBatch batch;
    batch.put();
    return write(opt,&batch);
}

Status DBImpl::erase(const WriteOptions &opt,const std::string_view &key)
{

}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::makeRoomForWrite(bool force)
{
    assert(!writers.empty());
    bool allowDelay = !force;
    Status s;
    while(true)
    {
        if (!force && mem->getMemoryUsage() <= opt.writeBufferSize)
        {
            break;
        }
        else if (imm != nullptr)
        {

        }
    }
}

Status DBImpl::write(const WriteOptions &opt,WriteBatch *updates)
{
    std::shared_ptr<Writer> w(new Writer());
    w->batch = updates;
    w->sync = opt.sync;
    w->done = false;

    writers.push_back(w);
    // May temporarily unlock and wait.
    Status status = makeRoomForWrite(updates == nullptr);
}

Status DBImpl::get(const ReadOptions &opt,const std::string_view &key,std::string *value)
{

}

Status DBImpl::open(const Options &opt,const std::string& dbname)
{

}
