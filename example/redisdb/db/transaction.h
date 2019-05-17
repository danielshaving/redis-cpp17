#pragma once
#include <atomic>
#include <map>
#include <set>
#include <memory>
#include <string>
#include "db.h"
#include "redis.h"
#include "lockmgr.h"

class TransactionDB;

class Transaction {
public:
    Transaction(TransactionDB *db);
    ~Transaction();

    Status Put(const WriteOptions& options, const std::string_view& key, const std::string_view& value);

	Status Delete(const WriteOptions& options, const std::string_view& key);

	Status Get(const ReadOptions& options, const std::string_view& key, std::string* value);

    Status Commit();

    Status Rollback();
private:
    TransactionDB *db;
    WriteBatch writebatch;
    std::shared_ptr<ReadSharedHashLock> readlock;
    std::shared_ptr<WriteSharedHashLock> writelock;
};

class TransactionDB {
public:
    TransactionDB(const Options& options, const std::string& path);
    ~TransactionDB();

    Status Open();

	Status Put(const WriteOptions& options, const std::string_view& key, const std::string_view& value);

	Status Delete(const WriteOptions& options, const std::string_view& key);

	Status Write(const WriteOptions& options, WriteBatch* updates);

	Status Get(const ReadOptions& options, const std::string_view& key, std::string* value);

    std::shared_ptr<Transaction> BeginTrasaction();

    LockMgr *GetLockMgr() {
        return &lockmgr;
    }

    std::shared_ptr<DB> GetDB() {
        return db;
    }
private:
    std::shared_ptr<DB> db;
    LockMgr lockmgr;
};