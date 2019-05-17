#include "transaction.h"

Transaction::Transaction(TransactionDB *db)
    :db(db) {

}

Transaction::~Transaction() {

}

Status Transaction::Put(const WriteOptions& options, const std::string_view& key, const std::string_view& value) {
    HashLock l(db->GetLockMgr(), key);
    return db->GetDB()->Put(options, key, value);
}

Status Transaction::Delete(const WriteOptions& options, const std::string_view& key) {
    HashLock l(db->GetLockMgr(), key);
    return db->GetDB()->Delete(options, key);
}

Status Transaction::Get(const ReadOptions& options, const std::string_view& key, std::string* value) {
    HashLock l(db->GetLockMgr(), key);
    return db->GetDB()->Get(options, key, value);
}

Status Transaction::Commit() {
    return db->Write(WriteOptions(), &writebatch);
}

Status Transaction::Rollback() {
    writebatch.clear();
}

TransactionDB::TransactionDB(const Options& options, const std::string& path):
    db(new DB(options, path)) {

}

Status TransactionDB::Open() {
    return db->Open();
}

TransactionDB::~TransactionDB() {

}

Status TransactionDB::Put(const WriteOptions& options, const std::string_view& key, const std::string_view& value) {
    WriteSharedHashLock l(&lockmgr, key);
    return db->Put(options, key, value);
}

Status TransactionDB::Delete(const WriteOptions& options, const std::string_view& key) {
    HashLock l(&lockmgr, key);
    return db->Delete(options, key);
}

Status TransactionDB::Write(const WriteOptions& options, WriteBatch* updates) {
    return db->Write(options, updates);
}

Status TransactionDB::Get(const ReadOptions& options, const std::string_view& key, std::string* value) {
    HashLock l(&lockmgr, key);
    return db->Get(options, key, value);
}

std::shared_ptr<Transaction> TransactionDB::BeginTrasaction() {
    std::shared_ptr<Transaction> tran(new Transaction(this));
    return tran;
 }