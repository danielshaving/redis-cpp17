#include "redisset.h"
#include "redisdb.h"

RedisSet::RedisSet(RedisDb *redisdb)
        : redisdb(redisdb),
          dbsize("dbsize") {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, "../db/setdb", &db);
    assert(s.ok());
}

RedisSet::~RedisSet() {
    delete db;
}

bool RedisSet::setCommand(const std::vector <RedisObjectPtr> &commands,
                          const TcpConnectionPtr &conn) {
    if (commands.size() != 2) {
        return false;
    }

    leveldb::Status s = db->Put(leveldb::WriteOptions(), leveldb::Slice(commands[0]->ptr,
                                                                        sdslen(commands[0]->ptr)),
                                leveldb::Slice(commands[1]->ptr, sdslen(commands[1]->ptr)));
    assert(s.ok());

    addReply(conn->outputBuffer(), shared.ok);
    return true;
}

bool RedisSet::getCommand(const std::vector <RedisObjectPtr> &commands,
                          const TcpConnectionPtr &conn) {
    if (commands.size() != 1) {
        return false;
    }

    value.clear();
    leveldb::Status s = db->Get(leveldb::ReadOptions(),
                                leveldb::Slice(commands[0]->ptr, sdslen(commands[0]->ptr)), &value);
    if (s.ok()) {
        RedisObjectPtr obj = createStringObject(value.data(), value.size());
        addReplyBulk(conn->outputBuffer(), obj);
    } else {
        addReply(conn->outputBuffer(), shared.nullbulk);
    }
    return true;
}

bool RedisSet::delCommand(const RedisObjectPtr &command,
                          const TcpConnectionPtr &conn) {
    value.clear();
    leveldb::Status s = db->Get(leveldb::ReadOptions(),
                                leveldb::Slice(command->ptr, sdslen(command->ptr)), &value);
    assert(s.ok());

    s = db->Delete(leveldb::WriteOptions(),
                   leveldb::Slice(command->ptr, sdslen(command->ptr)));
    assert(s.ok());
    return true;
}