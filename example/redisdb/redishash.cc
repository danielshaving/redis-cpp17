#include "redishash.h"
#include "redisdb.h"
#include "zmalloc.h"

RedisHash::RedisHash(RedisDb *redisdb)
        : redisdb(redisdb) {
    leveldb::Options options;
    options.create_if_missing = true;
    //options.block_cache = leveldb::NewLRUCache(kCacheSize);
    //options.write_buffer_size = kCacheSize;
    //options.block_size = 4096 * 64;
    leveldb::Status s = leveldb::DB::Open(options, "../db/hashdb", &db);
    assert(s.ok());
}

RedisHash::~RedisHash() {
    delete db;
}

bool RedisHash::hsetCommand(const std::vector <RedisObjectPtr> &commands,
                            const TcpConnectionPtr &conn) {
    if (commands.size() != 3) {
        return false;
    }

    int64_t version = leveldb::Env::Default()->NowMicros();
    leveldb::WriteBatch batch;
    bool update = false;

    value.clear();
    leveldb::Status s = db->Get(leveldb::ReadOptions(),
                                leveldb::Slice(commands[0]->ptr, sdslen(commands[0]->ptr)), &value);
    if (s.ok()) {
        int32_t len = 0;
        while (len < value.size()) {
            int64_t v;
            memcpy(&v, value.c_str() + len, 8);
            len += 8;

            int32_t result;
            memcpy(&result, value.c_str() + len, 4);
            len += 4;

            if (result == sdslen(commands[1]->ptr) &&
                memcmp(commands[1]->ptr, value.c_str() + len, result) == 0) {
                update = true;
                version = v;
                break;
            }
            len += result;
        }

        if (len == value.size()) {
            {
                char buf[8];
                memcpy(buf, &version, 8);
                value.append(buf, 8);
            }

            {
                char buf[4];
                int32_t result = sdslen(commands[1]->ptr);
                memcpy(buf, &result, 4);
                value.append(buf, 4);
                value.append(commands[1]->ptr, sdslen(commands[1]->ptr));
            }

            batch.Put(leveldb::Slice(commands[0]->ptr,
                                     sdslen(commands[0]->ptr)), leveldb::Slice(value.c_str(), value.size()));
            assert(s.ok());
        }
    } else {
        value.clear();
        {
            char buf[8];
            memcpy(buf, &version, 8);
            value.append(buf, 8);
        }

        {
            char buf[4];
            int32_t result = sdslen(commands[1]->ptr);
            memcpy(buf, &result, 4);
            value.append(buf, 4);
            value.append(commands[1]->ptr, sdslen(commands[1]->ptr));
        }

        batch.Put(leveldb::Slice(commands[0]->ptr,
                                 sdslen(commands[0]->ptr)), leveldb::Slice(value.c_str(), value.size()));
    }

    int32_t keyLen = sdslen(commands[0]->ptr) + sdslen(commands[1]->ptr) + 8;
    char key[keyLen];
    memcpy(key, &version, 8);
    memcpy(key + 8, commands[0]->ptr, sdslen(commands[0]->ptr));
    memcpy(key + sdslen(commands[0]->ptr) + 8, commands[1]->ptr, sdslen(commands[1]->ptr));

    batch.Put(leveldb::Slice(key, keyLen),
              leveldb::Slice(commands[2]->ptr, sdslen(commands[2]->ptr)));
    s = db->Write(leveldb::WriteOptions(), &batch);
    assert(s.ok());

    addReply(conn->outputBuffer(), update ? shared.czero : shared.cone);
    return true;
}

bool RedisHash::hgetCommand(const std::vector <RedisObjectPtr> &commands,
                            const TcpConnectionPtr &conn) {
    if (commands.size() != 2) {
        return false;
    }

    int64_t version = 0;
    value.clear();
    leveldb::Status s = db->Get(leveldb::ReadOptions(),
                                leveldb::Slice(commands[0]->ptr, sdslen(commands[0]->ptr)), &value);
    if (s.ok()) {
        int32_t len = 0;
        while (len < value.size()) {
            int64_t v;
            memcpy(&v, value.c_str() + len, 8);
            len += 8;

            int32_t result;
            memcpy(&result, value.c_str() + len, 4);
            len += 4;

            if (result == sdslen(commands[1]->ptr) &&
                memcmp(commands[1]->ptr, value.c_str() + len, result) == 0) {
                version = v;
                break;
            }
            len += result;
        }

        assert(len != value.size());

        int32_t keyLen = sdslen(commands[0]->ptr) + sdslen(commands[1]->ptr) + 8;
        char key[keyLen];
        memcpy(key, &version, 8);
        memcpy(key + 8, commands[0]->ptr, sdslen(commands[0]->ptr));
        memcpy(key + sdslen(commands[0]->ptr) + 8, commands[1]->ptr, sdslen(commands[1]->ptr));

        value.clear();
        leveldb::Status s = db->Get(leveldb::ReadOptions(), leveldb::Slice(key, keyLen), &value);
        if (s.ok()) {
            RedisObjectPtr obj = createStringObject(value.data(), value.size());
            addReplyBulk(conn->outputBuffer(), obj);
        } else {
            addReply(conn->outputBuffer(), shared.nullbulk);
        }
    } else {
        addReply(conn->outputBuffer(), shared.nullbulk);
    }
    return true;
}

bool RedisHash::hgetallCommand(const std::vector <RedisObjectPtr> &commands,
                               const TcpConnectionPtr &conn) {
    return true;
}

bool RedisHash::delCommand(const RedisObjectPtr &command,
                           const TcpConnectionPtr &conn) {
    value.clear();
    leveldb::Status s = db->Get(leveldb::ReadOptions(),
                                leveldb::Slice(command->ptr, sdslen(command->ptr)), &value);
    assert(s.ok());

    leveldb::WriteBatch batch;
    int32_t len = 0;
    while (len < value.size()) {
        int32_t result;
        memcpy(&result, value.c_str() + len, sizeof(result));
        len += sizeof(int32_t);

        char data[result];
        memcpy(data, value.c_str() + len, result);
        int32_t keyLen = result + sdslen(command->ptr);
        char key[keyLen];
        memcpy(key, command->ptr, sdslen(command->ptr));
        memcpy(key + sdslen(command->ptr), data, result);
        batch.Delete(leveldb::Slice(key, keyLen));
        len += result;
    }

    s = db->Write(leveldb::WriteOptions(), &batch);
    assert(s.ok());
    return true;
}

void RedisHash::print() {

}
