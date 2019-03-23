#pragma once

#include "all.h"
#include "object.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/env.h"
#include "leveldb/cache.h"

class RedisDb;

class RedisHash {
public:
    RedisHash(RedisDb *redisdb);

    ~RedisHash();

    bool hsetCommand(const std::vector <RedisObjectPtr> &commands,
                     const TcpConnectionPtr &conn);

    bool hgetCommand(const std::vector <RedisObjectPtr> &commands,
                     const TcpConnectionPtr &conn);

    bool hgetallCommand(const std::vector <RedisObjectPtr> &commands,
                        const TcpConnectionPtr &conn);

    bool delCommand(const RedisObjectPtr &command,
                    const TcpConnectionPtr &conn);

    void print();

private:
    leveldb::DB *db;
    RedisDb *redisdb;
    std::string value;
    static const int kCacheSize = 10000000;
};
