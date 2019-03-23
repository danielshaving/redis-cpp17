#pragma once

#include "all.h"
#include "hiredis.h"
#include "tcpserver.h"
#include "redissession.h"
#include "redisset.h"
#include "redishash.h"

class RedisDb {
public:
    RedisDb(const char *ip, int16_t port);

    ~RedisDb();

    void dbConnCallback(const TcpConnectionPtr &conn);

    void run();

    void initRedisDb();

    void initRedisCommand();

    void initSetDb();

    leveldb::DB *getDb() { return db; }

    void print();

    void clearsize();

    void addsize();

    void subsize(int32_t count);

    void del(const RedisObjectPtr &command);

    bool flushdbCommand(const std::vector <RedisObjectPtr> &commands,
                        const TcpConnectionPtr &conn);

    bool delCommand(const std::vector <RedisObjectPtr> &commands,
                    const TcpConnectionPtr &conn);

    bool dbsizeCommand(const std::vector <RedisObjectPtr> &commands,
                       const TcpConnectionPtr &conn);

    bool slaveofCommand(const std::vector <RedisObjectPtr> &commands,
                        const TcpConnectionPtr &conn);

    bool getRedisCommand(const RedisObjectPtr &command);

    bool handleRedisCommand(const RedisObjectPtr &command,
                            const RedisSessionPtr &session,
                            const std::vector <RedisObjectPtr> &commands,
                            const TcpConnectionPtr &conn);

    void highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent);

    void writeCompleteCallback(const TcpConnectionPtr &conn);

private:
    EventLoop loop;
    TcpServer server;
    RedisSet setdb;
    RedisHash hashdb;
    leveldb::DB *db;
    std::string value;
    std::string dbsize;

    const char *ip;
    int16_t port;
    int16_t threadCount;

    static const int32_t kHeart = 10;
    static const int32_t kHighWaterBytes = 1024 * 1024 * 64;

    std::unordered_map <int32_t, RedisSessionPtr> sessions;
    typedef std::function<bool(const std::vector <RedisObjectPtr> &,
                               const TcpConnectionPtr &)> CommandFunc;
    std::unordered_map <RedisObjectPtr, CommandFunc, Hash, Equal> redisCommands;
};
