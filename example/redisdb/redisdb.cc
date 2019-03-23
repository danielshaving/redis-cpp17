#include "redisdb.h"
#include "socket.h"
#include "object.h"

RedisDb::RedisDb(const char *ip, int16_t port)
        : server(&loop, ip, port, nullptr),
          ip(ip),
          port(port),
          threadCount(0),
          setdb(this),
          hashdb(this),
          dbsize("dbsize") {
    LOG_INFO << "Db initialized";
    initRedisDb();
    initRedisCommand();
}

RedisDb::~RedisDb() {
    sessions.clear();
}

void RedisDb::initRedisDb() {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, "../db/redisdb", &db);
    assert(s.ok());

    server.setThreadNum(threadCount);
    server.setConnectionCallback(std::bind(&RedisDb::dbConnCallback,
                                           this, std::placeholders::_1));
    server.start();
}

void RedisDb::print() {
    std::cout << "redisdb print" << std::endl;
    {
        leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
        }

        assert(it->status().ok()); // Check for any errors found during the scan delete it;
        delete it;
    }
}


bool RedisDb::flushdbCommand(const std::vector <RedisObjectPtr> &commands,
                             const TcpConnectionPtr &conn) {
    if (commands.size() > 0) {
        return false;
    }

    return true;
}

bool RedisDb::delCommand(const std::vector <RedisObjectPtr> &commands,
                         const TcpConnectionPtr &conn) {
    if (commands.size() < 1) {
        return false;
    }
    return true;
}

bool RedisDb::slaveofCommand(const std::vector <RedisObjectPtr> &commands,
                             const TcpConnectionPtr &conn) {
    return true;
}

void RedisDb::initRedisCommand() {
    createSharedObjects();
    redisCommands[shared.set] = std::bind(&RedisSet::setCommand,
                                          &setdb, std::placeholders::_1, std::placeholders::_2);
    redisCommands[shared.get] = std::bind(&RedisSet::getCommand,
                                          &setdb, std::placeholders::_1, std::placeholders::_2);
    redisCommands[shared.hset] = std::bind(&RedisHash::hsetCommand,
                                           &hashdb, std::placeholders::_1, std::placeholders::_2);
    redisCommands[shared.hget] = std::bind(&RedisHash::hgetCommand,
                                           &hashdb, std::placeholders::_1, std::placeholders::_2);
    redisCommands[shared.hgetall] = std::bind(&RedisHash::hgetallCommand,
                                              &hashdb, std::placeholders::_1, std::placeholders::_2);
    redisCommands[shared.slaveof] = std::bind(&RedisDb::slaveofCommand,
                                              this, std::placeholders::_1, std::placeholders::_2);
    redisCommands[shared.del] = std::bind(&RedisDb::delCommand,
                                          this, std::placeholders::_1, std::placeholders::_2);
}

bool RedisDb::handleRedisCommand(const RedisObjectPtr &command,
                                 const RedisSessionPtr &session,
                                 const std::vector <RedisObjectPtr> &commands,
                                 const TcpConnectionPtr &conn) {
    auto it = redisCommands.find(command);
    if (it != redisCommands.end()) {
        if (!it->second(commands, conn)) {
            return false;
        }
        return true;
    }
    return false;
}

void RedisDb::run() {
    LOG_INFO << "Ready to accept connections";
    loop.run();
}

void RedisDb::highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent) {
    LOG_INFO << " bytes " << bytesToSent;
    conn->getLoop()->assertInLoopThread();
    if (conn->outputBuffer()->readableBytes() > 0) {
        conn->stopRead();
        conn->setWriteCompleteCallback(
                std::bind(&RedisDb::writeCompleteCallback, this, std::placeholders::_1));
    }
}

void RedisDb::writeCompleteCallback(const TcpConnectionPtr &conn) {
    conn->startRead();
    conn->setWriteCompleteCallback(WriteCompleteCallback());
}

void RedisDb::dbConnCallback(const TcpConnectionPtr &conn) {
    if (conn->connected()) {
        Socket::setkeepAlive(conn->getSockfd(), kHeart);
        conn->setHighWaterMarkCallback(
                std::bind(&RedisDb::highWaterCallback,
                          this, std::placeholders::_1, std::placeholders::_2),
                kHighWaterBytes);

        char buf[64] = "";
        uint16_t port = 0;
        auto addr = Socket::getPeerAddr(conn->getSockfd());
        Socket::toIp(buf, sizeof(buf), (const struct sockaddr *) &addr);
        Socket::toPort(&port, (const struct sockaddr *) &addr);
        RedisSessionPtr session(new RedisSession(this, conn));

        auto it = sessions.find(conn->getSockfd());
        assert(it == sessions.end());
        sessions[conn->getSockfd()] = session;
        //LOG_INFO << "Client connect success " << buf << " " << port << " " << conn->getSockfd();
    } else {
        auto it = sessions.find(conn->getSockfd());
        assert(it != sessions.end());
        sessions.erase(it);
        //LOG_INFO << "Client diconnect " << conn->getSockfd();
    }
}



