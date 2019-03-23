#pragma once

#include "all.h"
#include "tcpconnection.h"
#include "object.h"
#include "sds.h"
#include "util.h"

class Redis;

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(Redis *redis, const TcpConnectionPtr &conn);

    ~Session();

    void clearCommand();

    void resetVlaue();

    void reset();

    void readCallback(const TcpConnectionPtr &conn, Buffer *buffer);

    int32_t processMultibulkBuffer(const TcpConnectionPtr &conn, Buffer *buffer);

    int32_t processInlineBuffer(const TcpConnectionPtr &conn, Buffer *buffer);

    int32_t processCommand(const TcpConnectionPtr &conn);

    void setAuth(bool enbaled);

private:
    Session(const Session &);

    void operator=(const Session &);

    Redis *redis;
    RedisObjectPtr cmd;
    std::deque <RedisObjectPtr> redisCommands;

    int32_t reqtype;
    int32_t multibulklen;
    int64_t bulklen;
    int32_t argc;
    size_t pos;

    Buffer slaveBuffer;
    Buffer pubsubBuffer;

    bool authEnabled;
    bool replyBuffer;
    bool fromMaster;
    bool fromSlave;
};

