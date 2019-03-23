#pragma once

#include "all.h"
#include "tcpconnection.h"
#include "sds.h"
#include "timer.h"

class RedisProxy;

class ProxySession : public std::enable_shared_from_this<ProxySession> {
public:
    ProxySession(RedisProxy *redis, const TcpConnectionPtr &conn);

    ~ProxySession();

    void proxyReadCallback(const TcpConnectionPtr &conn, Buffer *buffer);

    int32_t processMultibulkBuffer(const TcpConnectionPtr &conn, Buffer *buffer);

    int32_t processInlineBuffer(const TcpConnectionPtr &conn, Buffer *buffer);

    void reset();

private:
    ProxySession(const ProxySession &);

    void operator=(const ProxySession &);

    RedisProxy *redis;
    RedisObjectPtr command;
    std::vector<RedisObjectPtr> redisCommands;
    const char *buf;
    size_t len;
    int32_t pos;
    int32_t reqtype;
    int32_t multibulklen;
    int32_t bulklen;
    int32_t argc;
};
