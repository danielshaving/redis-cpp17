#pragma once

#include "all.h"
#include "httpcontext.h"
#include "tcpclient.h"
#include "tcpconnection.h"

class HttpClient {
public:
    typedef std::function<void(const TcpConnectionPtr &, HttpResponse &, const TcpConnectionPtr &)> HttpCallBack;

    HttpClient(EventLoop *loop);

    void onConnection(const TcpConnectionPtr &conn);

    void onMessage(const TcpConnectionPtr &conn, Buffer *buffer);

    void postUrl(const char *ip, int16_t port, const std::string &url,
                 const std::string &body, const std::string &host,
                 const TcpConnectionPtr &conn, HttpCallBack &&callback);

    void getUrl(const char *ip, int16_t port, const std::string &url,
                const std::string &host, const TcpConnectionPtr &conn, HttpCallBack &&callback);

private:
    EventLoop *loop;
    std::unordered_map <int64_t, TcpClientPtr> tcpclients;
    std::unordered_map <int64_t, TcpConnectionPtr> tcpConns;
    std::unordered_map <int64_t, HttpCallBack> httpCallbacks;
    static int32_t const kHeart = 10;
    int64_t index;
};
