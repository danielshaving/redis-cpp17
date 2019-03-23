#pragma once

#include "all.h"

class Tunnel : public std::enable_shared_from_this<Tunnel> {
public:
    Tunnel(EventLoop *loop,
           const char *ip, uint16_t port,
           const TcpConnectionPtr &serverConn)
            : client(loop, ip, port, nullptr),
              serverConn(serverConn) {

    }

    ~Tunnel() {

    }

    void setup() {
        using std::placeholders::_1;
        using std::placeholders::_2;
        using std::placeholders::_3;

        client.setConnectionCallback(
                std::bind(&Tunnel::onClientConnection, shared_from_this(), _1));
        client.setMessageCallback(
                std::bind(&Tunnel::onClientMessage, shared_from_this(), _1, _2, _3));
        serverConn->setHighWaterMarkCallback(
                std::bind(&Tunnel::onHighWaterMarkWeak,
                          std::weak_ptr<Tunnel>(shared_from_this()), kServer, _1, _2),
                1024 * 1024);
    }

    void connect() {
        client.connect();
    }

    void disconnect() {
        client.disconnect();
    }

private:
    void teardown() {
        client.setConnectionCallback(defaultConnectionCallback);
        client.setMessageCallback(defaultMessageCallback);
        if (serverConn) {
            serverConn->setContext(std::any());
            serverConn->shutdown();
        }
        clientConn.reset();
    }

    void onClientConnection(const TcpConnectionPtr &conn) {
        using std::placeholders::_1;
        using std::placeholders::_2;

        LOG_DEBUG << (conn->connected() ? "UP" : "DOWN");
        if (conn->connected()) {
            conn->setTcpNoDelay(true);
            conn->setHighWaterMarkCallback(
                    std::bind(&Tunnel::onHighWaterMarkWeak,
                              std::weak_ptr<Tunnel>(shared_from_this()), kClient, _1, _2),
                    1024 * 1024);
            serverConn->setContext(conn);
            serverConn->startRead();
            clientConn = conn;
            if (serverConn->inputBuffer()->readableBytes() > 0) {
                conn->send(serverConn->inputBuffer());
            }
        } else {
            teardown();
        }
    }

    void onClientMessage(const TcpConnectionPtr &conn,
                         Buffer *buf,
                         TimeStamp) {
        LOG_DEBUG << " " << buf->readableBytes();
        if (serverConn) {
            serverConn->send(buf);
        } else {
            buf->retrieveAll();
            abort();
        }
    }

    enum ServerClient {
        kServer, kClient
    };

    void onHighWaterMark(ServerClient which,
                         const TcpConnectionPtr &conn,
                         size_t bytesToSent) {
        using std::placeholders::_1;

        LOG_INFO << (which == kServer ? "server" : "client") << " bytes " << bytesToSent;

        if (which == kServer) {
            if (serverConn->outputBuffer()->readableBytes() > 0) {
                clientConn->stopRead();
                serverConn->setWriteCompleteCallback(
                        std::bind(&Tunnel::onWriteCompleteWeak,
                                  std::weak_ptr<Tunnel>(shared_from_this()), kServer, _1));
            }
        } else {
            if (clientConn->outputBuffer()->readableBytes() > 0) {
                serverConn->stopRead();
                clientConn->setWriteCompleteCallback(
                        std::bind(&Tunnel::onWriteCompleteWeak,
                                  std::weak_ptr<Tunnel>(shared_from_this()), kClient, _1));
            }
        }
    }

    static void onHighWaterMarkWeak(const std::weak_ptr <Tunnel> &wkTunnel,
                                    ServerClient which,
                                    const TcpConnectionPtr &conn,
                                    size_t bytesToSent) {
        std::shared_ptr <Tunnel> tunnel = wkTunnel.lock();
        if (tunnel) {
            tunnel->onHighWaterMark(which, conn, bytesToSent);
        }
    }

    void onWriteComplete(ServerClient which, const TcpConnectionPtr &conn) {
        LOG_INFO << (which == kServer ? "server" : "client");
        if (which == kServer) {
            clientConn->startRead();
            serverConn->setWriteCompleteCallback(WriteCompleteCallback());
        } else {
            serverConn->startRead();
            clientConn->setWriteCompleteCallback(WriteCompleteCallback());
        }
    }

    static void onWriteCompleteWeak(const std::weak_ptr <Tunnel> &wkTunnel,
                                    ServerClient which,
                                    const TcpConnectionPtr &conn) {
        std::shared_ptr <Tunnel> tunnel = wkTunnel.lock();
        if (tunnel) {
            tunnel->onWriteComplete(which, conn);
        }
    }

private:
    TcpClient client;
    TcpConnectionPtr serverConn;
    TcpConnectionPtr clientConn;
};

typedef std::shared_ptr <Tunnel> TunnelPtr;
