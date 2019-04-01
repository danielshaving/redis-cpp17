#pragma once

#include "all.h"
#include "buffer.h"
#include "callback.h"
#include "channel.h"

class EventLoop;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    enum StateE {
        kDisconnected, kConnecting, kConnected, kDisconnecting
    };

    TcpConnection(EventLoop *loop, int32_t sockfd, const std::any &);

    ~TcpConnection();

    EventLoop *getLoop() { return loop; }

    int32_t getSockfd() { return sockfd; }

    void setState(StateE s);

    void setConnectionCallback(const ConnectionCallback &&cb) {
        connectionCallback = std::move(cb);
    }

    void setMessageCallback(const MessageCallback &&cb) {
        messageCallback = std::move(cb);
    }

    void setWriteCompleteCallback(const WriteCompleteCallback &&cb) {
        writeCompleteCallback = std::move(cb);
    }

    void setHighWaterMarkCallback(const HighWaterMarkCallback &&cb, size_t highWaterMark) {
        highWaterMarkCallback = std::move(cb);
        this->highWaterMark = highWaterMark;
    }

    void setCloseCallback(const CloseCallback &cb) {
        closeCallback = cb;
    }

    void sendInLoop(const void *message, size_t len);

    void sendInLoop(const std::string_view &message);

    void sendPipeInLoop(const void *message, size_t len);

    void sendPipeInLoop(const std::string_view &message);

    static void bindSendInLoop(TcpConnection *conn, const std::string_view &message);

    static void bindSendPipeInLoop(TcpConnection *conn, const std::string_view &message);

    void sendInLoopPipe();

    void sendPipe();

    void sendPipe(const std::string_view &message);

    void sendPipe(Buffer *message);

    void sendPipe(const void *message, int32_t len);

    void send(const void *message, int32_t len);

    void send(Buffer *message);

    void send(const std::string_view &message);

    bool disconnected() const { return state == kDisconnected; }

    bool connected() { return state == kConnected; }

    void forceCloseInLoop();

    void connectEstablished();

    void forceCloseWithDelay(double seconds);

    void forceCloseDelay();

    void connectDestroyed();

    void shutdown();

    void shutdownInLoop();

    void forceClose();

    void handleRead();

    void handleWrite();

    void handleClose();

    void handleError();

    void startReadInLoop();

    void stopReadInLoop();

    void startRead();

    void stopRead();

    std::any *getMutableContext() { return &context; }

    const std::any &getContext() const { return context; }

    void resetContext() { context.reset(); }

    void setContext(const std::any &context) { this->context = context; }

    std::any *getMutableContext1() { return &context1; }

    const std::any &getContext1() const { return context1; }

    void resetContext1() { context1.reset(); }

    void setContext1(const std::any &context1) { this->context1 = context1; }
	
	const std::any &getContext2() const { return context2; }

    void resetContext2() { context2.reset(); }

    void setContext2(const std::any &context1) { this->context2 = context2; }

    Buffer *outputBuffer() { return &writeBuffer; }

    Buffer *intputBuffer() { return &readBuffer; }

private:
    TcpConnection(const TcpConnection &);

    void operator=(const TcpConnection &);

    EventLoop *loop;
    int32_t sockfd;
    bool reading;

    Buffer readBuffer;
    Buffer writeBuffer;
    ConnectionCallback connectionCallback;
    MessageCallback messageCallback;
    WriteCompleteCallback writeCompleteCallback;
    HighWaterMarkCallback highWaterMarkCallback;
    CloseCallback closeCallback;

    size_t highWaterMark;
    StateE state;
    ChannelPtr channel;
    std::any context;
    std::any context1;
	std::any context2;
};
