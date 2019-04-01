#include "acceptor.h"
#include "log.h"

Acceptor::Acceptor(EventLoop *loop, const char *ip, int16_t port) : loop(loop),
                                                                    channel(loop, Socket::createTcpSocket(ip, port)),
                                                                    sockfd(channel.getfd()),
#ifndef _WIN64
                                                                    idleFd(::open("/dev/null", O_RDONLY | O_CLOEXEC)),
#endif
                                                                    listenning(false) {
#ifndef _WIN64
    assert(idleFd >= 0);
#endif
    channel.setReadCallback(std::bind(&Acceptor::handleRead, this));
}

Acceptor::~Acceptor() {
    channel.disableAll();
    channel.remove();
    Socket::close(sockfd);
}

void Acceptor::handleRead() {
    loop->assertInLoopThread();
	int32_t connfd = Socket::accept(sockfd);
    if (connfd >= 0) {
        if (newConnectionCallback) {
            Socket::setSocketNonBlock(connfd);
            newConnectionCallback(connfd);
        } else {
            Socket::close(sockfd);
        }
    } else {

    }
}

void Acceptor::listen() {
    loop->assertInLoopThread();
    listenning = true;
    channel.enableReading();
}
