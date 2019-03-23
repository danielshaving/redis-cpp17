#pragma once

#include "all.h"

namespace Socket {
    uint64_t hostToNetwork64(uint64_t host64);

    uint32_t hostToNetwork32(uint32_t host32);

    uint16_t hostToNetwork16(uint16_t host16);

    uint64_t networkToHost64(uint64_t net64);

    uint32_t networkToHost32(uint32_t net32);

    uint16_t networkToHost16(uint16_t net16);

    int32_t pipe(int32_t fildes[2]);

    ssize_t read(int32_t sockfd, void *buf, int32_t count);

    ssize_t readv(int32_t sockfd, IOV_TYPE *iov, int32_t iovcnt);

    ssize_t write(int32_t sockfd, const void *buf, int32_t count);

    void close(int32_t sockfd);

    struct sockaddr_in6 getPeerAddr(int32_t sockfd);

    struct sockaddr_in6 getLocalAddr(int32_t sockfd);

    void toIpPort(char *buf, size_t size, const struct sockaddr *addr);

    void toIp(char *buf, size_t size, const struct sockaddr *addr);

    void toPort(uint16_t *port, const struct sockaddr *addr);

    void fromIpPort(const char *ip, uint16_t port, struct sockaddr_in *addr);

    void fromIpPort(const char *ip, uint16_t port, struct sockaddr_in6 *addr);

    int32_t createSocket();

    int32_t createTcpSocket(const char *ip, int16_t port);

    int32_t getSocketError(int32_t sockfd);

    int32_t connect(int32_t sockfd, struct sockaddr *sin);

    int32_t connect(int32_t sockfd, const char *ip, int16_t port);

    bool connectWaitReady(int32_t fd, int32_t msec);

    bool isSelfConnect(int32_t sockfd);

    bool setkeepAlive(int32_t fd, int32_t idle);

    bool setSocketNonBlock(int32_t sockfd);

    bool setSocketBlock(int32_t sockfd);

    bool setTcpNoDelay(int32_t sockfd, bool on);

    bool setTimeOut(int32_t sockfd, const struct timeval tc);

    void setReuseAddr(int32_t sockfd, bool on);

    void setReusePort(int32_t sockfd, bool on);

    bool resolve(std::string_view hostname, struct sockaddr_in *out);

    bool resolve(std::string_view hostname, struct sockaddr_in6 *out);
};

