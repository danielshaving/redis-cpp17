#pragma once
#include "all.h"

class Socket
{
public:
	Socket();
	~Socket();

	static uint64_t hostToNetwork64(uint64_t host64);
	static uint32_t hostToNetwork32(uint32_t host32);
	static uint16_t hostToNetwork16(uint16_t host16);
	static uint64_t networkToHost64(uint64_t net64);
	static uint32_t networkToHost32(uint32_t net32);
	static uint16_t networkToHost16(uint16_t net16);

	static int32_t pipe(int32_t fildes[2]);

	static ssize_t read(int32_t sockfd, void *buf, int32_t count);
	static ssize_t readv(int32_t sockfd, IOV_TYPE *iov, int32_t iovcnt);
	static ssize_t write(int32_t sockfd, const void* buf, int32_t count);

	static void close(int32_t sockfd);
	static struct sockaddr_in6 getPeerAddr(int32_t sockfd);
	static struct sockaddr_in6 getLocalAddr(int32_t sockfd);

	static void toIpPort(char *buf, size_t size, const struct sockaddr *addr);
	static void toIp(char *buf, size_t size, const struct sockaddr *addr);
	static void toPort(uint16_t *port, const struct sockaddr *addr);

	static void fromIpPort(const char *ip, uint16_t port, struct sockaddr_in *addr);
	static void fromIpPort(const char *ip, uint16_t port, struct sockaddr_in6 *addr);

	static int32_t createSocket();
	static int32_t createTcpSocket(const char *ip, int16_t port);
	static int32_t getSocketError(int32_t sockfd);

	static int32_t connect(int32_t sockfd, struct sockaddr *sin);
	static int32_t connect(int32_t sockfd, const char *ip, int16_t port);
	static bool connectWaitReady(int32_t fd, int32_t msec);

	static bool isSelfConnect(int32_t sockfd);
	static void setkeepAlive(int32_t fd, int32_t idle);
	static bool setSocketNonBlock(int32_t sockfd);
	static bool setSocketBlock(int32_t sockfd);
	static bool setTcpNoDelay(int32_t sockfd, bool on);
	static bool setTimeOut(int32_t sockfd, const struct timeval tc);
	static void setReuseAddr(int32_t sockfd, bool on);
	static void setReusePort(int32_t sockfd, bool on);

private:
	Socket(const Socket&);
	void operator=(const Socket&);
};

