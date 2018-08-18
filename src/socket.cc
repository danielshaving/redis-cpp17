#include "socket.h"
#include "log.h"

Socket::Socket()
{

}

Socket::~Socket()
{

}

ssize_t Socket::readv(int32_t sockfd, IOV_TYPE *iov, int32_t iovcnt)
{
#ifdef _WIN32
	DWORD bytesRead;
	DWORD flags = 0;
	if (::WSARecv(sockfd, iov, iovcnt, &bytesRead, &flags, nullptr, nullptr))
	{
		if (GetLastError() == WSAECONNABORTED)
			return  0;
		else
			return -1;
	}
	else
	{
		return bytesRead;
	}
#else
	return ::readv(sockfd, iov, iovcnt);
#endif
}

ssize_t Socket::read(int32_t sockfd, void *buf, int32_t count)
{
	return ::recv(sockfd, static_cast<char*>(buf), count, 0);
}

ssize_t Socket::write(int32_t sockfd, const void* buf, int32_t count)
{
	return ::send(sockfd, static_cast<const char*>(buf), count, 0);
}

int32_t Socket::pipe(int32_t fildes[2])
{
	int32_t tcp1 = -1, tcp2 = 1;
	sockaddr_in name;
	memset(&name, 0, sizeof(name));
	name.sin_family = AF_INET;
	name.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	socklen_t namelen = sizeof(name);
	int32_t tcp = createSocket();

	if (tcp == -1)
	{
		goto clean;
	}

	if (::bind(tcp, (sockaddr*)&name, namelen) == -1)
	{
		goto clean;
	}

	if (::listen(tcp, 5) == -1)
	{
		goto clean;
	}

	if (::getsockname(tcp, (sockaddr*)&name, &namelen) == -1)
	{
		goto clean;
	}

	tcp1 = createSocket();
	if (tcp1 == -1)
	{
		goto clean;
	}

	if (-1 == connect(tcp1, (sockaddr*)&name))
	{
		goto clean;
	}

	tcp2 = accept(tcp, (sockaddr*)&name, &namelen);
	if (tcp2 == -1)
	{
		goto clean;
	}

	close(tcp);

	fildes[0] = tcp1;
	fildes[1] = tcp2;
	return 0;
clean:
	if (tcp != -1)
	{
		close(tcp);
	}

	if (tcp2 != -1)
	{
		close(tcp2);
	}

	if (tcp1 != -1)
	{
		close(tcp1);
	}
	return -1;
}

uint64_t Socket::hostToNetwork64(uint64_t host64)
{
#ifdef _WIN32
	uint64_t ret = 0;
	uint32_t high, low;

	low = host64 & 0xFFFFFFFF;
	high = (host64 >> 32) & 0xFFFFFFFF;
	low = htonl(low);
	high = htonl(high);
	ret = low;
	ret <<= 32;
	ret |= high;
	return ret;
#else
	return htobe64(host64);
#endif
}

uint32_t Socket::hostToNetwork32(uint32_t host32)
{
#ifdef _WIN32
	return htonl(host32);
#else
	return htobe32(host32);
#endif
}

uint16_t Socket::hostToNetwork16(uint16_t host16)
{
#ifdef _WIN32
	return htons(host16);
#else
	return htobe16(host16);
#endif
}

uint64_t Socket::networkToHost64(uint64_t net64)
{
#ifdef _WIN32
	uint64_t ret = 0;
	uint32_t high, low;

	low = net64 & 0xFFFFFFFF;
	high = (net64 >> 32) & 0xFFFFFFFF;
	low = ntohl(low);
	high = ntohl(high);

	ret = low;
	ret <<= 32;
	ret |= high;
	return ret;
#else
	return be64toh(net64);
#endif
}

uint32_t Socket::networkToHost32(uint32_t net32)
{
#ifdef _WIN32
	return ntohl(net32);
#else
	return be32toh(net32);
#endif 
}

uint16_t Socket::networkToHost16(uint16_t net16)
{
#ifdef _WIN32
	return ntohs(net16);
#else
	return be16toh(net16);
#endif
}

void Socket::close(int32_t sockfd)
{
#ifdef _WIN32
	::closesocket(sockfd);
#else
	::close(sockfd);
#endif
}
struct sockaddr_in6 Socket::getLocalAddr(int32_t sockfd)
{
	struct sockaddr_in6 localaddr;
	memset(&localaddr, sizeof localaddr, 0);
	socklen_t addrlen = static_cast<socklen_t>(sizeof localaddr);
	if (::getsockname(sockfd, (struct sockaddr*)&localaddr, &addrlen) < 0)
	{
		LOG_WARN << "Socket::getLocalAddr";
	}
	return localaddr;
}

struct sockaddr_in6 Socket::getPeerAddr(int32_t sockfd)
{
	struct sockaddr_in6 peeraddr;
	memset(&peeraddr, sizeof peeraddr, 0);
	socklen_t addrlen = static_cast<socklen_t>(sizeof peeraddr);
	if (::getpeername(sockfd, (struct sockaddr*)&peeraddr, &addrlen) < 0)
	{
		LOG_WARN << "Socket::getPeerAddr";
	}
	return peeraddr;
}

bool Socket::isSelfConnect(int32_t sockfd)
{
	struct sockaddr_in6 localaddr = getLocalAddr(sockfd);
	struct sockaddr_in6 peeraddr = getPeerAddr(sockfd);
	if (localaddr.sin6_family == AF_INET)
	{
		const struct sockaddr_in *laddr4 = reinterpret_cast<struct sockaddr_in*>(&localaddr);
		const struct sockaddr_in *raddr4 = reinterpret_cast<struct sockaddr_in*>(&peeraddr);
		return laddr4->sin_port == raddr4->sin_port
			&& laddr4->sin_addr.s_addr == raddr4->sin_addr.s_addr;
	}
	else if (localaddr.sin6_family == AF_INET6)
	{
		return localaddr.sin6_port == peeraddr.sin6_port
			&& memcmp(&localaddr.sin6_addr, &peeraddr.sin6_addr, sizeof localaddr.sin6_addr) == 0;
	}
	else
	{
		return false;
	}
}

int32_t Socket::getSocketError(int32_t sockfd)
{
	int32_t optval;
	socklen_t optlen = static_cast<socklen_t>(sizeof optval);

	if (::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, (char*)&optval, &optlen) < 0)
	{
		return errno;
	}
	else
	{
		return optval;
	}
}

void Socket::toIpPort(char *buf, size_t size, const struct sockaddr *addr)
{
	toIp(buf, size, addr);
	size_t end = ::strlen(buf);
	const struct sockaddr_in *addr4 = (const struct sockaddr_in*)(addr);
#ifdef _WIN32
	uint16_t port = networkToHost16(addr4->sin_port);
#else
	uint16_t port = ntohs(addr4->sin_port);
#endif
	assert(size > end);
	snprintf(buf + end, size - end, ":%u", port);
}

void Socket::toPort(uint16_t *port, const struct sockaddr *addr)
{
	const struct sockaddr_in *addr4 = (const struct sockaddr_in*)(addr);
#ifdef __linux__
	*port = networkToHost16(addr4->sin_port);
#endif
#ifdef __APPLE__
	*port = ntohs(addr4->sin_port);
#endif
}

void Socket::toIp(char *buf, size_t size, const struct sockaddr *addr)
{
	if (addr->sa_family == AF_INET)
	{
		assert(size >= INET_ADDRSTRLEN);
		const struct sockaddr_in *addr4 = (const struct sockaddr_in*)(addr);
		::inet_ntop(AF_INET, &addr4->sin_addr, buf, static_cast<socklen_t>(size));
	}
	else if (addr->sa_family == AF_INET6)
	{
		assert(size >= INET6_ADDRSTRLEN);
		const struct sockaddr_in6 *addr6 = (const struct sockaddr_in6*)(addr);
		::inet_ntop(AF_INET6, &addr6->sin6_addr, buf, static_cast<socklen_t>(size));
	}
}

void Socket::fromIpPort(const char *ip, uint16_t port, struct sockaddr_in *addr)
{
	addr->sin_family = AF_INET;
#ifdef __linux__
	addr->sin_port = hostToNetwork16(port);
#endif
#ifdef __APPLE__
	addr->sin_port = htons(port);
#endif
	if (::inet_pton(AF_INET, ip, &addr->sin_addr) <= 0)
	{
		LOG_WARN << "Socket::fromIpPort";
	}
}

void Socket::fromIpPort(const char *ip, uint16_t port, struct sockaddr_in6 *addr)
{
	addr->sin6_family = AF_INET6;
#ifdef __linux__
	addr->sin6_port = hostToNetwork16(port);
#endif
#ifdef __APPLE__
	addr->sin6_port = htons(port);
#endif
	if (::inet_pton(AF_INET6, ip, &addr->sin6_addr) <= 0)
	{
		LOG_WARN << "Socket::fromIpPort";
	}
}

int32_t Socket::createSocket()
{
#ifdef _WIN32
	return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#endif

#ifdef __linux__
	return ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
#endif

#ifdef __APPLE__
	return ::socket(AF_INET, SOCK_STREAM , 0);
	//return ::socket(AF_UNIX, SOCK_STREAM, 0);
#endif
}

bool Socket::connectWaitReady(int32_t fd, int32_t msec)
{
#ifdef _WIN32
#else
	struct pollfd wfd[1];
	wfd[0].fd = fd;
	wfd[0].events = POLLOUT;

	if (errno == EINPROGRESS)
	{
		int32_t res;
		if ((res = ::poll(wfd, 1, msec)) == -1)
		{
			return false;
		}
		else if (res == 0)
		{
			errno = ETIMEDOUT;
			return false;
		}
	}
#endif
	return true;
}

int32_t Socket::connect(int32_t sockfd, const char *ip, int16_t port)
{
	struct sockaddr_in sin;
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = inet_addr(ip);
	return connect(sockfd, (struct sockaddr *)&sin);
}

int32_t Socket::connect(int32_t sockfd, struct sockaddr *sin)
{
	return ::connect(sockfd, sin, sizeof(*sin));
}

bool Socket::setTimeOut(int32_t sockfd, const struct timeval tv)
{
	if (::setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char*)&tv, sizeof(tv)) == -1)
	{
		LOG_WARN << "setsockopt(SO_RCVTIMEO)";
		return false;
	}

	if (::setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, (char*)&tv, sizeof(tv)) == -1)
	{
		LOG_WARN << "setsockopt(SO_SNDTIMEO)";
		return false;
	}
	return true;
}

void Socket::setkeepAlive(int32_t fd, int32_t idle)
{
#ifdef __linux__
	int32_t keepalive = 1;
	int32_t keepidle = idle;
	int32_t keepintvl = 2;
	int32_t keepcnt = 3;
	int32_t err = 0;

	if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive, sizeof(keepalive)) < 0)
	{
		LOG_DEBUG << "SOL_SOCKET";
	}

	if (::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, (char *)&keepidle, sizeof(keepidle)) < 0)
	{
		LOG_DEBUG << "TCP_KEEPIDLE";
	}

	if (::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, (char *)&keepintvl, sizeof(keepintvl)) < 0)
	{
		LOG_DEBUG << "TCP_KEEPINTVL";
	}

	if (::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, (char *)&keepcnt, sizeof(keepcnt)) < 0)
	{
		LOG_DEBUG << "TCP_KEEPCNT";
	}
#endif
}

void Socket::setReuseAddr(int32_t sockfd, bool on)
{
	int32_t optval = on ? 1 : 0;
	::setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR,
		(char*)&optval, static_cast<socklen_t>(sizeof optval));
	// FIXME CHECK
}

void Socket::setReusePort(int32_t sockfd, bool on)
{
#ifdef SO_REUSEPORT
	int32_t optval = on ? 1 : 0;
	int32_t ret = ::setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT,
		&optval, static_cast<socklen_t>(sizeof optval));
	if (ret < 0 && on)
	{
		LOG_WARN << "SO_REUSEPORT failed.";
	}
#else
	if (on)
	{
		LOG_WARN << "SO_REUSEPORT is not supported.";
	}
#endif
}

int32_t Socket::createTcpSocket(const char *ip, int16_t port)
{
	struct sockaddr_in sa;
	sa.sin_family = AF_INET;
	sa.sin_port = htons(port);
	sa.sin_addr.s_addr = inet_addr(ip);

	/*#ifdef __APPLE__
	   struct sockaddr_un sa;
	   sa.sun_family = AF_UNIX;
	   char *path = "./redis.sock";
	   strncpy(sa.sun_path,path,sizeof(sa.sun_path) - 1);
	   mode_t unixsocketperm = 777;
	   ::chmod(sa.sun_path,unixsocketperm);
   #endif
   */

	int32_t sockfd = createSocket();
	if (sockfd < 0)
	{
		LOG_WARN << "Create Tcp Socket Failed! " << strerror(errno);
		return false;
	}

	if (!setSocketNonBlock(sockfd))
	{
		LOG_WARN << "Set listen socket to non-block failed!";
		return false;
	}

	int32_t optval = 1;
#ifdef _WIN32
	if (::setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(optval)) < 0)
	{
		LOG_WARN << "Set SO_REUSEPORT socket failed! error " << strerror(errno);
		Socket::close(sockfd);
		return false;
	}
#else
	if (::setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, (const char*)&optval, sizeof(optval)) < 0)
	{
		LOG_WARN << "Set SO_REUSEPORT socket failed! error " << strerror(errno);
		Socket::close(sockfd);
		return false;
	}
#endif

	if (::setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(optval)) < 0)
	{
		LOG_WARN << "Set SO_REUSEADDR socket  failed! error " << strerror(errno);
		Socket::close(sockfd);
		return false;
	}

	if (::bind(sockfd, (struct sockaddr*)&sa, sizeof(sa)) < 0)
	{
		LOG_WARN << "Bind bind socket failed! error " << strerror(errno);
		Socket::close(sockfd);
		return false;
	}

	//        FILE *fp = fopen("/proc/sys/net/core/somaxconn","r");
	//        char buf[1024];
	//        if (!fp) return;
	//        if (fgets(buf,sizeof(buf),fp) != NULL)
	//            int32_t somaxconn = atoi(buf);
	//            if (somaxconn > 0 && somaxconn < server.tcp_backlog)

	if (::listen(sockfd, SOMAXCONN))
	{
		LOG_WARN << "Listen listen socket failed! error " << strerror(errno);
		close(sockfd);
		return false;
	}

#ifdef __linux__
	::setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &optval, static_cast<socklen_t>(sizeof optval));

	int32_t len = 65536;
	::setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, (void*)&len, sizeof(len));
	::setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, (void*)&len, sizeof(len));
#endif
	return sockfd;
}

bool Socket::setTcpNoDelay(int32_t sockfd, bool on)
{
#ifdef __linux__
	int32_t optval = on ? 1 : 0;
	int32_t opt = ::setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &optval, static_cast<socklen_t>(sizeof optval));
	if (opt < 0)
	{
		return false;
	}
#endif
	return true;
}

bool Socket::setSocketBlock(int32_t sockfd)
{
#ifdef _WIN32
	u_long nonblock = 0;
	int32_t ret = ::ioctlsocket(sockfd, FIONBIO, &nonblock);
	if (ret < 0)
	{
		return false;
	}
	return true;
#else
	int32_t opt = ::fcntl(sockfd, F_GETFL);
	if (opt < 0)
	{
		LOG_WARN << "fcntl F_GETFL) failed! error" << strerror(errno);
		return false;
	}

	opt = opt & ~O_NONBLOCK;
	if (::fcntl(sockfd, F_SETFL, opt) < 0)
	{
		LOG_WARN << "fcntl F_GETFL) failed! error" << strerror(errno);
		return false;
	}
	return true;
#endif

}

bool Socket::setSocketNonBlock(int32_t sockfd)
{
#ifdef _WIN32
	u_long nonblock = 1;
	int32_t ret = ::ioctlsocket(sockfd, FIONBIO, &nonblock);
	if (ret < 0)
	{
		return false;
	}
	return true;
#else
	int32_t opt = ::fcntl(sockfd, F_GETFL);
	if (opt < 0)
	{
		LOG_WARN << "fcntl F_GETFL) failed! error" << strerror(errno);
		return false;
	}

	opt = opt | O_NONBLOCK;
	if (::fcntl(sockfd, F_SETFL, opt) < 0)
	{
		LOG_WARN << "fcntl F_GETFL) failed! error" << strerror(errno);
		return false;
	}
	return true;
#endif
}
