#include "acceptor.h"
#include "log.h"

Acceptor::Acceptor(EventLoop *loop, const char *ip, int16_t port)
	:loop(loop),
	channel(loop, Socket::createTcpSocket(ip, port)),
	sockfd(channel.getfd()),
#ifndef _WIN32
	idleFd(::open("/dev/null", O_RDONLY | O_CLOEXEC)),
#endif
	listenning(false)
{
#ifndef _WIN32
	assert(idleFd >= 0);
#endif
	channel.setReadCallback(std::bind(&Acceptor::handleRead, this));
}

Acceptor::~Acceptor()
{
	channel.disableAll();
	channel.remove();
	Socket::close(sockfd);
}

void Acceptor::handleRead()
{
	loop->assertInLoopThread();
	struct sockaddr_in6 address;
	socklen_t len = sizeof(address);
#ifdef __linux__
	int32_t connfd = ::accept4(sockfd, (struct sockaddr*)&address,
		&len, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
	int32_t connfd = ::accept(sockfd, (struct sockaddr*)&address, &len);
#endif

	if (connfd >= 0)
	{
		if (newConnectionCallback)
		{
			socket.setSocketNonBlock(connfd);
			newConnectionCallback(connfd);
		}
		else
		{
			Socket::close(sockfd);
		}
	}
	else
	{

	}
}

void Acceptor::listen()
{
	loop->assertInLoopThread();
	listenning = true;
	channel.enableReading();
}
