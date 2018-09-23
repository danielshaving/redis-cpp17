#include "connector.h"

const int Connector::kMaxRetryDelayMs;
Connector::Connector(EventLoop *loop, const char *ip, int16_t port)
	:loop(loop),
	ip(ip),
	port(port),
	state(kDisconnected),
	connect(false),
	retryDelayMs(kInitRetryDelayMs)
{

}

Connector::~Connector()
{
	assert(!channel);
}

void Connector::start(bool s)
{
	connect = true;
	loop->runInLoop(std::bind(&Connector::startInLoop, this, s));
}

void Connector::startInLoop(bool s)
{
	loop->assertInLoopThread();
	assert(state == kDisconnected);
	if (connect)
	{
		connecting(s);
	}
	else
	{
		LOG_WARN << "do not connect";
	}
}

void Connector::stop()
{
	connect = false;
	loop->queueInLoop(std::bind(&Connector::stopInLoop, this));
}

void Connector::stopInLoop()
{
	loop->assertInLoopThread();
	if (state == kConnecting)
	{
		setState(kDisconnected);
		removeAndResetChannel();
	}
}

void Connector::resetChannel()
{
	channel.reset();
}

int32_t Connector::removeAndResetChannel()
{
	channel->disableAll();
	channel->remove();
	int32_t sockfd = channel->getfd();
	loop->queueInLoop(std::bind(&Connector::resetChannel, this));
	return sockfd;
}

void Connector::connecting(bool s, int32_t sockfd)
{
	if (s)
	{
		setState(kConnected);
		if (connect)
		{
			newConnectionCallback(sockfd);
		}
		else
		{
			Socket::close(sockfd);
		}
	}
	else
	{
		assert(!channel);
		channel.reset(new Channel(loop, sockfd));
		channel->setWriteCallback(std::bind(&Connector::handleWrite, this));
		channel->setErrorCallback(std::bind(&Connector::handleError, this));
		channel->enableWriting();
	}
}

void Connector::retry(int32_t sockfd)
{
	Socket::close(sockfd);
	setState(kDisconnected);
	if (connect)
	{
		LOG_INFO << "Connector::retry - Retry connecting to " << ip << " " << port
			<< " in " << retryDelayMs << " milliseconds. ";
		loop->runAfter(retryDelayMs / 1000.0, false,
			std::bind(&Connector::startInLoop, shared_from_this(), false));
#ifdef _WIN64
		retryDelayMs = (retryDelayMs * 2) < (kMaxRetryDelayMs) ? (retryDelayMs * 2) : (kMaxRetryDelayMs);
#else
		retryDelayMs = std::min(retryDelayMs * 2, kMaxRetryDelayMs);
#endif
	}
	else
	{
		LOG_DEBUG << "do not connect";
	}
}

void Connector::restart()
{
	loop->assertInLoopThread();
	setState(kDisconnected);
	retryDelayMs = kInitRetryDelayMs;
	connect = true;
	startInLoop(false);
}

void Connector::handleWrite()
{
	if (state == kConnecting)
	{
		int sockfd = removeAndResetChannel();
		int err = Socket::getSocketError(sockfd);
		if (err)
		{
			retry(sockfd);
		}
		else if (Socket::isSelfConnect(sockfd))
		{
			LOG_WARN << "Connector::handleWrite - Self connect";
			retry(sockfd);
		}
		else
		{
			setState(kConnected);
			if (connect)
			{
				newConnectionCallback(sockfd);
				Socket::setkeepAlive(sockfd, 1);
			}
			else
			{
				Socket::close(sockfd);
			}
		}
	}
	else
	{
		assert(state == kDisconnected);
	}
}

void Connector::handleError()
{
	if (state == kConnecting)
	{
		int sockfd = removeAndResetChannel();
		int err = Socket::getSocketError(sockfd);
		LOG_TRACE << "SO_ERROR = " << err << " " << strerror(err);
		retry(sockfd);
	}
}

void Connector::connecting(bool s)
{
	int32_t sockfd = Socket::createSocket();
	assert(sockfd >= 0);

	int32_t savedErrno = Socket::connect(sockfd, ip.c_str(), port);
	if (savedErrno < 0)
	{
#ifdef _WIN64
		savedErrno = GetLastError();
#else
		savedErrno = errno;
#endif
	}

	switch (savedErrno)
	{
	case 0:
	case EINPROGRESS:
	case EINTR:
	case EISCONN:
		Socket::setSocketNonBlock(sockfd);
		setState(kConnecting);
		connecting(s, sockfd);
		break;

	case EAGAIN:
	case EADDRINUSE:
	case EADDRNOTAVAIL:
	case ECONNREFUSED:
	case ENETUNREACH:
		retry(sockfd);
		break;

	case EACCES:
	case EPERM:
	case EAFNOSUPPORT:
	case EALREADY:
	case EBADF:
	case EFAULT:
	case ENOTSOCK:
		LOG_WARN << "connect error " << savedErrno << " " << ip << " " << port;
		Socket::close(sockfd);
		break;

	default:
		LOG_WARN << "Unexpected error " << savedErrno << " " << ip << " " << port;
		Socket::close(sockfd);
		break;
	}
}

