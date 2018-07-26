#include "connector.h"

const int Connector::kMaxRetryDelayMs;
Connector::Connector(EventLoop *loop,const char *ip,int16_t port)
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

bool Connector::syncStart()
{
	connect = true;
	return syncStartInLoop();
}

void Connector::asyncStart()
{
	connect = true;
	loop->runInLoop(std::bind(&Connector::asyncStartInLoop,this));
}

void Connector::startInLoop()
{
	asyncStartInLoop();
}

bool Connector::syncStartInLoop()
{
	loop->assertInLoopThread();
	if (connect)
	{
		return syncConnect();
	}
	else
	{
		LOG_WARN<<"do not async connect";
	}
}

void Connector::asyncStartInLoop()
{
	loop->assertInLoopThread();
	if (connect)
	{
		asyncConnect();
	}
	else
	{
		LOG_WARN<<"do not sync connect";
	}
}

void Connector::stop()
{
	connect= false;
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
	loop->queueInLoop(std::bind(&Connector::resetChannel,this));
	return sockfd;
}

void Connector::connecting(int32_t sockfd)
{
	assert(!channel);
	channel.reset(new Channel(loop,sockfd));
	channel->setWriteCallback(std::bind(&Connector::handleWrite,this));
	channel->setErrorCallback(std::bind(&Connector::handleError,this));
	channel->enableWriting();
}

void Connector::retry(int32_t sockfd)
{
	::close(sockfd);
	setState(kDisconnected);
	if (connect)
	{
		LOG_INFO << "Connector::retry - Retry connecting to "<<ip<<" "<<port
				 << " in " << retryDelayMs << " milliseconds. ";
		loop->runAfter(retryDelayMs/1000.0,false,
						std::bind(&Connector::startInLoop,shared_from_this()));
		retryDelayMs = std::min(retryDelayMs * 2,kMaxRetryDelayMs);
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
	asyncStartInLoop();
}

void Connector::handleWrite()
{
	if(state == kConnecting)
	{
		int sockfd = removeAndResetChannel();
		int err = socket.getSocketError(sockfd);
		if (err)
		{
			retry(sockfd);
		}
		else if (socket.isSelfConnect(sockfd))
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
			}
			else
			{
				::close(sockfd);
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
		int err = socket.getSocketError(sockfd);
		LOG_TRACE << "SO_ERROR = " << err << " " << strerror(err);
		retry(sockfd);
	}
}

void Connector::asyncConnect()
{
	int32_t sockfd = socket.createSocket();
	int32_t ret = socket.connect(sockfd,ip,port);
	int32_t savedErrno = (ret == 0) ? 0 : errno;
	switch (savedErrno)
	{
		case 0:
		case EINPROGRESS:
		case EINTR:
		case EISCONN:
			socket.setSocketNonBlock(sockfd);
			setState(kConnecting);
			connecting(sockfd);
			socket.setkeepAlive(sockfd,1);;
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
			LOG_ERROR << "connect error in Connector::startInLoop " << savedErrno;
			::close(sockfd);
			break;

		default:
			LOG_ERROR << "Unexpected error in Connector::startInLoop " << savedErrno;
			::close(sockfd);
			// connectErrorCallback();
			break;
	}
}

bool Connector::syncConnect()
{
    int32_t blocking = 1;
    int32_t sockfd = socket.createSocket();
    if(sockfd == -1)
    {
    	return false;
    }

    if(!socket.setSocketNonBlock(sockfd))
    {
    	return false;
    }

    int32_t ret = socket.connect(sockfd,ip,port);
    int32_t savedErrno = (ret == 0) ? 0 : errno;
	if (savedErrno == EINPROGRESS && !blocking)
	{

	}
	else
	{
		if(!socket.connectWaitReady(sockfd,kInitRetryDelayMs))
		{
			LOG_ERROR << "Unexpected error" << savedErrno <<" " << strerror(errno);
			return false;
		}
	}

    setState(kConnected);
    if(!socket.setSocketBlock(sockfd))
    {
    	return false;
    }

    if(!socket.setTcpNoDelay(sockfd,true))
    {
    	return false;
    }
    return true;
}

