#include "xConnector.h"

const int xConnector::kMaxRetryDelayMs;
xConnector::xConnector(xEventLoop *loop,const char *ip,int16_t port)
 :loop(loop),
  ip(ip),
  port(port),
  state(kDisconnected),
  connect(false),
  retryDelayMs(kInitRetryDelayMs)
{

}

xConnector::~xConnector()
{
	assert(!channel);
}

void xConnector::syncStart()
{
	connect = true;
	loop->runInLoop(std::bind(&xConnector::syncStartInLoop,this));
}

void xConnector::asyncStart()
{
	connect = true;
	loop->runInLoop(std::bind(&xConnector::asyncStartInLoop,this));
}

void xConnector::startInLoop(const std::any &context)
{
	asyncStartInLoop();
}

void xConnector::syncStartInLoop()
{
	loop->assertInLoopThread();
	if (connect)
	{
		syncConnect();
	}
	else
	{
		LOG_WARN<<"do not async connect";
	}
}

void xConnector::asyncStartInLoop()
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

void xConnector::stop()
{
	connect= false;
	loop->queueInLoop(std::bind(&xConnector::stopInLoop, this));
}

void xConnector::stopInLoop()
{
	loop->assertInLoopThread();
	if (state == kConnecting)
	{
		setState(kDisconnected);
		removeAndResetChannel();
	}
}

void xConnector::resetChannel()
{
	channel.reset();
}

int32_t  xConnector::removeAndResetChannel()
{
	channel->disableAll();
	channel->remove();
	int32_t sockfd = channel->getfd();
	loop->queueInLoop(std::bind(&xConnector::resetChannel,this));
	return sockfd;
}

void xConnector::connecting(int32_t sockfd)
{
	assert(!channel);
	channel.reset(new xChannel(loop, sockfd));
	channel->setWriteCallback(std::bind(&xConnector::handleWrite, this));
	channel->setErrorCallback(std::bind(&xConnector::handleError, this));
	channel->enableWriting();
}

void xConnector::retry(int32_t sockfd)
{
	::close(sockfd);
	setState(kDisconnected);
	if (connect)
	{
		LOG_INFO << "Connector::retry - Retry connecting to "<<ip<<" "<<port
				 << " in " << retryDelayMs << " milliseconds. ";
		loop->runAfter(retryDelayMs/1000.0,nullptr,false,
						std::bind(&xConnector::startInLoop, shared_from_this(),std::placeholders::_1));
		retryDelayMs = std::min(retryDelayMs * 2,kMaxRetryDelayMs);
	}
	else
	{
		LOG_DEBUG << "do not connect";
	}
}

void xConnector::restart()
{
	loop->assertInLoopThread();
	setState(kDisconnected);
	retryDelayMs = kInitRetryDelayMs;
	connect = true;
	asyncStartInLoop();
}

void xConnector::syncConnect()
{

}

void xConnector::handleWrite()
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

void xConnector::handleError()
{
	if (state == kConnecting)
	{
		int sockfd = removeAndResetChannel();
		int err = socket.getSocketError(sockfd);
		LOG_TRACE << "SO_ERROR = " << err << " " << strerror(err);
		retry(sockfd);
	}
}

void xConnector::asyncConnect()
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
			LOG_ERROR << "connect error in xConnector::startInLoop " << savedErrno;
			::close(sockfd);
			break;

		default:
			LOG_ERROR << "Unexpected error in xConnector::startInLoop " << savedErrno;
			::close(sockfd);
			// connectErrorCallback();
			break;
	}
}
