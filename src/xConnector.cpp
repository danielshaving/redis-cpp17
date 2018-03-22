#include "xConnector.h"

xConnector::xConnector(xEventLoop *loop)
 :loop(loop),
  state(kDisconnected),
  isconnect(false)
{

}

xConnector::~xConnector()
{

}

void xConnector::syncStart(const char *ip, int16_t port)
{
	isconnect = true;
	loop->runInLoop(std::bind(&xConnector::syncStartInLoop,this,ip,port));
}

void xConnector::asyncStart(const char *ip, int16_t port)
{
	isconnect = true;
	loop->runInLoop(std::bind(&xConnector::asyncStartInLoop,this,ip,port));
}

void xConnector::syncStartInLoop(const char *ip,int16_t port)
{
	loop->assertInLoopThread();
	if (isconnect)
	{
		syncConnect(ip,port);
	}
	else
	{
		LOG_WARN<<"do not async connect";
	}
}

void xConnector::asyncStartInLoop(const char *ip, int16_t port)
{
	loop->assertInLoopThread();
	if (isconnect)
	{
		asyncConnect(ip,port);
	}
	else
	{
		LOG_WARN<<"do not sync connect";
	}
}

void xConnector::stop()
{
	isconnect= false;
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
	loop->queueInLoop(std::bind(&xConnector::resetChannel, this));
	return sockfd;
}

void xConnector::connecting(int32_t sockfd)
{
	if(state == kConnecting)
	{
		newConnectionCallback(sockfd);
	}
	else
	{
		LOG_ERROR<<"connect error";
	}
}

void xConnector::syncConnect(const char *ip,int16_t port)
{

}

void xConnector::asyncConnect(const char *ip, int16_t port)
{
	int32_t sockfd = socket.createSocket();
	int32_t ret = socket.connect(sockfd, ip,port);
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
			socket.setkeepAlive(sockfd,3);
			break;
		default:
			LOG_WARN<<strerror(savedErrno);
			::close(sockfd);
			setState(kDisconnected);

			if(errorConnectionCallback)
			{
				errorConnectionCallback();
			}
			break;
	}
}
