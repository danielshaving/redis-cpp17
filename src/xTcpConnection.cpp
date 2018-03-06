#include "all.h"
#include "xEventLoop.h"
#include "xTcpConnection.h"

xTcpConnection::xTcpConnection(xEventLoop *loop,int32_t sockfd,const std::any & context)
:loop(loop),
 sockfd(sockfd),
 state(kConnecting),
 channel(new xChannel(loop,sockfd)),
 context(context)
{
	channel->setReadCallback(
	  std::bind(&xTcpConnection::handleRead, this));
	channel->setWriteCallback(
	  std::bind(&xTcpConnection::handleWrite, this));
	channel->setCloseCallback(
	  std::bind(&xTcpConnection::handleClose, this));
	channel->setErrorCallback(
	  std::bind(&xTcpConnection::handleError, this));
}

xTcpConnection::~xTcpConnection()
{
    assert(state == kDisconnected);
    ::close(sockfd);
}



void xTcpConnection::shutdown()
{
    if (state == kConnected)
    {
        setState(kDisconnecting);
        loop->runInLoop(std::bind(&xTcpConnection::shutdownInLoop, this));
     }
}


void xTcpConnection::forceClose()
{
	  if (state == kConnected || state == kDisconnecting)
	  {
	    setState(kDisconnecting);
	    loop->queueInLoop(std::bind(&xTcpConnection::forceCloseInLoop, shared_from_this()));
	  }
}


void xTcpConnection::forceCloseInLoop()
{
  loop->assertInLoopThread();
  if (state== kConnected   ||  state == kDisconnecting)
  {
        // as if we received 0 byte in handleRead();
        handleClose();
  }
}

void xTcpConnection::shutdownInLoop()
{
    loop->assertInLoopThread();
    if (!channel->isWriting())
    {
        if (::shutdown(sockfd, SHUT_WR) < 0)
        {
            //TRACE_ERR("sockets::shutdownWrite");
        }
    }
}

void xTcpConnection::handleRead()
{
	loop->assertInLoopThread();
	int savedErrno = 0;
	ssize_t n = recvBuff.readFd(channel->getfd(), &savedErrno);
	if (n > 0)
	{
		messageCallback(shared_from_this(), &recvBuff);
	}
	else if (n == 0)
	{
		handleClose();
	}
	else
	{
		errno = savedErrno;
		if(errno != ECONNRESET || errno !=  ETIMEDOUT)
		{
#ifdef __DEBUG__
			LOG_ERROR<<"TcpConnection::handleRead "<<errno;
#endif
		}
#ifdef __APPLE__
		handleClose();
#endif
		//handleError();
	}
}

void xTcpConnection::handleWrite()
{
	loop->assertInLoopThread();
	if (channel->isWriting())
	{
		ssize_t n = ::write(channel->getfd(),sendBuff.peek(),sendBuff.readableBytes());
		if (n > 0)
		{
			sendBuff.retrieve(n);
			if (sendBuff.readableBytes() == 0)
			{
				channel->disableWriting();
				if (writeCompleteCallback)
				{
					loop->queueInLoop(std::bind(writeCompleteCallback, shared_from_this()));
				}
				if (state == kDisconnecting)
				{
					shutdownInLoop();
				}
			}
		}
		else
		{
			LOG_ERROR<<"TcpConnection::handleWrite";
		}

	}
	else
	{
		LOG_ERROR<<"Connection fd  is down, no more writing "<< channel->getfd();
	}
}
void xTcpConnection::handleClose()
{
	loop->assertInLoopThread();
	assert(state == kConnected || state == kDisconnecting);
	setState(kDisconnected);
	channel->disableAll();
	TcpConnectionPtr guardThis(shared_from_this());
	connectionCallback(guardThis);
	closeCallback(guardThis);
}

void xTcpConnection::handleError()
{
	//LOG_ERROR<<"handleError";
}

bool xTcpConnection::connected()
{
	return state == kConnected;
}

xEventLoop *xTcpConnection::getLoop()
{
	return loop;
}

int xTcpConnection::getSockfd()
{
	return sockfd;
}

void xTcpConnection::sendPipe(xBuffer *buf)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendPipeInLoop(buf->peek(),buf->readableBytes());
		}
		else
		{
			loop->runInLoop(
						  std::bind(&bindSendPipeInLoop,
								  this, buf->retrieveAllAsString()));
		}
	}
}


void xTcpConnection::sendPipe(const void *message, int len)
{
    send(xStringPiece(static_cast<const char*>(message), len));
}

void xTcpConnection::sendPipe(const xStringPiece &message)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendPipeInLoop(message.data(),message.size());
		}
		else
		{
			loop->runInLoop(
						  std::bind(&bindSendPipeInLoop,
								  this, message.as_string()));
		}
	}
}

void xTcpConnection::send(const void *message, int len)
{
	send(xStringPiece(static_cast<const char*>(message), len));
}


void xTcpConnection::send(const xStringPiece &message)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
		  	sendPipeInLoop(message.data(),message.size());
		}
		else
		{
		  	loop->runInLoop(
				  std::bind(&bindSendInLoop,
						  this, message.as_string()));
		}
	}
}

void xTcpConnection::sendPipeInLoop(const xStringPiece &message)
{
	sendPipeInLoop(message.data(),message.size());
}



void xTcpConnection::sendPipeInLoop(const void *message, size_t len)
{
	sendBuff.append(message,len);
	if (!channel->isWriting())
	{
		channel->enableWriting();
	}
}

void xTcpConnection::bindSendPipeInLoop(xTcpConnection *conn, const xStringPiece &message)
{
	conn->sendPipeInLoop(message.data(),message.size());
}


void xTcpConnection::bindSendInLoop(xTcpConnection *conn, const xStringPiece &message)
{
	 conn->sendInLoop(message.data(),message.size());
}

void xTcpConnection::send(xBuffer* buf)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
		  	sendInLoop(buf->peek(), buf->readableBytes());
		}
		else
		{
		  	loop->runInLoop(
		    	  std::bind(&bindSendInLoop,
		                  this, buf->retrieveAllAsString()));
		}
	}
}

void xTcpConnection::sendInLoop(const xStringPiece & message)
{
	sendInLoop(message.data(),message.size());
}


void xTcpConnection::sendInLoop(const void* data, size_t len)
{
	loop->assertInLoopThread();
	ssize_t nwrote = 0;
	size_t remaining = len;
	bool faultError = false;
	if (state == kDisconnected)
	{
		assert(false);
		return;
	}

	if (!channel->isWriting() && sendBuff.readableBytes() == 0)
	{
		nwrote = ::write(channel->getfd(), data, len);
		if (nwrote >= 0)
		{
			remaining = len - nwrote;
			if (remaining == 0 && writeCompleteCallback)
			{
				loop->queueInLoop(std::bind(writeCompleteCallback, shared_from_this()));
			}
		}
		else // nwrote < 0
		{
			nwrote = 0;
			if (errno != EWOULDBLOCK)
			{
				if (errno == EPIPE || errno == ECONNRESET) // FIXME: any others?
				{
				  	faultError = true;
				}
			}
		}
	}

	assert(remaining <= len);
	if (!faultError && remaining > 0)
	{
		size_t oldLen = sendBuff.readableBytes();
		if (oldLen + remaining >= highWaterMark
		    && oldLen < highWaterMark
		    && highWaterMarkCallback)
		{
		  	loop->queueInLoop(std::bind(highWaterMarkCallback, shared_from_this(), oldLen + remaining));
		}
		
		sendBuff.append(static_cast<const char*>(data)+nwrote, remaining);
		if (!channel->isWriting())
		{
		  	channel->enableWriting();
		}
	}
}

void xTcpConnection::connectEstablished()
{
	loop->assertInLoopThread();
	assert(state == kConnecting);
	setState(kConnected);
	channel->setTie(shared_from_this());
	channel->enableReading();
	connectionCallback(shared_from_this());
}

void xTcpConnection::connectDestroyed()
{
	loop->assertInLoopThread();
	if (state == kConnected)
	{
		setState(kDisconnected);
		channel->disableAll();
		connectionCallback(shared_from_this());
	}
	channel->remove();
}


