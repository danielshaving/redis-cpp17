#include "all.h"
#include "xEventLoop.h"
#include "xTcpconnection.h"

xTcpconnection::xTcpconnection(xEventLoop *loop,int sockfd,void *data)
:loop(loop),
 sockfd(sockfd),
 state(kConnecting),
 channel(new xChannel(loop,sockfd)),
 data(data)
{
	channel->setReadCallback(
	  std::bind(&xTcpconnection::handleRead, this));
	channel->setWriteCallback(
	  std::bind(&xTcpconnection::handleWrite, this));
	channel->setCloseCallback(
	  std::bind(&xTcpconnection::handleClose, this));
	channel->setErrorCallback(
	  std::bind(&xTcpconnection::handleError, this));
}

xTcpconnection::~xTcpconnection()
{
    assert(state == kDisconnected);
    ::close(sockfd);
}



void xTcpconnection::shutdown()
{
    if (state == kConnected)
    {
        setState(kDisconnecting);
        loop->runInLoop(std::bind(&xTcpconnection::shutdownInLoop, this));
     }
}


void xTcpconnection::forceClose()
{
	  if (state == kConnected || state == kDisconnecting)
	  {
	    setState(kDisconnecting);
	    loop->queueInLoop(std::bind(&xTcpconnection::forceCloseInLoop, shared_from_this()));
	  }
}


void xTcpconnection::forceCloseInLoop()
{
  loop->assertInLoopThread();
  if (state== kConnected   ||  state == kDisconnecting)
  {
        // as if we received 0 byte in handleRead();
        handleClose();
  }
}

void xTcpconnection::shutdownInLoop()
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

void xTcpconnection::handleRead()
{
	loop->assertInLoopThread();
	int savedErrno = 0;
	ssize_t n = recvBuff.readFd(channel->getfd(), &savedErrno);
	if (n > 0)
	{
		messageCallback(shared_from_this(), &recvBuff,data);
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

void xTcpconnection::handleWrite()
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
void xTcpconnection::handleClose()
{
	loop->assertInLoopThread();
	assert(state == kConnected || state == kDisconnecting);
	setState(kDisconnected);
	channel->disableAll();
	xTcpconnectionPtr guardThis(shared_from_this());
	connectionCallback(guardThis,data);
	closeCallback(guardThis);
}
void xTcpconnection::handleError()
{
	LOG_ERROR<<"handleError";
}


bool xTcpconnection::connected()
{
	return state == kConnected;
}

xEventLoop *xTcpconnection::getLoop()
{
	return loop;
}

int xTcpconnection::getSockfd()
{
	return sockfd;
}


void xTcpconnection::sendPipe(xBuffer* buf)
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


void xTcpconnection::sendPipe(const void* message, int len)
{

    send(xStringPiece(static_cast<const char*>(message), len));

}

void xTcpconnection::sendPipe(const xStringPiece & message)
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

void xTcpconnection::send(const void* message, int len)
{
	send(xStringPiece(static_cast<const char*>(message), len));
}


void xTcpconnection::send(const xStringPiece & message)
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

void xTcpconnection::sendPipeInLoop(const xStringPiece & message)
{
	sendPipeInLoop(message.data(),message.size());
}



void xTcpconnection::sendPipeInLoop(const void* message, size_t len)
{
	sendBuff.append(message,len);
	if (!channel->isWriting())
	{
		channel->enableWriting();
	}
}

void xTcpconnection::bindSendPipeInLoop(xTcpconnection* conn, const xStringPiece& message)
{
	conn->sendPipeInLoop(message.data(),message.size());
}


void xTcpconnection::bindSendInLoop(xTcpconnection* conn, const xStringPiece& message)
{
	 conn->sendInLoop(message.data(),message.size());
}

void xTcpconnection::send(xBuffer* buf)
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

void xTcpconnection::sendInLoop(const xStringPiece & message)
{
	sendInLoop(message.data(),message.size());
}


void xTcpconnection::sendInLoop(const void* data, size_t len)
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

void xTcpconnection::connectEstablished()
{
	loop->assertInLoopThread();
	assert(state == kConnecting);
	setState(kConnected);
	channel->setTie(shared_from_this());
	channel->enableReading();
	connectionCallback(shared_from_this(),data);
}

void xTcpconnection::connectDestroyed()
{
	loop->assertInLoopThread();
	if (state == kConnected)
	{
		setState(kDisconnected);
		channel->disableAll();
		connectionCallback(shared_from_this(),data);
	}
	channel->remove();
}


