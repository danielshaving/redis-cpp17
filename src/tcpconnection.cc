#include "eventloop.h"
#include "tcpconnection.h"

TcpConnection::TcpConnection(EventLoop *loop,int32_t sockfd,const std::any &context)
:loop(loop),
 sockfd(sockfd),
 reading(true),
 state(kConnecting),
 channel(new Channel(loop,sockfd)),
 context(context)
{
	channel->setReadCallback(
	  std::bind(&TcpConnection::handleRead,this));
	channel->setWriteCallback(
	  std::bind(&TcpConnection::handleWrite,this));
	channel->setCloseCallback(
	  std::bind(&TcpConnection::handleClose,this));
	channel->setErrorCallback(
	  std::bind(&TcpConnection::handleError,this));
}

TcpConnection::~TcpConnection()
{
	assert(state == kDisconnected);
	::close(sockfd);
}

void TcpConnection::shutdown()
{
	if (state == kConnected)
	{
		setState(kDisconnecting);
		loop->runInLoop(std::bind(&TcpConnection::shutdownInLoop,this));
	}
}

void TcpConnection::forceClose()
{
	if (state == kConnected || state == kDisconnecting)
	{
		setState(kDisconnecting);
		loop->queueInLoop(std::bind(&TcpConnection::forceCloseInLoop,shared_from_this()));
	}
}

void TcpConnection::forceCloseInLoop()
{
	loop->assertInLoopThread();
	if (state== kConnected || state == kDisconnecting)
	{
		// as if we received 0 byte in handleRead();
		handleClose();
	}
}

void TcpConnection::shutdownInLoop()
{
    loop->assertInLoopThread();
    if (!channel->isWriting())
    {
        if (::shutdown(sockfd,SHUT_WR) < 0)
        {
            LOG_ERROR<<"sockets::shutdownWrite";
        }
    }
}

void TcpConnection::handleRead()
{
	loop->assertInLoopThread();
	int savedErrno = 0;
	ssize_t n = recvBuff.readFd(channel->getfd(),&savedErrno);
	if (n > 0)
	{
		messageCallback(shared_from_this(),&recvBuff);
	}
	else if (n == 0)
	{
		handleClose();
	}
	else
	{
		errno = savedErrno;
		if (errno != ECONNRESET || errno != ETIMEDOUT)
		{

		}
	}
}

void TcpConnection::handleWrite()
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
					loop->queueInLoop(std::bind(writeCompleteCallback,shared_from_this()));
				}

				if (state == kDisconnecting)
				{
					shutdownInLoop();
				}
			}
		}
		else
		{

		}

	}
	else
	{

	}
}

void TcpConnection::handleClose()
{
	loop->assertInLoopThread();
	assert(state == kConnected || state == kDisconnecting);
	setState(kDisconnected);
	channel->disableAll();
	TcpConnectionPtr guardThis(shared_from_this());
	connectionCallback(guardThis);
	closeCallback(guardThis);
}

void TcpConnection::startRead()
{
	loop->runInLoop(std::bind(&TcpConnection::startReadInLoop,this));
}

void TcpConnection::stopRead()
{
	loop->runInLoop(std::bind(&TcpConnection::stopReadInLoop,this));
}

void TcpConnection::startReadInLoop()
{
	loop->assertInLoopThread();
	if (!reading || !channel->isReading())
	{
		channel->enableReading();
		reading = true;
	}
}

void TcpConnection::stopReadInLoop()
{
	loop->assertInLoopThread();
	if (reading || channel->isReading())
	{
		channel->disableReading();
		reading = false;
	}
}

void TcpConnection::forceCloseDelay()
{
	forceClose();
}

void TcpConnection::forceCloseWithDelay(double seconds)
{
	if (state == kConnected || state == kDisconnecting)
	{
		setState(kDisconnecting);
		loop->runAfter(seconds,false,std::bind(&TcpConnection::forceCloseDelay,shared_from_this()));
	}
}

void TcpConnection::handleError()
{

}

void TcpConnection::sendPipe(Buffer *buf)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendPipeInLoop(buf->peek(),buf->readableBytes());
		}
		else
		{
			loop->runInLoop(std::bind(&bindSendPipeInLoop,this,buf->retrieveAllAsString()));
		}
	}
}

void TcpConnection::sendPipe(const void *message,int len)
{
    sendPipe(std::string_view(static_cast<const char*>(message),len));
}

void TcpConnection::sendPipe(const std::string_view &message)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendPipeInLoop(message.data(),message.size());
		}
		else
		{
			void (TcpConnection::*fp)(const std::string_view &message) = &TcpConnection::sendPipeInLoop;
			loop->runInLoop(std::bind(fp,this,std::string(message)));
			//loop->runInLoop(std::bind(&bindSendPipeInLoop,this,std::string(message)));
		}
	}
}

void TcpConnection::send(const void *message,int len)
{
	send(std::string_view(static_cast<const char*>(message),len));
}

void TcpConnection::send(const std::string_view &message)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
		  	sendInLoop(message.data(),message.size());
		}
		else
		{
			void (TcpConnection::*fp)(const std::string_view &message) = &TcpConnection::sendInLoop;
			loop->runInLoop( std::bind(fp,this,std::string(message)));
		  	//loop->runInLoop(std::bind(&bindSendInLoop,this,std::string(message)));
		}
	}
}

void TcpConnection::sendPipeInLoop(const std::string_view &message)
{
	sendPipeInLoop(message.data(),message.size());
}

void TcpConnection::sendPipeInLoop(const void *message,size_t len)
{
	sendBuff.append(message,len);
	if (!channel->isWriting())
	{
		channel->enableWriting();
	}
}

void TcpConnection::bindSendPipeInLoop(TcpConnection *conn,const std::string_view &message)
{
	conn->sendPipeInLoop(message.data(),message.size());
}

void TcpConnection::bindSendInLoop(TcpConnection *conn,const std::string_view &message)
{
	conn->sendInLoop(message.data(),message.size());
}

void TcpConnection::send(Buffer *buf)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
		  	sendInLoop(buf->peek(),buf->readableBytes());
		}
		else
		{
		  	loop->runInLoop(std::bind(&bindSendInLoop,this,buf->retrieveAllAsString()));
		}
	}
}

void TcpConnection::sendInLoop(const std::string_view &message)
{
	sendInLoop(message.data(),message.size());
}

void TcpConnection::sendInLoop(const void *data,size_t len)
{
	loop->assertInLoopThread();
	ssize_t nwrote = 0;
	size_t remaining = len;
	bool faultError = false;
	assert(state != kDisconnected);

	if (!channel->isWriting() && sendBuff.readableBytes() == 0)
	{
		nwrote = ::write(channel->getfd(),data,len);
		if (nwrote >= 0)
		{
			remaining = len - nwrote;
			if (remaining == 0 && writeCompleteCallback)
			{
				loop->queueInLoop(std::bind(writeCompleteCallback,shared_from_this()));
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
		  	loop->queueInLoop(std::bind(highWaterMarkCallback,shared_from_this(),oldLen + remaining));
		}
		
		sendBuff.append(static_cast<const char*>(data) + nwrote,remaining);
		if (!channel->isWriting())
		{
		  	channel->enableWriting();
		}
	}
}

void TcpConnection::connectEstablished()
{
	loop->assertInLoopThread();
	assert(state == kConnecting);
	setState(kConnected);
	channel->setTie(shared_from_this());
	channel->enableReading();
	connectionCallback(shared_from_this());
}

void TcpConnection::connectDestroyed()
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


