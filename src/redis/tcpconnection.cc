#include "eventloop.h"
#include "tcpconnection.h"
#include "socket.h"

TcpConnection::TcpConnection(EventLoop *loop, int32_t sockfd, const std::any &context)
	:loop(loop),
	sockfd(sockfd),
	reading(true),
	state(kConnecting),
	channel(new Channel(loop, sockfd)),
	context(context)
{
	channel->setReadCallback(
		std::bind(&TcpConnection::handleRead, this));
	channel->setWriteCallback(
		std::bind(&TcpConnection::handleWrite, this));
	channel->setCloseCallback(
		std::bind(&TcpConnection::handleClose, this));
	channel->setErrorCallback(
		std::bind(&TcpConnection::handleError, this));
}

TcpConnection::~TcpConnection()
{
	assert(state == kDisconnected);
	Socket::close(sockfd);
}

void TcpConnection::shutdown()
{
	if (state == kConnected)
	{
		setState(kDisconnecting);
		loop->runInLoop(std::bind(&TcpConnection::shutdownInLoop, this));
	}
}

void TcpConnection::setState(StateE s)
{
	state = s;
}

void TcpConnection::forceClose()
{
	if (state == kConnected || state == kDisconnecting)
	{
		setState(kDisconnecting);
		loop->queueInLoop(std::bind(&TcpConnection::forceCloseInLoop, shared_from_this()));
	}
}

void TcpConnection::forceCloseInLoop()
{
	loop->assertInLoopThread();
	if (state == kConnected || state == kDisconnecting)
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
#ifdef _WIN64
		if (::shutdown(sockfd, SD_SEND) < 0)
		{
			LOG_WARN << "sockets::shutdownWrite";
		}
#else
		if (::shutdown(sockfd, SHUT_WR) < 0)
		{
			LOG_WARN << "sockets::shutdownWrite";
		}
#endif
	}
}

void TcpConnection::handleRead()
{
	loop->assertInLoopThread();
	int saveErrno = 0;
	ssize_t n = readBuffer.readFd(channel->getfd(), &saveErrno);
	if (n > 0)
	{
		messageCallback(shared_from_this(), &readBuffer);
	}
#ifdef _WIN64
	else if (n == 0 || saveErrno == WSAECONNRESET)
	{
		handleClose();
	}
	else
	{
		errno = saveErrno;
		handleError();
	}
#else
	else if (n == 0)
	{
		handleClose();
	}
	else
	{
		errno = saveErrno;
		if (errno != ECONNRESET || errno != ETIMEDOUT)
		{

		}
	}
#endif
}

void TcpConnection::handleWrite()
{
	loop->assertInLoopThread();
	if (state == kDisconnected)
	{
		return;
	}

	if (channel->isWriting())
	{
#ifdef _WIN64
		ssize_t n = ::send(channel->getfd(), writeBuffer.peek(), writeBuffer.readableBytes(), 0);
#else
		ssize_t n = ::write(channel->getfd(), writeBuffer.peek(), writeBuffer.readableBytes());
#endif
		if (n > 0)
		{
			writeBuffer.retrieve(n);
			if (writeBuffer.readableBytes() == 0)
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
	loop->runInLoop(std::bind(&TcpConnection::startReadInLoop, this));
}

void TcpConnection::stopRead()
{
	loop->runInLoop(std::bind(&TcpConnection::stopReadInLoop, this));
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
		loop->runAfter(seconds, false, std::bind(&TcpConnection::forceCloseDelay, shared_from_this()));
	}
}

void TcpConnection::handleError()
{

}

void TcpConnection::sendInLoopPipe()
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendPipe();
		}
		else
		{
			void (TcpConnection::*fp)() = &TcpConnection::sendPipe;
			loop->runInLoop(std::bind(fp, this));
		}
	}
}

void TcpConnection::sendPipe()
{
	loop->assertInLoopThread();
	if (!channel->isNoneEvent())
	{
		if (!channel->isWriting())
		{
			channel->enableWriting();
		}
	}
}

void TcpConnection::sendPipe(Buffer *buf)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendPipeInLoop(buf->peek(), buf->readableBytes());
		}
		else
		{
			void (TcpConnection::*fp)(const std::string_view &message) = &TcpConnection::sendPipeInLoop;
			loop->runInLoop(std::bind(fp, this, buf->retrieveAllAsString()));
		}
	}
}

void TcpConnection::sendPipe(const void *message, int len)
{
	sendPipe(std::string_view(static_cast<const char*>(message), len));
}

void TcpConnection::sendPipe(const std::string_view &message)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendPipeInLoop(message.data(), message.size());
		}
		else
		{
			void (TcpConnection::*fp)(const std::string_view &message) = &TcpConnection::sendPipeInLoop;
			loop->runInLoop(std::bind(fp, this, std::string(message)));
		}
	}
}

void TcpConnection::send(const void *message, int len)
{
	send(std::string_view(static_cast<const char*>(message), len));
}

void TcpConnection::send(const std::string_view &message)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendInLoop(message.data(), message.size());
		}
		else
		{
			void (TcpConnection::*fp)(const std::string_view &message) = &TcpConnection::sendInLoop;
			loop->runInLoop(std::bind(fp, this, std::string(message)));
			//loop->runInLoop(std::bind(&bindSendInLoop,this,std::string(message)));
		}
	}
}

void TcpConnection::sendPipeInLoop(const std::string_view &message)
{
	sendPipeInLoop(message.data(), message.size());
}

void TcpConnection::sendPipeInLoop(const void *message, size_t len)
{
	writeBuffer.append(message, len);
	if (!channel->isNoneEvent())
	{
		if (!channel->isWriting())
		{
			channel->enableWriting();
		}
	}
}

void TcpConnection::bindSendPipeInLoop(TcpConnection *conn, const std::string_view &message)
{
	conn->sendPipeInLoop(message.data(), message.size());
}

void TcpConnection::bindSendInLoop(TcpConnection *conn, const std::string_view &message)
{
	conn->sendInLoop(message.data(), message.size());
}

void TcpConnection::send(Buffer *buf)
{
	if (state == kConnected)
	{
		if (loop->isInLoopThread())
		{
			sendInLoop(buf->peek(), buf->readableBytes());
		}
		else
		{
			loop->runInLoop(std::bind(&bindSendInLoop, this, buf->retrieveAllAsString()));
		}
	}
}

void TcpConnection::sendInLoop(const std::string_view &message)
{
	sendInLoop(message.data(), message.size());
}

void TcpConnection::sendInLoop(const void *data, size_t len)
{
	loop->assertInLoopThread();
	ssize_t nwrote = 0;
	size_t remaining = len;
	bool faultError = false;

	if (state == kDisconnected)
	{
		LOG_WARN << "disconnected, give up writing";
		return;
	}

	if (!channel->isWriting() && writeBuffer.readableBytes() == 0)
	{
#ifdef _WIN64
		nwrote = ::send(channel->getfd(), (const char *)data, len, 0);
#else
		nwrote = ::write(channel->getfd(), data, len);
#endif
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

		assert(remaining <= len);
		if (!faultError && remaining > 0)
		{
			size_t oldLen = writeBuffer.readableBytes();
			if (oldLen + remaining >= highWaterMark
				&& oldLen < highWaterMark
				&& highWaterMarkCallback)
			{
				loop->queueInLoop(std::bind(highWaterMarkCallback, shared_from_this(), oldLen + remaining));
			}

			writeBuffer.append(static_cast<const char*>(data) + nwrote, remaining);
			if (!channel->isWriting())
			{
				channel->enableWriting();
			}
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


