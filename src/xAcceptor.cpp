#include "xAcceptor.h"
#include "xLog.h"

 xAcceptor::xAcceptor(xEventLoop* loop,std::string ip, int16_t port)
 :loop(loop),
  socket(loop,ip,port),
  channel(loop,socket.getListenFd()),
  listenfd(socket.getListenFd()),
  listenning(false)
 {
	 channel.setReadCallback(std::bind(&xAcceptor::handleRead,this));
 }

 xAcceptor::~xAcceptor()
 {
	 channel.disableAll();
	 channel.remove();
	::close(listenfd);
 }

 void xAcceptor::handleRead()
 {
	loop->assertInLoopThread();
	struct sockaddr_in address;
	socklen_t   len  = sizeof( address );

	int connfd = ::accept( listenfd, ( struct sockaddr* )&address, &len);
	if (connfd >= 0)
	{
		if (newConnectionCallback)
		{
			socket.setSocketNonBlock(connfd);
			newConnectionCallback(connfd);
		}
		else
		{
			::close(connfd);
			LOG_WARN<<"handleRead";
		}
	}
	else
	{
		 LOG_SYSERR << "in xAcceptor::handleRead";
	}
 }



 void xAcceptor::listen()
 {
	loop->assertInLoopThread();
	listenning = true;
	channel.enableReading();
 }
