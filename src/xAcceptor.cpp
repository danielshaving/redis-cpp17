#include "xAcceptor.h"

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

	struct sockaddr_in client_address;
	socklen_t client_addrlength = sizeof( client_address );

	int connfd = accept( listenfd, ( struct sockaddr* )&client_address, &client_addrlength );
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
		  //TRACE("xAcceptor::handleRead");
		}
	}
	else
	{
		int savedErrno = errno;
		//TRACE("errno %d",errno);
	}
 }



 void xAcceptor::listen()
 {
	loop->assertInLoopThread();
	listenning = true;
	channel.enableReading();
 }
