#include "xConnector.h"

xConnector::xConnector(xEventLoop *loop,std::string ip,int port)
 :loop(loop),
  ip(ip),
  port(port),
  state(kDisconnected),
  isconnect(false)
{

}


xConnector::~xConnector()
{

}


void xConnector::start()
{
  isconnect = true;
  loop->runInLoop(std::bind(&xConnector::startInLoop, this));
}



void xConnector::startInLoop()
{
  loop->assertInLoopThread();
  //assert(state == kDisconnected);
  if (isconnect)
  {
    connect();
  }
  else
  {
    LOG_WARN<<"do not connect";
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
    int sockfd = removeAndResetChannel();
    //retry(sockfd);
  }
}

int  xConnector::removeAndResetChannel()
{
	return -1;
}

void xConnector::connecting(int sockfd)
{
	if(state == kConnecting)
	{
		newConnectionCallback(sockfd);
	}
	else
	{
		//TRACE("connect error\n");
	}
}


void xConnector::connect()
{
  int sockfd = socket.createNonBloackSocket();
  int ret = socket.connect(sockfd, ip,port);
  int savedErrno = (ret == 0) ? 0 : errno;
  switch (savedErrno)
  {
    case 0:
    case EINPROGRESS:
    case EINTR:
    case EISCONN:
      setState(kConnecting);
      connecting(sockfd);
      socket.setSocketNonBlock(sockfd);
      break;
    default:
//
//    case EAGAIN:
//    case EADDRINUSE:
//    case EADDRNOTAVAIL:
//    case ECONNREFUSED:
//    case ENETUNREACH:
//    case EACCES:
//    case EPERM:
//    case EAFNOSUPPORT:
//    case EALREADY:
//    case EBADF:
//    case EFAULT:
//    case ENOTSOCK:
      LOG_WARN<<"Connect savedErrno "<<savedErrno;
      ::close(sockfd);
      setState(kDisconnected);
      errorConnectionCallback();
      break;
  }
}
