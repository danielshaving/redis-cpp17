#include "all.h"
#include "xSocket.h"
#include "xLog.h"


xSocket::xSocket(const std::string &ip, int16_t port)
{
	listenSocketFd = -1;
	onlineNumber = 0;
	createTcpListenSocket(ip,port);
}

xSocket::xSocket()
{
	listenSocketFd = -1;
	onlineNumber = 0;
}

xSocket::~xSocket()
{
	::close(listenSocketFd);
}


int  xSocket::getListenFd()
{
	return listenSocketFd;
}


bool xSocket::getpeerName(int32_t fd,std::string *ip, int16_t &port)
{
	struct sockaddr_in sa;
	socklen_t len = sizeof(sa);
	if(!getpeername(fd, (struct sockaddr *)&sa, &len))
	{
		char * inIp = inet_ntoa(sa.sin_addr);
		*ip = inIp;
		port = ntohs(sa.sin_port);
		return true;
	}
	else
	{
		return false;
	}

}

int  xSocket::createSocket()
{
	return socket(AF_INET, SOCK_STREAM, 0);
}

int  xSocket::connect(int sockfd,const std::string &ip, int16_t port)
{
	struct sockaddr_in sin;
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = inet_addr(ip.c_str());

	return  ::connect(sockfd, (struct sockaddr *)&sin, sizeof(sin));
}



int xSocket::setFlag(int fd, int flag)
{
	   int ret = fcntl(fd, F_GETFD);
	   return fcntl(fd, F_SETFD, ret | flag);
}

bool xSocket::setTimeOut(int sockfd,const struct timeval tv)
{
    if (setsockopt(sockfd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv)) == -1)
    {
        LOG_ERROR<<"setsockopt(SO_RCVTIMEO)";
        return false;
    }

    if (setsockopt(sockfd,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv)) == -1)
    {
        LOG_ERROR<<"setsockopt(SO_SNDTIMEO)";
        return false;
    }
	
    return true;
}

void  xSocket::setkeepAlive(int fd,int idle)
{
#ifdef __linux__
	int keepalive = 1;
	int keepidle = idle;
	int keepintvl = 2;
	int keepcnt = 3;
	int err = 0;

	if(setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive, sizeof(keepalive)) < 0)
	{
		LOG_DEBUG<<"SOL_SOCKET";
	}

	if(setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, (char *)&keepidle, sizeof(keepidle)) < 0)
	{
		LOG_DEBUG<<"TCP_KEEPIDLE";
	}

	if(setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL,(char *)&keepintvl, sizeof(keepintvl)) < 0)
	{
		LOG_DEBUG<<"TCP_KEEPINTVL";
	}

	if(setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT,(char *)&keepcnt, sizeof(keepcnt)) < 0)
	{
		LOG_DEBUG<<"TCP_KEEPCNT";
	}
#endif
}

bool xSocket::createTcpListenSocket(const std::string &ip,int16_t port)
{
    struct sockaddr_in serverAdress;
    serverAdress.sin_family = AF_INET;
    serverAdress.sin_port   = htons(port);

    if (ip.length() > 0 )
    {
        serverAdress.sin_addr.s_addr = inet_addr(ip.c_str());
    }
    else
    {
        serverAdress.sin_addr.s_addr = htonl(INADDR_ANY);
    }

    listenSocketFd = socket(AF_INET, SOCK_STREAM, 0);

    if (listenSocketFd < 0)
    {
        LOG_WARN<<"Create Tcp Socket Failed! "<< strerror(errno);
        return false;
    }

    if (!setSocketNonBlock(listenSocketFd))
    {
		LOG_WARN<<"Set listen socket  to non-block failed!";
		return false;
    }

    int optval = 1;
	
	if (setsockopt(listenSocketFd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) < 0)
    {
		LOG_SYSERR<<"Set SO_REUSEPORT socket  failed! error "<<strerror(errno);
        close(listenSocketFd);
        return false;
    }
	
  
    if (setsockopt(listenSocketFd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0)
    {
		LOG_SYSERR<<"Set SO_REUSEADDR socket  failed! error "<<strerror(errno);
		close(listenSocketFd);
        return false;
    }

    if (bind(listenSocketFd, (struct sockaddr*)&serverAdress, sizeof(serverAdress)) < 0 )
    {
        LOG_SYSERR<<"Bind bind socket failed! error "<<strerror(errno);
        close(listenSocketFd);
        return false;
    }

    if (listen(listenSocketFd, SOMAXCONN))
    {
        LOG_SYSERR<<"Listen listen socket failed! error "<<strerror(errno);
        close(listenSocketFd);
        return false;
    }

#ifdef __linux__
    setsockopt(listenSocketFd, IPPROTO_TCP, TCP_NODELAY,&optval, static_cast<socklen_t>(sizeof optval));

    int len = 65536;
    setsockopt(listenSocketFd, SOL_SOCKET, SO_RCVBUF, (void*)&len, sizeof(len));
    setsockopt(listenSocketFd, SOL_SOCKET, SO_SNDBUF, (void*)&len, sizeof(len));
#endif
    return true;
}



bool xSocket::setTcpNoDelay(int socketFd, bool on)
{
#ifdef __linux__
	int optval = on ? 1 : 0;
	::setsockopt(socketFd, IPPROTO_TCP, TCP_NODELAY,&optval, static_cast<socklen_t>(sizeof optval));
#endif
}

bool  xSocket::setSocketBlock(int socketFd)
{
    int opt = fcntl(socketFd, F_GETFL);
    if (opt < 0)
    {
        LOG_WARN<<"fcntl F_GETFL) failed! error"<<strerror(errno);
        return false;
    }

    opt = opt &~ O_NONBLOCK;
    if (fcntl(socketFd, F_SETFL, opt) < 0)
    {
    	 LOG_WARN<<"fcntl F_GETFL) failed! error"<<strerror(errno);
        return false;
    }
}


bool xSocket::setSocketNonBlock(int socketFd)
{
    int opt = fcntl(socketFd, F_GETFL);
    if (opt < 0)
    {
        LOG_WARN<<"fcntl F_GETFL) failed! error"<<strerror(errno);
        return false;
    }

    opt = opt | O_NONBLOCK;
    if (fcntl(socketFd, F_SETFL, opt) < 0)
    {
    	 LOG_WARN<<"fcntl F_GETFL) failed! error"<<strerror(errno);
        return false;
    }

    return true;
}



