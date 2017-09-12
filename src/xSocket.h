#pragma once
#include "all.h"

class xEventLoop;
class xSocket: noncopyable
{
public:
    typedef std::function<void (int sockfd,const std::string &ip, int16_t port)> NewConnectionCallback;
	xSocket();
	xSocket(xEventLoop *loop,std::string ip, int16_t port);
	~xSocket();

	bool getpeerName(int32_t sockfd,std::string *ip, int32_t &port);
	int  createSocket();
	int  connect(int sockfd,std::string ip, int16_t port);

	bool  initialize();
	int   getListenFd();
	void  setkeepAlive(int fd,int idle);
	bool  createTcpListenSocket();
	bool  setSocketNonBlock(int sockfd);
	bool  setSocketBlock(int sockfd);
	bool  setTcpNoDelay(int sockfd,bool on);
	bool  setTimeOut(int sockfd,const struct timeval tc);
private:
	
	xEventLoop	 *loop;
	std::string 	ip;
	int32_t  port;

	int  listenSocketFd;
	int  maxSocketFd;
	int  onlineNumber;
	bool protocol;

};

