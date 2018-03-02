#pragma once
#include "all.h"

class xSocket : noncopyable
{
public:
	xSocket();
	xSocket(const std::string &ip, int16_t port);
	~xSocket();

	bool getpeerName(int32_t sockfd,std::string *ip, int16_t &port);
	int  createSocket();
	int  connect(int sockfd,const std::string &ip, int16_t port);

	int    getListenFd();
	void  setkeepAlive(int fd,int idle);
	bool  createTcpListenSocket(const std::string &ip,int16_t port);
	bool  setSocketNonBlock(int sockfd);
	bool  setSocketBlock(int sockfd);
	bool  setTcpNoDelay(int sockfd,bool on);
	bool  setTimeOut(int sockfd,const struct timeval tc);
	int    setFlag(int fd, int flag);

private:
	int  listenSocketFd;
	int  onlineNumber;
	bool protocol;

};

