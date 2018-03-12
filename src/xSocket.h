#pragma once
#include "all.h"

class xSocket : noncopyable
{
public:
	xSocket();
	xSocket(const std::string &ip, int16_t port);
	~xSocket();

	bool getpeerName(int32_t sockfd,std::string *ip, int16_t &port);
	int32_t createSocket();
	int32_t connect(int32_t sockfd,const std::string &ip, int16_t port);

	int32_t getListenFd();
	void setkeepAlive(int32_t fd,int32_t idle);
	bool createTcpListenSocket(const std::string &ip,int16_t port);
	bool setSocketNonBlock(int32_t sockfd);
	bool setSocketBlock(int32_t sockfd);
	bool setTcpNoDelay(int32_t sockfd,bool on);
	bool setTimeOut(int32_t sockfd,const struct timeval tc);
	int32_t setFlag(int32_t fd, int32_t flag);

private:
	int32_t  listenSocketFd;
	int32_t  onlineNumber;
	bool protocol;

};

