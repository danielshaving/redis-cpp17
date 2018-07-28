#pragma once
#include "all.h"

class Socket
{
public:
	Socket();
	Socket(const char *ip,int16_t port);
	~Socket();

	static uint64_t hostToNetwork64(uint64_t host64); 
	static uint32_t hostToNetwork32(uint32_t host32); 
	static uint16_t hostToNetwork16(uint16_t host16);
	static uint64_t networkToHost64(uint64_t net64);
	static uint32_t networkToHost32(uint32_t net32); 
	static uint16_t networkToHost16(uint16_t net16); 

	struct sockaddr_in6 getPeerAddr(int32_t sockfd);
	struct sockaddr_in6 getLocalAddr(int32_t sockfd);
	void toIpPort(char *buf,size_t size,const struct sockaddr *addr);
	void toIp(char *buf,size_t size,const struct sockaddr *addr);
	void toPort(uint16_t *port,const struct sockaddr *addr);

	void fromIpPort(const char *ip,uint16_t port,struct sockaddr_in *addr);
	void fromIpPort(const char *ip,uint16_t port,struct sockaddr_in6 *addr);

	int32_t createSocket();
	int32_t connect(int32_t sockfd,struct sockaddr *sin);
	int32_t connect(int32_t sockfd,const char *ip,int16_t port);

	bool isSelfConnect(int32_t sockfd);
	int32_t getSocketError(int32_t sockfd);
	int32_t getListenFd();
	void setkeepAlive(int32_t fd,int32_t idle);
	bool createTcpSocket(const char *ip,int16_t port);
	bool setSocketNonBlock(int32_t sockfd);
	bool setSocketBlock(int32_t sockfd);
	bool setTcpNoDelay(int32_t sockfd,bool on);
	bool setTimeOut(int32_t sockfd,const struct timeval tc);
	void setReuseAddr(bool on);
	void setReusePort(bool on);
	int32_t setFlag(int32_t fd,int32_t flag);
	bool connectWaitReady(int32_t fd,int32_t msec);

private:
	Socket(const Socket&);
	void operator=(const Socket&);

	int32_t sockfd;
};

