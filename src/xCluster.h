#pragma once
#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"

class xRedis;
class xCluster : noncopyable
{
public:
	xCluster();
	~xCluster();
	void init();
	void connSetCluster(std::string ip, int32_t port, xRedis * redis);
	void connectCluster();
	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf, void *data);
	void connCallBack(const xTcpconnectionPtr& conn, void *data);
	void reconnectTimer(void * data);
	void structureProtocolSetCluster(std::string host, int32_t port, xBuffer &sendBuf, std::deque<rObj*> &robjs, const xTcpconnectionPtr & conn);
	
private:
	xEventLoop *loop;
	xTcpClient *client;
	xRedis *redis;
	xSocket socket;
	std::vector<std::shared_ptr<xTcpClient>> tcpvectors;
};
