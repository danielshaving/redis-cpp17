#pragma once
#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xSession.h"



struct xClusterNode
{
	std::string ip;
	int16_t port;
};


class xRedis;
class xCluster : noncopyable
{
public:
	xCluster();
	~xCluster();
	void init();
	void connSetCluster(std::string ip, int16_t port, xRedis * redis);
	void connectCluster();
	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf, void *data);
	void connCallBack(const xTcpconnectionPtr& conn, void *data);
	void reconnectTimer(void * data);
	void structureProtocolSetCluster(std::string host, int16_t port, xBuffer &sendBuf, std::deque<rObj*> &robjs, const xTcpconnectionPtr & conn);
	int getSlotOrReply(xSession  * session,rObj * o );
	unsigned int keyHashSlot(char *key, int keylen);
	void syncClusterSlot(std::deque<rObj*> &robj);
	void clusterRedirectClient(xSession * session, xClusterNode * node,int hashSlot,int errCode);

public:
	xEventLoop *loop;
	xRedis *redis;
	xSocket socket;
	std::vector<std::shared_ptr<xTcpClient>> tcpvectors;	
	std::unordered_map<int16_t, xClusterNode> clusterSlotNodes;
	std::unordered_map<std::string, std::unordered_set<int32_t>> migratingSlosTos;
	std::unordered_map<std::string, std::unordered_set<int32_t>> importingSlotsFrom;

};
