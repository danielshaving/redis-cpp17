#pragma once
#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xSession.h"



struct xClusterNode
{
	std::string ip;
	int32_t  port;
};


class xRedis;
class xCluster : noncopyable
{
public:
	xCluster(){}
	xCluster(xRedis * redis);
	~xCluster();

        void clear();
	bool connSetCluster(const std::string &ip, int32_t port);
	void connectCluster();
	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf, void *data);
	void connCallBack(const xTcpconnectionPtr& conn, void *data);
	void reconnectTimer(void * data);
        bool getSlotSet(const std::string &ipPort);

	void structureProtocolSetCluster(std::string host, int32_t port, xBuffer &sendBuf, const xTcpconnectionPtr & conn);
	int getSlotOrReply(const xSeesionPtr &session,rObj * o );
	unsigned int keyHashSlot(char *key, int keylen);
	void syncClusterSlot();
	void clusterRedirectClient(const xSeesionPtr &session, xClusterNode * node,int hashSlot,int errCode);
	bool replicationToNode(const xSeesionPtr &session,const std::string &ip,int32_t port);
	void delClusterImport(std::deque<rObj*> &robj);
	void eraseClusterNode(const std::string &ip,int32_t port);
	void eraseImportSlot(int slot);
	void getKeyInSlot(int slot, rObj **keys, int count);


public:
	xEventLoop *loop;
	xRedis *redis;
	xSocket socket;
	bool state;
	bool isConnect;
	std::vector<std::shared_ptr<xTcpClient>> tcpvectors;	
	std::map<int32_t, xClusterNode> clusterSlotNodes;
	std::unordered_map<std::string, std::unordered_set<int32_t>> migratingSlosTos;
	std::unordered_map<std::string, std::unordered_set<int32_t>> importingSlotsFrom;
	std::condition_variable condition;
	std::mutex cmtex;
	std::atomic<int> replyCount;
	std::deque<rObj*> deques;
	xBuffer sendBuf;
	std::unordered_set<int32_t>  uset;
};
