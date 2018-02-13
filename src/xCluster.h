#pragma once
#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xSession.h"

struct xClusterNode
{
	std::string ip;
	std::string name;
	std::string flag;
	int16_t port;
	int64_t createTime;
	uint64_t configEpoch;
	struct xClusterNode *slaves;
	struct xClusterNode *master;
};


class xRedis;
class xCluster : noncopyable
{
public:
	xCluster(xRedis * redis);
	~xCluster();

    void clear();
	bool connSetCluster(const char *ip, int16_t port);
	void connectCluster();
	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf);
	void connCallBack(const xTcpconnectionPtr& conn);
	void reconnectTimer(const std::any &context);
	bool getSlotSet(const std::string &ipPort);
	void cretateClusterNode(int32_t slot,const std::string &ip,int16_t port,const std::string &name);

	void structureProtocolSetCluster(std::string host, int16_t port, xBuffer &sendBuf, const xTcpconnectionPtr &conn);
	int32_t getSlotOrReply(const xSeesionPtr &session,rObj *o );
	uint32_t keyHashSlot(char *key, int32_t keylen);
	void syncClusterSlot();
	void clusterRedirectClient(const xSeesionPtr &session, xClusterNode * node,int32_t hashSlot,int32_t errCode);
	bool replicationToNode(const xSeesionPtr &session,const std::string &ip,int16_t port);
	void delClusterImport(std::deque<rObj*> &robj);
	
	void eraseClusterNode(const std::string &ip,int16_t port);
	void eraseImportSlot(int32_t slot);
	void eraseClusterNode(int32_t slot);
	void getKeyInSlot(int32_t slot, std::vector<rObj*> &keys , int32_t count);

	xClusterNode  *checkClusterSlot(int32_t slot);
	sds showClusterNodes();
	void delSlotDeques(rObj * obj,int32_t slot);
	void addSlotDeques(rObj * slot,const std::string &name);

	auto  & getMigrating() {  return migratingSlosTos; } 
	auto  & getImporting() { return importingSlotsFrom; }
	
	void clearMigrating() { migratingSlosTos.clear(); }
	void clearImporting() { importingSlotsFrom.clear(); }

	void eraseMigratingNode(const std::string &name);
	void eraseImportingNode(const std::string &name);
	
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
	std::atomic<int32_t> replyCount;
	std::deque<rObj*> deques;
	xBuffer sendBuf;
	std::unordered_set<int32_t>  uset;
};
