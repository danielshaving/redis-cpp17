#pragma once
#include "xAll.h"
#include "xObject.h"
#include "xTcpClient.h"
#include "xSocket.h"
#include "xSession.h"

struct clusterNode
{
	std::string ip;
	std::string name;
	std::string flag;
	int16_t port;
	int64_t createTime;
	uint64_t configEpoch;
	struct clusterNode *slaves;
	struct clusterNode *master;
};

class xRedis;
class xCluster : noncopyable
{
public:
	xCluster(xRedis *redis);
	~xCluster();

    void clear();
	bool connSetCluster(const char *ip,int16_t port);
	void connectCluster();
	void readCallBack(const TcpConnectionPtr &conn,xBuffer *buffer);
	void connCallBack(const TcpConnectionPtr &conn);
	void reconnectTimer(const std::any &context);
	void cretateClusterNode(int32_t slot,const std::string &ip,int16_t port,const std::string &name);

	bool getKeySlot(const std::string &name);
	void structureProtocolSetCluster(const std::string &host,int16_t port,xBuffer &buffer,const TcpConnectionPtr &conn);
	int32_t getSlotOrReply(const SessionPtr &session,rObj *o);
	uint32_t keyHashSlot(char *key,int32_t keylen);
	void syncClusterSlot();
	void clusterRedirectClient(const SessionPtr &session,clusterNode *node,int32_t hashSlot,int32_t errCode);
	bool replicationToNode(const std::deque<rObj*> &obj,const SessionPtr &session,
		const std::string &ip,int16_t port,int8_t copy,int8_t replace,int32_t numKeys,int32_t firstKey);
	void delClusterImport(std::deque<rObj*> &robj);
	
	void eraseClusterNode(const std::string &ip,int16_t port);
	void eraseClusterNode(int32_t slot);
	void getKeyInSlot(int32_t slot,std::vector<rObj*> &keys,int32_t count);

	clusterNode *checkClusterSlot(int32_t slot);
	sds showClusterNodes();
	void delSlotDeques(rObj *obj,int32_t slot);
	void addSlotDeques(rObj *slot,const std::string &name);

	auto &getMigrating() { return migratingSlosTos; } 
	auto &getImporting() { return importingSlotsFroms; }
	auto &getClusterNode() { return clusterSlotNodes; }
	
	size_t getImportSlotSize() { return importingSlotsFroms.size(); }
	size_t getMigratSlotSize() { return migratingSlosTos.size(); }
	
	void clearMigrating() { migratingSlosTos.clear(); }
	void clearImporting() { importingSlotsFroms.clear(); }

	void eraseMigratingSlot(const std::string &name);
	void eraseImportingSlot(const std::string &name);
	
private:
	xEventLoop *loop;
	xRedis *redis;
	xSocket socket;
	bool state;
	bool isConnect;
	std::vector<TcpClientPtr> clusterConns;
	std::map<int32_t,clusterNode> clusterSlotNodes;
	std::unordered_map<std::string, std::unordered_set<int32_t>> migratingSlosTos;
	std::unordered_map<std::string, std::unordered_set<int32_t>> importingSlotsFroms;
	std::vector<rObj*> clusterDelKeys;
	std::vector<rObj*> clusterDelCopys;
	std::condition_variable condition;
	std::mutex mtx;
	std::atomic<int32_t> replyCount;
	std::deque<rObj*> commands;
	std::unordered_set<int32_t> slotSets;
	xBuffer buffer;
	
};
