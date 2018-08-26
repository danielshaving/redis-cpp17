#pragma once
#include "all.h"
#include "object.h"
#include "tcpclient.h"
#include "socket.h"
#include "session.h"

struct ClusterNode
{
	std::string ip;
	std::string name;
	std::string flag;
	int16_t port;
	int64_t createTime;
	uint64_t configEpoch;
	struct ClusterNode *slaves;
	struct ClusterNode *master;
};

class Redis;
class Cluster
{
public:
	Cluster(Redis *redis);
	~Cluster();

	void clear();
	bool connSetCluster(const char *ip, int16_t port);
	void connectCluster();
	void readCallback(const TcpConnectionPtr &conn, Buffer *buffer);
	void connCallback(const TcpConnectionPtr &conn);
	void reconnectTimer(const std::any &context);
	void cretateClusterNode(int32_t slot, const std::string &ip,
		int16_t port, const std::string &name);
	bool getKeySlot(const std::string &name);
	void structureProtocolSetCluster(std::string host,
		int16_t port, Buffer &buffer, const TcpConnectionPtr &conn);
	int32_t getSlotOrReply(const SessionPtr &session,
		const RedisObjectPtr &o, const TcpConnectionPtr &conn);
	uint32_t keyHashSlot(char *key, int32_t keylen);

	void syncClusterSlot();
	void clusterRedirectClient(const TcpConnectionPtr &conn, const SessionPtr &session,
		ClusterNode *node, int32_t hashSlot, int32_t errCode);
	bool replicationToNode(const std::deque<RedisObjectPtr> &obj,
		const SessionPtr &session, const std::string &ip,
		int16_t port, int8_t copy, int8_t replace, int32_t numKeys, int32_t firstKey);
	void delClusterImport(std::deque<RedisObjectPtr> &robj);

	void eraseClusterNode(const std::string &ip, int16_t port);
	void eraseClusterNode(int32_t slot);
	void getKeyInSlot(int32_t slot, std::vector<RedisObjectPtr> &keys, int32_t count);

	ClusterNode *checkClusterSlot(int32_t slot);
	sds showClusterNodes();
	void delSlotDeques(const RedisObjectPtr &obj, int32_t slot);
	void addSlotDeques(const RedisObjectPtr &slot, std::string name);

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
	Cluster(const Cluster&);
	void operator=(const Cluster&);

	EventLoop *loop;
	Redis *redis;
	std::atomic<bool> state;
	std::atomic<bool> isConnect;
	std::vector<TcpClientPtr> clusterConns;
	std::map<int32_t, ClusterNode> clusterSlotNodes;
	std::unordered_map<std::string, std::unordered_set<int32_t>> migratingSlosTos;
	std::unordered_map<std::string, std::unordered_set<int32_t>> importingSlotsFroms;
	std::vector<RedisObjectPtr> clusterDelKeys;
	std::vector<RedisObjectPtr> clusterDelCopys;
	std::condition_variable condition;
	std::atomic<int32_t> replyCount;
	std::deque<RedisObjectPtr> redisCommands;
	std::unordered_set<int32_t> slotSets;
	Buffer buffer;

};
