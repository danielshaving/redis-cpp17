#pragma once
#include "xAll.h"
#include "xTcpConnection.h"
#include "xTcpServer.h"
#include "xZmalloc.h"
#include "xObject.h"
#include "xItem.h"
#include "xSession.h"

class xMemcached : noncopyable
{
public:
	struct Options
	{
		uint16_t port;
		std::string ip;
	};

	xMemcached(xEventLoop *loop,const Options &op);
	~xMemcached();

	void setThreadNum();
	void quit();
	void start();
	void stop();
	void run();

	void onConnection(const TcpConnectionPtr &conn);
	time_t getStartTime() const { return startTime; }

	bool storeItem(const ItemPtr &item,xItem::UpdatePolicy policy,bool *exists);
	ConstItemPtr getItem(const ConstItemPtr &key) const;
	bool deleteItem(const ConstItemPtr &key);

private:
	xEventLoop *loop;
	std::unordered_map<int32_t,SessionPtr> sessions;
	Options ops;

	std::mutex mtx;
	std::atomic<int64_t> cas;
	const time_t startTime;

	struct xHash
	{
		size_t operator()(const ConstItemPtr &x) const
		{
			return x->getHash();
		}
	};

	struct xEqual
	{
		bool operator()(const ConstItemPtr &x,const ConstItemPtr &y) const
		{
			return x->getKey() == y->getKey();
		}
	};

	typedef std::unordered_set<ConstItemPtr,xHash,xEqual> ItemMap;

	struct MapWithLock
	{
		ItemMap items;
		mutable std::mutex mutex;
	};

	const static int kShards = 4096;
	std::array<MapWithLock,kShards> shards;
	xTcpServer server;
};
