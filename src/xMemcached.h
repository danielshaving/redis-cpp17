#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xTcpServer.h"
#include "xZmalloc.h"
#include "xObject.h"
#include "xItem.h"
#include "xSession.h"


class xMemcachedServer : noncopyable
{
public:
	struct Options
	{
		uint16_t port;
		std::string ip;
	};

	xMemcachedServer(xEventLoop *loop,const Options &op);
	~xMemcachedServer();

	void setThreadNum();
	void quit(const std::any &context);
	void start();
	void stop();
	void run();

	void onConnection(const xTcpconnectionPtr &conn);
	time_t getStartTime() const { return startTime; }

	bool storeItem(const xItemPtr &item, xItem::UpdatePolicy policy, bool *exists);
	xConstItemPtr getItem(const xConstItemPtr &key) const;
	bool deleteItem(const xConstItemPtr &key);

private:
	xEventLoop *loop;
	std::unordered_map<int32_t,xSessionPtr>  sessionMaps;
	Options ops;

	std::mutex mtx;
	std::atomic<int64_t> cas;
	const time_t startTime;

	struct xHash
	{
		size_t operator()(const xConstItemPtr &x) const
		{
			return x->getHash();
		}
	};

	struct xEqual
	{
		bool operator()(const xConstItemPtr &x,const xConstItemPtr &y) const
		{
			return x->getKey() == y->getKey();
		}
	};

	typedef std::unordered_set<xConstItemPtr,xHash,xEqual> ItemMap;

	struct MapWithLock
	{
		ItemMap items;
		mutable std::mutex mutex;
	};

	const static int kShards = 4096;
	std::array<MapWithLock,kShards> shards;
	xTcpServer server;

};
