#pragma once
#include "all.h"
#include "tcpconnection.h"
#include "sds.h"
#include "timerqueue.h"

class RedisProxy;
class ProxySession : public std::enable_shared_from_this<ProxySession>
{
public:
	ProxySession(RedisProxy *redis, const TcpConnectionPtr &conn);
	~ProxySession();

	void proxyReadCallback(const TcpConnectionPtr &conn, Buffer *buffer);
	int32_t processMultibulkBuffer(const TcpConnectionPtr &conn, Buffer *buffer);
	int32_t processInlineBuffer(const TcpConnectionPtr &conn, Buffer *buffer);
	void reset();

	void timeWheelTimer();
	void dumpConnectionBuckets() const;

	struct Entry
	{
		Entry(const WeakTcpConnectionPtr &weakConn);
		~Entry();
		WeakTcpConnectionPtr weakConn;
	};

private:
	RedisProxy *redis;
	RedisObjectPtr command;
	TcpConnectionPtr conn;
	TimerPtr timer;
	typedef std::shared_ptr<Entry> EntryPtr;
	typedef std::weak_ptr<Entry> WeakEntryPtr;
	typedef std::unordered_set<EntryPtr> Bucket;
	typedef std::list<Bucket> WeakConnectionList;
	WeakConnectionList connectionBuckets;
	static const int32_t kBucketSize = 60;

	int32_t reqtype;
	int32_t multibulklen;
	int64_t bulklen;
	int32_t argc;
};
