#pragma once

#include "xAll.h"
#include "xTcpServer.h"
#include "xLog.h"
#include "xTcpConnection.h"
#include <boost/circular_buffer.hpp>

class xTimeWheel
{
public:
	xTimeWheel(xEventLoop *loop,const char *ip,int16_t port,int8_t idleSeconds);

private:
	void onConnection(const TcpConnectionPtr &conn);
	void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	void onTimer();
	void dumpConnectionBuckets() const;
	typedef std::weak_ptr<xTcpConnection> WeakTcpConnectionPtr;

	struct Entry
	{
		explicit Entry(const WeakTcpConnectionPtr &weakConn)
		: weakConn(weakConn)
		{

		}

		~Entry()
		{
			TcpConnectionPtr conn = weakConn.lock();
			if (conn)
			{
				conn->shutdown();
			}
		}

		WeakTcpConnectionPtr weakConn;
	};

	xEventLoop *loop;
	typedef std::shared_ptr<Entry> EntryPtr;
	typedef std::weak_ptr<Entry> WeakEntryPtr;
	typedef std::unordered_set<EntryPtr> Bucket;
	typedef boost::circular_buffer<Bucket> WeakConnectionList;

	xTcpServer server;
	WeakConnectionList connectionBuckets;
	int8_t idleSeconds;
};
