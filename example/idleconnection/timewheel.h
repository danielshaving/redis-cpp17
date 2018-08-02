#pragma once
#include "all.h"
#include "tcpserver.h"
#include "log.h"
#include "tcpconnection.h"
#include <boost/circular_buffer.hpp>

class TimeWheel : boost::noncopyable
{
public:
	TimeWheel(EventLoop *loop,const char *ip,int16_t port,int8_t idleSeconds);
	void onConnection(const TcpConnectionPtr &conn);
	void onMessage(const TcpConnectionPtr &conn,Buffer *buffer);
	void onTimer();
	void dumpConnectionBuckets() const;
	typedef std::weak_ptr<TcpConnection> WeakTcpConnectionPtr;

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

private:
	EventLoop *loop;
	typedef std::shared_ptr<Entry> EntryPtr;
	typedef std::weak_ptr<Entry> WeakEntryPtr;
	typedef std::unordered_set<EntryPtr> Bucket;
	typedef boost::circular_buffer<Bucket> WeakConnectionList;

	TcpServer server;
	WeakConnectionList connectionBuckets;
	int8_t idleSeconds;
};
