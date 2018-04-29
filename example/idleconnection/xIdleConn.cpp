#pragma once
#include "xAll.h"
#include "xEventLoop.h"
#include "xTcpServer.h"
#include "xTimer.h"
#include "xTcpConnection.h"

class xEchoServer : boost::noncopyable
{
public:
	xEchoServer(xEventLoop *loop,const char *ip,int16_t port,int8_t idleSeconds)
	:loop(loop),
	server(loop,ip,port,nullptr),
	ip(ip),
	port(port),
	idleSeconds(idleSeconds)
	{
		server.setConnectionCallback(std::bind(&xEchoServer::onConnection,this,std::placeholders::_1));
		server.setMessageCallback(std::bind(&xEchoServer::onMessage,this,std::placeholders::_1,std::placeholders::_2));
		server.start();
		loop->runAfter(1.0,nullptr,true,std::bind(&xEchoServer::onTimer,this,std::placeholders::_1));
	}

private:
	void dumpConnectionList() const;
	void onConnection(const TcpConnectionPtr &conn);
	void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	void onTimer(const std::any &context);

	typedef std::weak_ptr<xTcpConnection> WeakTcpConnectionPtr;
	typedef std::list<WeakTcpConnectionPtr> WeakConnectionList;

	struct Node
	{
		xTimeStamp lastReceiveTime;
		WeakConnectionList::iterator position;
	};

	xEventLoop *loop;
	xTcpServer server;
	const char *ip;
	int16_t port;
	int8_t idleSeconds;
	WeakConnectionList connectionList;
};

void xEchoServer::onConnection(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		Node node;
		node.lastReceiveTime = xTimeStamp::now();
		connectionList.push_back(conn);
		node.position = --connectionList.end();
		conn->setContext(node);
	}
	else
	{
		 auto node = std::any_cast<Node>(conn->getContext());
		 connectionList.erase(node->position);
	}
}

void xEchoServer::onMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	std::string msg(buffer->retrieveAllAsString());
	conn->send(msg);
	auto node = std::any_cast<Node>(conn->getContext());
	node->lastReceiveTime = xTimeStamp::now();
	connectionList.splice(connectionList.end(),connectionList,node->position);
	assert(node->position == --connectionList.end());
}

void xEchoServer::dumpConnectionList() const
{
	for(auto &it : connectionList)
	{
		auto conn = it.lock();
		if (conn)
		{
			printf("conn %p\n",conn.get());
			auto node = std::any_cast<Node>(conn->getContext());
			printf("time %s\n", node->lastReceiveTime.toString().c_str());
		}
		else
		{
			printf("expired\n");
		}
	}
}

void xEchoServer::onTimer(const std::any &context)
{
	dumpConnectionList();
	xTimeStamp now = xTimeStamp::now();
	for(auto it = connectionList.begin(); it != connectionList.end();)
	{
		auto conn = it->lock();
		if(conn)
		{
			auto node = std::any_cast<Node>(conn->getContext());
			double age = timeDifference(now, node->lastReceiveTime);
			if (age > idleSeconds)
			{
				if (conn->connected())
				{
					printf("shutdown: %d\n",getpid());
					conn->shutdown();
					conn->forceCloseWithDelay(3.5);  // > round trip of the whole Internet.
				}
			}
			else if (age < 0)
			{
				node->lastReceiveTime = now;
			}
			else
			{
				break;
			}
			++it;
		}
		else
		{
			printf("expired\n");
			it = connectionList.erase(it);
		}
	}
}

int main(int argc, char* argv[])
{
	if(argc == 4)
	{
		const char *ip = argv[1];
		int16_t port = static_cast<uint16_t>(atoi(argv[2]));

		int8_t idleSeconds = static_cast<int8_t>(atoi(argv[3]));
		xEventLoop loop;
		xEchoServer server(&loop,ip,port,idleSeconds);
		loop.run();
	}
	else if (argc == 1)
	{
		xEventLoop loop;
		xEchoServer server(&loop,"0.0.0.0",8888,10);
		loop.run();
	}
	return 0;
}
