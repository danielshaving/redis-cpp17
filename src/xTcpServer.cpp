#include "xTcpServer.h"
#include "xTcpConnection.h"

xTcpServer::xTcpServer(xEventLoop *loop,std::string ip,int16_t port,const std::any &context)
:loop(loop),
 acceptor(new xAcceptor(loop,ip,port)),
 threadPool(new xThreadPool(loop)),
 context(context)
{
	acceptor->setNewConnectionCallback(std::bind(&xTcpServer::newConnection,this,std::placeholders::_1));
}

xTcpServer::~xTcpServer()
{
	loop->assertInLoopThread();
	for (auto &it : connections)
	{
		TcpConnectionPtr conn = it.second;
		it.second.reset();
		conn->getLoop()->runInLoop(std::bind(&xTcpConnection::connectDestroyed,conn));
		conn.reset();
	}
}

void xTcpServer::newConnection(int32_t sockfd)
{
	loop->assertInLoopThread();
	xEventLoop *loop = threadPool->getNextLoop();
	TcpConnectionPtr conn(new xTcpConnection(loop,sockfd,context));
	connections[sockfd] = conn;
	conn->setConnectionCallback(std::move(connectionCallback));
	conn->setMessageCallback(std::move(messageCallback));
	conn->setWriteCompleteCallback(std::move(writeCompleteCallback));
	conn->setCloseCallback(std::bind(&xTcpServer::removeConnection,this,std::placeholders::_1));
	loop->runInLoop(std::bind(&xTcpConnection::connectEstablished,conn));
}

void xTcpServer::setThreadNum(int16_t numThreads)
{
	threadPool->setThreadNum(numThreads);
}

void xTcpServer::start()
{
	threadPool->start(threadInitCallback);
	acceptor->listen();
}

void xTcpServer::removeConnection(const TcpConnectionPtr &conn)
{
	loop->runInLoop(std::bind(&xTcpServer::removeConnectionInLoop,this,conn));
}

void xTcpServer::removeConnectionInLoop(const TcpConnectionPtr &conn)
{
	loop->assertInLoopThread();
	size_t n = connections.erase(conn->getSockfd());
	(void)n;
	assert(n == 1);
	xEventLoop *ioLoop = conn->getLoop();
	ioLoop->queueInLoop(std::bind(&xTcpConnection::connectDestroyed,conn));
}
