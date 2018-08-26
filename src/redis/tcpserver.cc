#include "tcpserver.h"
#include "tcpconnection.h"

TcpServer::TcpServer(EventLoop *loop, const char *ip, int16_t port, const std::any &context)
	:loop(loop),
	acceptor(new Acceptor(loop, ip, port)),
	threadPool(new ThreadPool(loop)),
	context(context)
{
	acceptor->setNewConnectionCallback(std::bind(&TcpServer::newConnection, this, std::placeholders::_1));
}

TcpServer::~TcpServer()
{
	loop->assertInLoopThread();
	for (auto &it : connections)
	{
		TcpConnectionPtr conn = it.second;
		it.second.reset();
		conn->getLoop()->runInLoop(std::bind(&TcpConnection::connectDestroyed, conn));
		conn.reset();
	}
}

void TcpServer::newConnection(int32_t sockfd)
{
	loop->assertInLoopThread();
	EventLoop *loop = threadPool->getNextLoop();
	TcpConnectionPtr conn(new TcpConnection(loop, sockfd, context));
	connections[sockfd] = conn;
	conn->setConnectionCallback(std::move(connectionCallback));
	conn->setMessageCallback(std::move(messageCallback));
	conn->setWriteCompleteCallback(std::move(writeCompleteCallback));
	conn->setCloseCallback(std::bind(&TcpServer::removeConnection, this, std::placeholders::_1));
	loop->runInLoop(std::bind(&TcpConnection::connectEstablished, conn));
}

void TcpServer::setThreadNum(int16_t numThreads)
{
	threadPool->setThreadNum(numThreads);
}

void TcpServer::start()
{
	threadPool->start(threadInitCallback);
	acceptor->listen();
}

void TcpServer::removeConnection(const TcpConnectionPtr &conn)
{
	loop->runInLoop(std::bind(&TcpServer::removeConnectionInLoop, this, conn));
}

void TcpServer::removeConnectionInLoop(const TcpConnectionPtr &conn)
{
	loop->assertInLoopThread();
	size_t n = connections.erase(conn->getSockfd());
	(void)n;
	assert(n == 1);
	EventLoop *ioLoop = conn->getLoop();
	ioLoop->queueInLoop(std::bind(&TcpConnection::connectDestroyed, conn));
}
