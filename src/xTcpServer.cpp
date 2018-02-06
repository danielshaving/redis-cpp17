#include "xTcpServer.h"
#include "xTcpconnection.h"

xTcpServer::xTcpServer(xEventLoop *loop,std::string ip,int16_t port,const std::any & context)
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

	for (auto  it = connections.begin(); it != connections.end(); ++it)
	{
		xTcpconnectionPtr conn = it->second;
		it->second.reset();
		conn->getLoop()->runInLoop(std::bind(&xTcpconnection::connectDestroyed, conn));
		conn.reset();
	}
}


void xTcpServer::newConnection(int32_t sockfd)
{
	loop->assertInLoopThread();
	xEventLoop* loop = threadPool->getNextLoop();
	xTcpconnectionPtr conn(new xTcpconnection(loop,sockfd,context));
	connections[sockfd] = conn;
	conn->setConnectionCallback(connectionCallback);
	conn->setMessageCallback(messageCallback);
	conn->setWriteCompleteCallback(writeCompleteCallback);
	conn->setCloseCallback(std::bind(&xTcpServer::removeConnection, this, std::placeholders::_1));
	loop->runInLoop(std::bind(&xTcpconnection::connectEstablished, conn));
}


void xTcpServer::setThreadNum(int numThreads)
{
	threadPool->setThreadNum(numThreads);
}

void xTcpServer::start()
{
	threadPool->start(threadInitCallback);
	acceptor->listen();
}

void xTcpServer::removeConnection(const xTcpconnectionPtr& conn)
{
	loop->runInLoop(std::bind(&xTcpServer::removeConnectionInLoop, this, conn));
}


void xTcpServer::removeConnectionInLoop(const xTcpconnectionPtr& conn)
{
	loop->assertInLoopThread();
	size_t n = connections.erase(conn->getSockfd());
	(void)n;
	assert(n == 1);
	xEventLoop* ioLoop = conn->getLoop();
	ioLoop->queueInLoop(std::bind(&xTcpconnection::connectDestroyed, conn));
}
