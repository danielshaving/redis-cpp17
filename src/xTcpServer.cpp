#include "xTcpServer.h"
#include "xTcpconnection.h"

xTcpServer::xTcpServer()
{

}


void xTcpServer::init(xEventLoop *loop,std::string ip,int16_t port,void *data)
{
	this->loop = loop;
	this->data = data;
	acceptor =  std::unique_ptr<xAcceptor>(new xAcceptor(loop,ip,port));
	threadPool = std::shared_ptr<xThreadPool>(new xThreadPool(loop));
	acceptor->setNewConnectionCallback(std::bind(&xTcpServer::newConnection,this,std::placeholders::_1));

}

xTcpServer::~xTcpServer()
{
	loop->assertInLoopThread();

	for (ConnectionMap::iterator it(connections.begin());
	  it != connections.end(); ++it)
	{
		xTcpconnectionPtr conn = it->second;
		it->second.reset();
		conn->getLoop()->runInLoop(
		  std::bind(&xTcpconnection::connectDestroyed, conn));
		conn.reset();
	}
}


void xTcpServer::newConnection(int sockfd)
{
	loop->assertInLoopThread();
	xEventLoop* loop = threadPool->getNextLoop();
	xTcpconnectionPtr conn(new xTcpconnection(loop,sockfd,data));
	connections[sockfd] = conn;
	conn->setConnectionCallback(connectionCallback);
	conn->setMessageCallback(messageCallback);
	conn->setWriteCompleteCallback(writeCompleteCallback);
	conn->setCloseCallback(std::bind(&xTcpServer::removeConnection, this, std::placeholders::_1)); // FIXME: unsafe
	loop->runInLoop(std::bind(&xTcpconnection::connectEstablished, conn));
}


void xTcpServer::setData(void *data)
{
	this->data = data;
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
	  /// Not thread safe, but in loop
void xTcpServer::removeConnectionInLoop(const xTcpconnectionPtr& conn)
{
	loop->assertInLoopThread();
	size_t n = connections.erase(conn->getSockfd());
	(void)n;
	assert(n == 1);
	xEventLoop* ioLoop = conn->getLoop();
	ioLoop->queueInLoop(
	std::bind(&xTcpconnection::connectDestroyed, conn));


}
