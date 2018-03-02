#include "xTcpClient.h"
#include "xConnector.h"
#include "xTcpConnection.h"

xTcpClient::xTcpClient()
{

}

xTcpClient::xTcpClient(xEventLoop *loop,const std::any & context)
: connector(new xConnector(loop)),
loop(loop),
isconnect(false),
nextConnId(0),
context(context)
{
	  connector->setNewConnectionCallback(std::bind(&xTcpClient::newConnection, this, std::placeholders::_1));
	  connector->setConnectionErrorCallBack(std::bind(&xTcpClient::errorConnection,this));
}

namespace detail
{
	void removeConnection(xEventLoop* loop, const TcpConnectionPtr& conn)
	{
	 	loop->queueInLoop(std::bind(&xTcpConnection::connectDestroyed, conn));
	}

	void removeConnector(const ConnectorPtr& connector)
	{

	}
}

xTcpClient::~xTcpClient()
{
	  TcpConnectionPtr conn;
	  bool unique = false;
	  {
		std::unique_lock<std::mutex> lk(mutex);
		unique = connection.unique();
		conn = connection;
	  }

	  if (conn)
	  {
		assert(loop == conn->getLoop());
		CloseCallback cb = std::bind(&detail::removeConnection, loop, conn);
		loop->runInLoop(std::bind(&xTcpConnection::setCloseCallback, conn, cb));
		if (unique)
		{
			conn->forceClose();
		}
	  }
	  else
	  {
	   	 connector->stop();
	   	 loop->runAfter(1, nullptr,false,std::bind(&detail::removeConnector, connector));
	  }
}

void xTcpClient::connect(const char *ip,int16_t port)
{
	 isconnect = true;
	 this->ip = ip;
	 this->port = port;
	 connector->start(ip,port);
}

void xTcpClient::disconnect()
{
	isconnect = false;
	{
		std::unique_lock<std::mutex> lk(mutex);
		if (connection)
		{
		  	connection->shutdown();
		}
	}
}

void xTcpClient::stop()
{
	isconnect = false;
	connector->stop();
}

void xTcpClient::errorConnection()
{
	connectionErrorCallBack(context);
}

void xTcpClient::newConnection(int32_t sockfd)
{
	TcpConnectionPtr conn(new xTcpConnection(loop,sockfd,context));
	conn->setConnectionCallback(connectionCallback);
	conn->setMessageCallback(messageCallback);
	conn->setWriteCompleteCallback(writeCompleteCallback);
	conn->setCloseCallback(std::bind(&xTcpClient::removeConnection, this,std::placeholders::_1));
	{
		std::unique_lock<std::mutex> lk(mutex);
		connection = conn;
	}
	conn->connectEstablished();
}

void xTcpClient::removeConnection(const TcpConnectionPtr& conn)
{
	loop->assertInLoopThread();
	assert(loop == conn->getLoop());
	{
		std::unique_lock<std::mutex> lk(mutex);
		assert(connection == conn);
		connection.reset();
	}
	loop->queueInLoop(std::bind(&xTcpConnection::connectDestroyed, conn));
}
