#include "xTcpClient.h"
#include "xConnector.h"
#include "xTcpconnection.h"

xTcpClient::xTcpClient()
{

}

xTcpClient::xTcpClient(xEventLoop *loop,void *data)
: connector(new xConnector(loop)),
loop(loop),
isconnect(false),
nextConnId(0),
data(data)
{
	  connector->setNewConnectionCallback(std::bind(&xTcpClient::newConnection, this, std::placeholders::_1));
	  connector->setConnectionErrorCallBack(std::bind(&xTcpClient::errorConnection,this));
}


namespace detail
{

	void removeConnection(xEventLoop* loop, const xTcpconnectionPtr& conn)
	{
	 	loop->queueInLoop(std::bind(&xTcpconnection::connectDestroyed, conn));
	}

	void removeConnector(const xConnectorPtr& connector)
	{

	}

}
xTcpClient::~xTcpClient()
{
	  xTcpconnectionPtr conn;
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
		loop->runInLoop(std::bind(&xTcpconnection::setCloseCallback, conn, cb));
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


void xTcpClient::connect(const char *ip,int32_t port)
{
	 isconnect = true;
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
	connectionErrorCallBack(data);
}


void xTcpClient::newConnection(int sockfd)
{
	  xTcpconnectionPtr conn(new xTcpconnection(loop,sockfd,data));
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


void xTcpClient::removeConnection(const xTcpconnectionPtr& conn)
{
	loop->assertInLoopThread();
	assert(loop == conn->getLoop());
	{
		std::unique_lock<std::mutex> lk(mutex);
		assert(connection == conn);
		connection.reset();
	}

	loop->queueInLoop(std::bind(&xTcpconnection::connectDestroyed, conn));
}
