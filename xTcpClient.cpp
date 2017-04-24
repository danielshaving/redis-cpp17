#include "xTcpClient.h"
#include "xConnector.h"
#include "xTcpconnection.h"

xTcpClient::xTcpClient()
{

}

xTcpClient::xTcpClient(xEventLoop *loop,std::string ip,int port,void *data)
: connector(new xConnector(loop,ip,port)),
loop(loop),
 ip(ip),
 port(port),
 isconnect(false),
 nextConnId(0),
 data(data)
{
	  connector->setNewConnectionCallback(std::bind(&xTcpClient::newConnection, this, std::placeholders::_1));
	  connector->setConnectionErrorCallBack(std::bind(&xTcpClient::errorConnection,this));
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
	    CloseCallback cb = std::bind(&xTcpClient::removeConnection, this, std::placeholders::_1);
	    loop->runInLoop(std::bind(&xTcpconnection::setCloseCallback, conn, cb));
	    if (unique)
	    {
	      conn->forceClose();
	    }
	  }
	  else
	  {
	    connector->stop();
	  }
}


void xTcpClient::connect()
{
	 isconnect = true;
	 connector->start();
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
	  connectionErrorCallBack(data);
}
