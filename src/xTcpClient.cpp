#include "xTcpClient.h"
#include "xConnector.h"
#include "xTcpConnection.h"

xTcpClient::xTcpClient(xEventLoop *loop,const char *ip,int16_t port,const std::any &context)
:connector(new xConnector(loop,ip,port)),
loop(loop),
nextConnId(0),
context(context),
retry(false),
connect(true)
{
	enableRetry();
	connector->setNewConnectionCallback(std::bind(&xTcpClient::newConnection,this,std::placeholders::_1));
	connector->setConnectionErrorCallBack(std::bind(&xTcpClient::errorConnection,this));
}

namespace detail
{
	void removeConnection(xEventLoop *loop,const TcpConnectionPtr &conn)
	{
		loop->queueInLoop(std::bind(&xTcpConnection::connectDestroyed,conn));
	}

	void removeConnector(const ConnectorPtr &connector)
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

	if(conn)
	{
		assert(loop == conn->getLoop());
		CloseCallback cb = std::bind(&detail::removeConnection,loop,conn);
		loop->runInLoop(std::bind(&xTcpConnection::setCloseCallback,conn,cb));
		if (unique)
		{
			conn->forceClose();
		}
	}
	else
	{
		 connector->stop();
		 loop->runAfter(1,false,std::bind(&detail::removeConnector,connector));
	}
}

void xTcpClient::asyncConnect()
{
	connect = true;
	connector->asyncStart();
}

bool xTcpClient::syncConnect()
{
	return connector->syncStart();
}

void xTcpClient::disConnect()
{
	connect = false;
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
	connect = false;
	connector->stop();
}

void xTcpClient::errorConnection()
{
	connectionErrorCallBack(context);
}

void xTcpClient::newConnection(int32_t sockfd)
{
	TcpConnectionPtr conn(new xTcpConnection(loop,sockfd,context));
	conn->setConnectionCallback(std::move(connectionCallback));
	conn->setMessageCallback(std::move(messageCallback));
	conn->setWriteCompleteCallback(std::move(writeCompleteCallback));
	conn->setCloseCallback(std::bind(&xTcpClient::removeConnection,this,std::placeholders::_1));
	{
		std::unique_lock<std::mutex> lk(mutex);
		connection = conn;
	}
	conn->connectEstablished();
}

void xTcpClient::removeConnection(const TcpConnectionPtr &conn)
{
	loop->assertInLoopThread();
	assert(loop == conn->getLoop());
	{
		std::unique_lock<std::mutex> lk(mutex);
		assert(connection == conn);
		connection.reset();
	}

	loop->queueInLoop(std::bind(&xTcpConnection::connectDestroyed,conn));
	if (retry && connect)
	{
		connector->restart();
	}
}
