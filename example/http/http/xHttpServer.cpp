#include "xHttpServer.h"
#include "xHttpContext.h"
#include "xHttpResponse.h"
#include "xHttpRequest.h"
#include "xBuffer.h"
#include "xTcpConnection.h"

xHttpServer::xHttpServer(xEventLoop *loop,const  char *ip,uint16_t port)
:loop(loop),
 server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&xHttpServer::onConnection,this,std::placeholders::_1));
	server.setMessageCallback(std::bind(&xHttpServer::onMessage,this,std::placeholders::_1,std::placeholders::_2));
}


void xHttpServer::disPlayer(const char *begin)
{

}


void xHttpServer::setMessageCallback(HttpCallBack callback)
{
	httpCallback = callback;
}

xHttpServer::~xHttpServer()
{

}


void xHttpServer::start()
{
	server.start();
}

void xHttpServer::onConnection(const TcpConnectionPtr & conn)
{
	if(conn->connected())
	{
		LOG_INFO<<"connecct";
		xHttpContext context;
		conn->setContext((void*)&context);
	}
	else
	{
		LOG_INFO<<"disconnecct";
	}
}


void xHttpServer::onMessage(const TcpConnectionPtr &conn,xBuffer *recvBuf)
{
	xHttpContext * context = std::any_cast<xHttpContext>(conn->getContext());
	if(!context->parseRequest(recvBuf))
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->shutdown();
	}

	if(context->gotAll())
	{
		if(context->getRequest().getMethod() == xHttpRequest::kPost)
		{
			context->getRequest().setQuery(recvBuf->peek(),recvBuf->peek() + recvBuf->readableBytes());
		}
		onRequest(conn,context->getRequest());
		context->reset();
	}

}

void xHttpServer::onRequest(const TcpConnectionPtr & conn ,const xHttpRequest & req)
{
	const std::string &connection =req.getHeader("Connection");
	bool close = connection == "close" || (req.getVersion() == xHttpRequest::kHttp10 && connection != "Keep-Alive");
	 xHttpResponse response(close);
	 httpCallback(req,&response);
	 xBuffer sendBuf;
	 response.appendToBuffer(&sendBuf);
	 conn->send(&sendBuf);
	 if(response.getCloseConnection())
	 {
		 conn->shutdown();
	 }
}












