#include "xHttpServer.h"
#include "xHttpContext.h"
#include "xHttpResponse.h"
#include "xHttpRequest.h"
#include "xBuffer.h"
#include "xTcpconnection.h"
xHttpServer::xHttpServer(xEventLoop *loop,const std::string &ip,const int32_t port)
{

}


void xHttpServer::disPlayer(const char *begin)
{

}


xHttpServer::~xHttpServer()
{

}


void xHttpServer::start()
{
	server.start();
}

void xHttpServer::onConnection(const xTcpconnectionPtr & conn)
{
	if(conn->connected())
	{
		xHttpContext context;
		conn->setContext((void*)&context);
	}
}


void xHttpServer::onMessage(const xTcpconnectionPtr &conn,xBuffer *recvBuf)
{
	xHttpContext * context = (xHttpContext*)(conn->getContext());
	if(!context->parseClientRequest(recvBuf))
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->shutdown();
	}

	if(context->gotAll())
	{
		onRequest(conn,context->getRequest());
		context->reset();
	}


}


void xHttpServer::onRequest(const xTcpconnectionPtr & conn ,const xHttpRequest & req)
{
	const std::string &connection =req.getHeader("Connection");
	bool close = connection == "close" ||
	    (req.getVersion() == xHttpRequest::kHttp10 && connection != "Keep-Alive");
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












