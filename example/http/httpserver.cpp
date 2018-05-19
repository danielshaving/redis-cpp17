#include "httpcontext.h"
#include "httprequest.h"
#include "httpresponse.h"
#include "httpserver.h"
#include "buffer.h"
#include "tcpconnection.h"

HttpServer::HttpServer(EventLoop *loop,const char *ip,uint16_t port)
:loop(loop),
 server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&HttpServer::onConnection,this,std::placeholders::_1));
	server.setMessageCallback(std::bind(&HttpServer::onMessage,this,std::placeholders::_1,std::placeholders::_2));
}


void HttpServer::disPlayer(const char *begin)
{

}


void HttpServer::setMessageCallback(HttpCallBack callback)
{
	httpCallback = callback;
}

HttpServer::~HttpServer()
{

}


void HttpServer::start()
{
	server.start();
}

void HttpServer::onConnection(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		HttpContext context;
		conn->setContext(context);
	}
	else
	{

	}
}

void HttpServer::onMessage(const TcpConnectionPtr &conn,Buffer *buffer)
{
	HttpContext *context = std::any_cast<HttpContext>(conn->getMutableContext());
	if(!context->parseRequest(buffer))
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->shutdown();
	}

	if(context->gotAll())
	{
		if(context->getRequest().getMethod() == HttpRequest::kPost)
		{
			context->getRequest().setQuery(buffer->peek(),buffer->peek() + buffer->readableBytes());
		}
		onRequest(conn,context->getRequest());
		context->reset();
	}
}

void HttpServer::onRequest(const TcpConnectionPtr & conn,const HttpRequest &req)
{
	const std::string &connection = req.getHeader("Connection");
	bool close = connection == "close" ||
			(req.getVersion() == HttpRequest::kHttp10 && connection != "Keep-Alive");
	HttpResponse response(close);
	httpCallback(req,&response);
	Buffer sendBuf;
	response.appendToBuffer(&sendBuf);
	conn->send(&sendBuf);
	if(response.getCloseConnection())
	{
		conn->shutdown();
	}
}












