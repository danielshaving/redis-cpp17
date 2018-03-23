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
	webCallback = callback;
}

xHttpServer::~xHttpServer()
{

}

void xHttpServer::start()
{
	server.start();
}

void xHttpServer::onConnection(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		xHttpContext context;
		conn->setContext(context);
	}
	else
	{

	}
}

void xHttpServer::webMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	xHttpContext *context = std::any_cast<xHttpContext>(conn->getContext());
	if(context->parseWebRequest(conn,buffer))
	{
		xHttpResponse resp(true);
		xBuffer sendBuf;
		webCallback(context->getRequest(),&resp);
		context->wsFrameBuild(resp.getBody().c_str(),resp.getBody().size(),
				&sendBuf,xHttpRequest::TEXT_FRAME,true,false);
		conn->send(&sendBuf);
		context->reset();
	}
}

void xHttpServer::onMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	xHttpContext *context = std::any_cast<xHttpContext>(conn->getContext());
	if(!context->parseRequest(buffer))
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->shutdown();
	}

	auto &headers = context->getRequest().getHeaders();
	auto it = headers.find("Sec-WebSocket-Key");
	if(it != headers.end())
	{
		context->getRequest().setSecKey(it->second + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
	}
	else
	{
		conn->shutdown();
	}

	if(context->gotAll())
	{
		if(context->getRequest().getMethod() == xHttpRequest::kPost)
		{
			conn->shutdown();
		}

		onRequest(conn,context->getRequest());
		context->reset();
	}
}

void xHttpServer::onRequest(const TcpConnectionPtr &conn,const xHttpRequest &req)
{
	xBuffer sendBuf;
	SHA1_CTX ctx;
	unsigned char hash[20];
	SHA1Init(&ctx);
	SHA1Update(&ctx, (const unsigned char*)req.getSecKey().c_str(), req.getSecKey().size());
	SHA1Final(hash, &ctx);
	std::string base64Str = base64Encode((const unsigned char *)hash, sizeof(hash));

	sendBuf.append("HTTP/1.1 101 Switching Protocols\r\n"
			 "Upgrade: websocket\r\n"
			 "Connection: Upgrade\r\n"
			 "Sec-WebSocket-Accept: ");

	sendBuf.append(base64Str.data(),base64Str.size());
	sendBuf.append("\r\n\r\n");
	conn->send(&sendBuf);
	conn->setMessageCallback(std::bind(&xHttpServer::webMessage,this,std::placeholders::_1,std::placeholders::_2));
}












