#include "xHttpServer.h"

xHttpServer::xHttpServer(xEventLoop *loop,const char *ip,uint16_t port)
:loop(loop),
 server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&xHttpServer::onConnection,this,std::placeholders::_1));
	server.setMessageCallback(std::bind(&xHttpServer::onHandeShake,this,std::placeholders::_1,std::placeholders::_2));
}

void xHttpServer::setConnCallback(HttpConnCallBack callback)
{
	httpConnCallback = callback;
}

void xHttpServer::setMessageCallback(HttpReadCallBack callback)
{
	httpReadCallback = callback;
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
	httpConnCallback(conn);
}

void xHttpServer::onMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	auto context = std::any_cast<xHttpContext>(conn->getContext());
	if(buffer->readableBytes() > 1024)
	{
		LOG_WARN<<"xHttpServer::onMessage length > 1024";
		conn->forceClose();
		return ;
	}

	while(buffer->readableBytes() > 0)
	{
		size_t size = 0;
		bool fin = false;

		if (!context->wsFrameExtractBuffer(conn,buffer->peek(),buffer->readableBytes(),size,fin))
		{
			break;
		}

		if(context->getRequest().getOpCode() == xHttpRequest::PING_FRAME ||
						context->getRequest().getOpCode() == xHttpRequest::PONG_FRAME ||
						context->getRequest().getOpCode() == xHttpRequest::CLOSE_FRAME)
		{
			conn->forceClose();
			LOG_WARN<<"xHttpServer::onMessage type error";
			break;
		}
		else if(context->getRequest().getOpCode() == xHttpRequest::CONTINUATION_FRAME)
		{

		}
		else if(fin)
		{
			if(!httpReadCallback(&(context->getRequest()),conn))
			{
				conn->forceClose();
				break;
			}
			context->reset();
		}
		else
		{
			conn->forceClose();
#ifdef __DEBUG__

			assert(false);
#endif
			break;
		}

		buffer->retrieve(size);
	}
}

void xHttpServer::onHandeShake(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	auto context = std::any_cast<xHttpContext>(conn->getContext());
	if(!context->parseRequest(buffer) ||
			context->getRequest().getMethod() != xHttpRequest::kGet)
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->forceClose();
	}

	if(context->gotAll())
	{
		onRequest(conn,context->getRequest());
		context->reset();
	}
	else
	{
		conn->forceClose();
	}
}

void xHttpServer::onRequest(const TcpConnectionPtr &conn,const xHttpRequest &req)
{
	auto &headers = req.getHeaders();
	auto iter = headers.find("Sec-WebSocket-Key");
	if(iter == headers.end())
	{
		LOG_WARN<<"xHttpServer::onRequest error";
		conn->forceClose();
		return ;
	}

	xBuffer sendBuf;
	sendBuf.append("HTTP/1.1 101 Switching Protocols\r\n"
			 "Upgrade: websocket\r\n"
			 "Connection: Upgrade\r\n"
			 "Sec-WebSocket-Accept: ");

	std::string secKey = iter->second + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	SHA1_CTX ctx;
	unsigned char hash[20];
	SHA1Init(&ctx);
	SHA1Update(&ctx, (const unsigned char*)secKey.c_str(),secKey.size());
	SHA1Final(hash, &ctx);
	std::string base64Str = base64Encode((const unsigned char *)hash, sizeof(hash));
	sendBuf.append(base64Str.data(),base64Str.size());
	sendBuf.append("\r\n\r\n");
	conn->send(&sendBuf);
	conn->setMessageCallback(std::bind(&xHttpServer::onMessage,this,std::placeholders::_1,std::placeholders::_2));
}












