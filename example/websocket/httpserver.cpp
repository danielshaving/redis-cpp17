#include "httpserver.h"

HttpServer::HttpServer(EventLoop *loop,const char *ip,uint16_t port)
:loop(loop),
 server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&HttpServer::onConnection,this,std::placeholders::_1));
	server.setMessageCallback(std::bind(&HttpServer::onHandeShake,this,std::placeholders::_1,std::placeholders::_2));
}

void HttpServer::setConnCallback(HttpConnCallBack callback)
{
	httpConnCallback = callback;
}

void HttpServer::setMessageCallback(HttpReadCallBack callback)
{
	httpReadCallback = callback;
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
	httpConnCallback(conn);
}

void HttpServer::onMessage(const TcpConnectionPtr &conn,Buffer *buffer)
{
	auto context = std::any_cast<xHttpContext>(conn->getContext());
	while(buffer->readableBytes() > 0)
	{
		size_t size = 0;
		bool fin = false;

		if (!context->wsFrameExtractBuffer(conn,buffer->peek(),buffer->readableBytes(),size,fin))
		{
			break;
		}

		if(context->getRequest().getOpCode() == HttpRequest::PING_FRAME ||
						context->getRequest().getOpCode() == HttpRequest::PONG_FRAME ||
						context->getRequest().getOpCode() == HttpRequest::CLOSE_FRAME)
		{
			conn->forceClose();
			break;
		}
		else if(context->getRequest().getOpCode() == HttpRequest::CONTINUATION_FRAME)
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
			break;
		}

		buffer->retrieve(size);
	}
}

void HttpServer::onHandeShake(const TcpConnectionPtr &conn,Buffer *buffer)
{
	auto context = std::any_cast<xHttpContext>(conn->getContext());
	if(!context->parseRequest(buffer) ||
			context->getRequest().getMethod() != HttpRequest::kGet)
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

void HttpServer::onRequest(const TcpConnectionPtr &conn,const HttpRequest &req)
{
	auto &headers = req.getHeaders();
	auto iter = headers.find("Sec-WebSocket-Key");
	if(iter == headers.end())
	{
		conn->forceClose();
		return ;
	}

	Buffer sendBuf;
	sendBuf.append("HTTP/1.1 101 Switching Protocols\r\n"
			 "Upgrade: websocket\r\n"
			 "Connection: Upgrade\r\n"
			 "Sec-WebSocket-Accept: ");

	std::string secKey = iter->second + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	SHA1_CTX ctx;
	unsigned char hash[20];
	SHA1Init(&ctx);
	SHA1Update(&ctx,(const unsigned char*)secKey.c_str(),secKey.size());
	SHA1Final(hash,&ctx);
	std::string base64Str = base64Encode((const unsigned char *)hash,sizeof(hash));
	sendBuf.append(base64Str.data(),base64Str.size());
	sendBuf.append("\r\n\r\n");
	conn->send(&sendBuf);
	conn->setMessageCallback(std::bind(&HttpServer::onMessage,this,std::placeholders::_1,std::placeholders::_2));
}












