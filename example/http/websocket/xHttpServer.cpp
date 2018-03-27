#include "xHttpServer.h"

xHttpServer::xHttpServer(xEventLoop *loop,const char *ip,uint16_t port)
:loop(loop),
 server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&xHttpServer::onConnection,this,std::placeholders::_1));
	server.setMessageCallback(std::bind(&xHttpServer::onHandShake,this,std::placeholders::_1,std::placeholders::_2));
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

void xHttpServer::onConnection(const TcpConnectionPtr &conn)
{
	if(conn->connected())
	{
		std::shared_ptr<xHttpContext> context (new xHttpContext());
		auto it = webSockets.find(conn->getSockfd());
		assert(it == webSockets.end());
		webSockets[conn->getSockfd()] = context;
		LOG_INFO<<"onConnection";
	}
	else
	{
		auto it = webSockets.find(conn->getSockfd());
		assert(it != webSockets.end());
		webSockets.erase(conn->getSockfd());
		LOG_INFO<<"disonConnection";
	}
}

void xHttpServer::onMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	auto it = webSockets.find(conn->getSockfd());
	assert(it != webSockets.end());
	auto context = it->second;

	auto &cacheFrame = context->getRequest().getWSCacheFrame();
	auto &parseString = context->getRequest().getWSParseString();

	while(buffer->readableBytes() > 0)
	{
		parseString.clear();
		size_t size = 0;
		bool ok = false;
		context->getRequest().setOpCode();

		if (!context->wsFrameExtractBuffer(buffer->peek(),buffer->readableBytes(),size,ok))
		{
			break;
		}

//		if (ok && (context->getRequest().getOpCode() == xHttpRequest::TEXT_FRAME ||
//				context->getRequest().getOpCode() == xHttpRequest::BINARY_FRAME))
		if (ok)
		{
			 if (!cacheFrame.empty())
			{
				cacheFrame += parseString;
				parseString = std::move(cacheFrame);
				cacheFrame.clear();
			}

			xHttpResponse resp;
			httpCallback(context->getRequest(),&resp);
			context->wsFrameBuild(resp.intputBuffer(),xHttpRequest::BINARY_FRAME,true,false);
			conn->send(resp.intputBuffer());
		}
		else if(context->getRequest().getOpCode() == xHttpRequest::CONTINUATION_FRAME)
		{
			 cacheFrame += parseString;
			 parseString.clear();
		}
		else if (context->getRequest().getOpCode() == xHttpRequest::PING_FRAME ||
				context->getRequest().getOpCode() == xHttpRequest::PONG_FRAME )
		{
			conn->shutdown();
		}
		else if(context->getRequest().getOpCode() == xHttpRequest::CLOSE_FRAME)
		{
			conn->shutdown();
		}
		else
		{
			assert(false);
		}

		buffer->retrieve(size);
	}
}

void xHttpServer::onHandShake(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	auto it = webSockets.find(conn->getSockfd());
	assert(it != webSockets.end());

	if(!it->second->parseRequest(buffer) ||
			it->second->getRequest().getMethod() != xHttpRequest::kGet)
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->shutdown();
	}

	if(it->second->gotAll())
	{
		onRequest(conn,it->second->getRequest());
		sendBuf.retrieveAll();
		secKey.clear();
		it->second->reset();
	}
	else
	{
		conn->shutdown();
	}
}

void xHttpServer::onRequest(const TcpConnectionPtr &conn,const xHttpRequest &req)
{
	auto &headers = req.getHeaders();
	auto iter = headers.find("Sec-WebSocket-Key");
	if(iter == headers.end())
	{
		conn->shutdown();
		return ;
	}

	sendBuf.append("HTTP/1.1 101 Switching Protocols\r\n"
			 "Upgrade: websocket\r\n"
			 "Connection: Upgrade\r\n"
			 "Sec-WebSocket-Accept: ");

	secKey = iter->second + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
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












