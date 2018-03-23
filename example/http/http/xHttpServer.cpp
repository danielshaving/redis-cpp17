#include "xHttpServer.h"

xHttpServer::xHttpServer(xEventLoop *loop,const char *ip,uint16_t port)
:loop(loop),
 server(loop,ip,port,nullptr)
{
	server.setConnectionCallback(std::bind(&xHttpServer::onConnection,this,std::placeholders::_1));
	server.setMessageCallback(std::bind(&xHttpServer::onMessage,this,std::placeholders::_1,std::placeholders::_2));
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
		std::shared_ptr<xHttpContext> context (new xHttpContext());
		auto it = webSockets.find(conn->getSockfd());
		assert(it == webSockets.end());
		webSockets[conn->getSockfd()] = context;
		//conn->setContext(context);
	}
	else
	{
		auto it = webSockets.find(conn->getSockfd());
		assert(it != webSockets.end());
		webSockets.erase(conn->getSockfd());
	}
}

void xHttpServer::webMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	auto it = webSockets.find(conn->getSockfd());
	assert(it != webSockets.end());
	while(buffer->readableBytes() > 0)
	{
		size_t size = 0;
		bool fin = false;
		it->second->getRequest().setOpCode();
		if (!it->second->wsFrameExtractBuffer(buffer->peek(),buffer->readableBytes(),size,fin))
		{
			break;
		}

		if (fin && (it->second->getRequest().getOpCode() == xHttpRequest::TEXT_FRAME ||
				it->second->getRequest().getOpCode() == xHttpRequest::BINARY_FRAME))
		{
			xBuffer sendBuf;
			xHttpResponse resp(true);
			webCallback(it->second->getRequest(),&resp);
			auto intputBuffer = resp.intputBuffer();
			it->second->wsFrameBuild(intputBuffer->peek(),intputBuffer->readableBytes(),&sendBuf,xHttpRequest::TEXT_FRAME,true,false);
			conn->send(&sendBuf);
			it->second->reset();
		}
		else if(it->second->getRequest().getOpCode() == xHttpRequest::CONTINUATION_FRAME)
		{

		}
		else if (it->second->getRequest().getOpCode() == xHttpRequest::PING_FRAME ||
				it->second->getRequest().getOpCode() == xHttpRequest::PONG_FRAME )
		{

		}
		else if(it->second->getRequest().getOpCode() == xHttpRequest::CLOSE_FRAME)
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

void xHttpServer::onMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	auto it = webSockets.find(conn->getSockfd());
	assert(it != webSockets.end());

	//xHttpContext *context = std::any_cast<xHttpContext>(conn->getContext());
	if(!it->second->parseRequest(buffer))
	{
		conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
		conn->shutdown();
	}

	auto &headers = it->second->getRequest().getHeaders();
	auto iter = headers.find("Sec-WebSocket-Key");
	if(iter != headers.end())
	{
		it->second->getRequest().setSecKey(iter->second + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
	}
	else
	{
		conn->shutdown();
	}

	if(it->second->gotAll())
	{
		if(it->second->getRequest().getMethod() == xHttpRequest::kPost)
		{
			conn->shutdown();
		}

		onRequest(conn,it->second->getRequest());
		it->second->reset();
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












