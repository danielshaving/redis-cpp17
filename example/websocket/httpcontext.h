#pragma once
#include "httprequest.h"
#include "tcpconnection.h"
#include "log.h"

class HttpContext
{
public:
	enum HttpRequestParseState
	{
		kExpectRequestLine,
		kExpectHeaders,
		kExpectBody,
		kGotAll,
	};

	HttpContext()
	:state(kExpectRequestLine)
	{

	}

	bool parseRequest(Buffer *buffer);
	bool wsFrameExtractBuffer(const TcpConnectionPtr &conn,const char *buf,const size_t bufferSize,size_t &size,bool &fin);
	bool wsFrameBuild(Buffer *buffer,HttpRequest::WebSocketType framType,bool ok = true,bool masking = false);

	bool gotAll() const { return state == kGotAll; }
	void reset() { request.reset(); }

	HttpRequest &getRequest() { return request; }
	bool processRequestLine(const char *begin,const char *end);

private:
	HttpRequestParseState state;
	HttpRequest request;
};
