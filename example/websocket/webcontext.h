#pragma once
#include "webrequest.h"
#include "tcpconnection.h"
#include "log.h"

class WebContext
{
public:
	enum WebRequestParseState
	{
		kExpectRequestLine,
		kExpectHeaders,
		kExpectBody,
		kGotAll,
	};

	WebContext()
	:state(kExpectRequestLine)
	{

	}

	bool parseRequest(Buffer *buffer);
	bool wsFrameExtractBuffer(const TcpConnectionPtr &conn,const char *buf,const size_t bufferSize,size_t &size,bool &fin);
	bool wsFrameBuild(Buffer *buffer,WebRequest::WebSocketType framType,bool ok = true,bool masking = false);

	bool gotAll() const { return state == kGotAll; }
	void reset() { request.reset(); }

	WebRequest &getRequest() { return request; }
	bool processRequestLine(const char *begin,const char *end);

private:
	WebRequestParseState state;
	WebRequest request;
};
