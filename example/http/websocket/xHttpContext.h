#pragma once
#include "xHttpRequest.h"
#include "xTcpConnection.h"
#include "xLog.h"

class xHttpContext
{
public:
	enum HttpRequestParseState
	{
		kExpectRequestLine,
		kExpectHeaders,
		kExpectBody,
		kGotAll,
	};

	xHttpContext()
	:state(kExpectRequestLine)
	{

	}

	bool parseRequest(xBuffer *buffer);
	bool wsFrameExtractBuffer(const char *buf,const size_t bufferSize,size_t &size,bool &ok);
	bool wsFrameBuild(xBuffer *buffer,xHttpRequest::WebSocketType framType,bool ok = true,bool masking = false);

	bool gotAll() const { return state == kGotAll; }
	void reset()
	{
		state = kExpectRequestLine;
		request.reset();
	}

	xHttpRequest &getRequest() { return request; }
	bool processRequestLine(const char *begin,const char *end);

private:
	HttpRequestParseState state;
	xHttpRequest request;
};
