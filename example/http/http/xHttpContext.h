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
	bool wsFrameExtractBuffer(const char *peek,const size_t bufferSize,size_t &size,bool &ok);
	bool wsFrameBuild(const char *payload,size_t payloadLen,xBuffer *buffer,
	          xHttpRequest::WebSocketType framType = xHttpRequest::WebSocketType::TEXT_FRAME,
	          bool ok = true,bool masking = false);

	bool gotAll() const { return state == kGotAll; }
	void reset()
	{
		state = kExpectRequestLine;
		request.reset();
	}

	xHttpRequest &getRequest() { return request;}
	bool processRequestLine(const char *begin,const char *end);

private:
	HttpRequestParseState state;
	xHttpRequest request;
};
