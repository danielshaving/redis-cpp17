#pragma once
#include "xHttpRequest.h"
#include "xLog.h"

class xBuffer;
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

	bool parseRequest(xBuffer *buf);
	bool wsFrameExtractBuffer(const char *peek,const size_t bufferSize,
			 xHttpRequest::WebSocketType &outopcode,size_t &frameSize,bool &outfin);
	bool parseWebRequest(xBuffer *buf);
	bool wsFrameBuild(const char *payload,size_t payloadLen,xBuffer *buffer,
	          xHttpRequest::WebSocketType frame_type = xHttpRequest::WebSocketType::TEXT_FRAME,
	          bool isFin = true,bool masking = false);

	bool gotAll() const
	{
		return state == kGotAll;
	}

	void reset()
	{
		state = kExpectRequestLine;
		xHttpRequest dummy;
		request.swap(dummy);
	}

	xHttpRequest & getRequest()
	{
		return request;
	}

	bool processRequestLine(const char *begin,const char *end);

private:
	HttpRequestParseState state;
	xHttpRequest request;

};
