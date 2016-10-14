#pragma once
#include "xHttpRequest.h"
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

	bool parseServerRequest(xBuffer *buf);
	bool parseClientRequest(xBuffer *buf);

	bool gotAll() const
	{;
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
