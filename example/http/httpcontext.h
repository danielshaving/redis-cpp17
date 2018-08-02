#pragma once
#include "httprequest.h"
class Buffer;
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

	bool parseRequest(Buffer *buf);
	bool gotAll() const
	{
		return state == kGotAll;
	}

	void reset()
	{
		state = kExpectRequestLine;
		HttpRequest dummy;
		request.swap(dummy);
	}

	HttpRequest &getRequest()
	{
		return request;
	}

	bool processRequestLine(const char *begin,const char *end);

private:
	HttpRequestParseState state;
	HttpRequest request;
};
