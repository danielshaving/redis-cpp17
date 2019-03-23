#pragma once

#include "httprequest.h"
#include "httpresponse.h"

class Buffer;

class HttpContext {
public:
    enum HttpRequestParseState {
        kExpectRequestLine,
        kExpectHeaders,
        kExpectBody,
        kGotAll,
    };

    HttpContext()
            : state(kExpectRequestLine) {

    }

    bool parseRequest(Buffer *buf);

    bool parseResponse(Buffer *buf);

    bool gotAll() const {
        return state == kGotAll;
    }

    void reset() {
        state = kExpectRequestLine;
        HttpRequest dummy;
        request.swap(dummy);
    }

    HttpRequest &getRequest() {
        return request;
    }

    HttpResponse &getResponse() {
        return response;
    }

    bool processRequestLine(const char *begin, const char *end);

    bool processResponseLine(const char *begin, const char *end);

private:
    HttpRequestParseState state;
    HttpRequest request;
    HttpResponse response;
};
