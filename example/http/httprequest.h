#pragma once

#include "all.h"
#include "buffer.h"

class HttpResponse;
class HttpRequest {
public:
    HttpRequest()
            : method(kInvalid),
              version(kUnknown) {
    }

    enum Method {
        kInvalid, kGet, kPost, kHead, kPut, kDelete, kContent
    };

    enum Version {
        kUnknown, kHttp10, kHttp11
    };

    void setVersion(Version v) {
        version = v;
    }

    Version getVersion() const {
        return version;
    }

    void setMethod(Method method) {
    	this->method = method;
    }
    void setMethod() ;

    bool setMethod(const char *start, const char *end);

    Method getMethod() const {
        return method;
    }

    const char *methodString() const;

    const std::string &getPath() const;

    void setPath(const char *start, const char *end);

    void setQuery(const std::string &query) {
    	this->query = query;
    }

    void setBody(const std::string &body) {
    	this->body = body;
    }

    void setQuery(const char *start, const char *end);

    const std::string &getQuery() const;

    void setReceiveTime(int64_t t);

    void addContent(const char *start, const char *colon, const char *end);

    void addHeader(const char *start, const char *colon, const char *end);

    void addHeader(const std::string &key, const std::string &value);

    std::string getHeader(const std::string &field) const;

    const std::map <std::string, std::string> &getHeaders() const;

    void swap(HttpRequest &that);

    void appendToBuffer(Buffer *output) const;

    void setIndex(int64_t idx) {
    	index = idx;
    }

    int64_t getIndex() {
    	return index;
    }
private:
    Method method;
    Version version;
    std::string path;
    std::string query;
    std::string body;
    int32_t queryLength;
    std::map <std::string, std::string> headers;
    int64_t receiveTime;
    int32_t contentLength;
    int64_t index;
};
