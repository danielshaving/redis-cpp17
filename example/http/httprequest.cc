#include "httprequest.h"

void HttpRequest::setMethod() {
    method = kContent;
}

bool HttpRequest::setMethod(const char *start, const char *end) {
    assert(method == kInvalid);
    std::string m(start, end);
    if (m == "GET") {
        method = kGet;
    } else if (m == "POST") {
        method = kPost;
    } else if (m == "HEAD") {
        method = kHead;
    } else if (m == "PUT") {
        method = kPut;
    } else if (m == "DELETE") {
        method = kDelete;
    } else {
        method = kInvalid;
    }
    return method != kInvalid;
}

const char *HttpRequest::methodString() const {
    const char *result = "UNKNOWN";
    switch (method) {
        case kGet: {
            result = "GET";
            break;
        }
        case kPost: {
            result = "POST";
            break;
        }
        case kHead: {
            result = "HEAD";
            break;
        }
        case kPut: {
            result = "PUT";
            break;
        }
        case kDelete: {
            result = "DELETE";
            break;
        }
        default:
            break;

    }
    return result;
}

const std::string &HttpRequest::getPath() const {
    return path;
}

void HttpRequest::setPath(const char *start, const char *end) {
    path.assign(start, end);
}

void HttpRequest::setQuery(const char *start, const char *end) {
    query.assign(start, end);
}

const std::string &HttpRequest::getQuery() const {
    return query;
}

void HttpRequest::setReceiveTime(int64_t t) {
    receiveTime = t;
}

void HttpRequest::addContent(const char *start, const char *colon, const char *end) {
    std::string field(start, colon);
    ++colon;
    while (colon < end && isspace(*colon)) {
        ++colon;
    }
    std::string value(colon, end);
    while (!value.empty() && isspace(value[value.size() - 1])) {
        value.resize(value.size() - 1);
    }
    contentLength = atoi(value.c_str());
}

void HttpRequest::addHeader(const char *start, const char *colon, const char *end) {
    std::string field(start, colon);
    ++colon;
    while (colon < end && isspace(*colon)) {
        ++colon;
    }

    std::string value(colon, end);
    while (!value.empty() && isspace(value[value.size() - 1])) {
        value.resize(value.size() - 1);
    }
    headers[field] = value;
}

std::string HttpRequest::getHeader(const std::string &field) const {
    std::string result;
    auto it = headers.find(field);
    if (it != headers.end()) {
        result = it->second;
    }
    return result;
}

const std::map <std::string, std::string> &HttpRequest::getHeaders() const {
    return headers;
}

void HttpRequest::swap(HttpRequest &that) {
    std::swap(method, that.method);
    path.swap(that.path);
    query.swap(that.query);
    headers.swap(that.headers);
}

void HttpRequest::addHeader(const std::string &key, const std::string &value) {
    headers[key] = value;
}

void HttpRequest::appendToBuffer(Buffer *output) const {
    if (method == kPost) {
        output->append("POST ");
    } else if (method == kGet) {
        output->append("GET ");
    }

    output->append(query.c_str(), query.size());
    output->append(" HTTP/1.1\r\n");

    for (const auto &header : headers) {
        output->append(header.first);
        output->append(": ");
        output->append(header.second);
        output->append("\r\n");
    }

    output->append("\r\n");
    if (body.size() > 0) {
        output->append(body.c_str(), body.size());
    }
}
