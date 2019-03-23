#pragma once

#include "all.h"
#include "util.h"
class Buffer;

class HttpResponse {
public:
    enum HttpStatusCode {
        kUnknown,
        k200k = 200,
        k301MovedPermanently = 301,
        k400BadRequest = 400,
        k404NotFound = 404,
    };

    explicit HttpResponse()
            : statusCode(kUnknown),
			  closeConnection(false) {

    }

	void setStatusCode(const char *start, const char *end) {
		int64_t code;
		string2ll(start, end - start, &code);
		setStatusCode(HttpStatusCode(code));
	}

	void setStatusMessage(const char *start, const char *end) {
		statusMessage.assign(start, end);
	}

	HttpStatusCode getStatusCode() {
		return statusCode;
	}

    void setStatusCode(HttpStatusCode code) {
        statusCode = code;
    }

    void setStatusMessage(const std::string &message) {
        statusMessage = message;
    }

    void setCloseConnection(bool on) {
        closeConnection = on;
    }

    bool getCloseConnection() {
        return closeConnection;
    }

    void setContentType(const std::string &contentType) {
        addHeader("Content-Type", contentType);
    }

    void addHeader(const char *start, const char *colon, const char *end) {
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

    void addHeader(const std::string &key, const std::string &value) {
        headers[key] = value;
    }

    void setBody(const std::string &body) {
        this->body = body;
    }

    std::string getBody() const {
    	return body;
    }

    void appendToBuffer(Buffer *output) const;

    int32_t getBodySize() {
    	auto it = headers.find("Content-Length");
    	assert (it != headers.end());
    	int32_t len = atol(it->second.c_str());
    	return len;
    }
private:
    std::map <std::string, std::string> headers;
    HttpStatusCode statusCode;
    std::string statusMessage;
    bool closeConnection;
    std::string body;
};
