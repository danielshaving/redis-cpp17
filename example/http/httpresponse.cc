#include "httpresponse.h"
#include "buffer.h"

void HttpResponse::appendToBuffer(Buffer *output) const {
    char buf[32] = {};
    snprintf(buf, sizeof buf, "HTTP/1.1 %d ", statusCode);
    output->append(buf);
    output->append(statusMessage);
    output->append("\r\n");

    if (closeConnection) {
        output->append("Connection: close\r\n");
    } else {
        snprintf(buf, sizeof buf, "Content-Length: %zd\r\n", body.size());
        output->append(buf);
        output->append("Connection: Keep-Alive\r\n");
    }

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
