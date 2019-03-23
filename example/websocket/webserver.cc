#include "webserver.h"

WebServer::WebServer(EventLoop *loop, const char *ip, uint16_t port)
        : loop(loop),
          server(loop, ip, port, nullptr) {
    server.setConnectionCallback(std::bind(&WebServer::onConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&WebServer::onHandeShake, this, std::placeholders::_1, std::placeholders::_2));
}

void WebServer::setConnCallback(WebConnCallBack callback) {
    httpConnCallback = callback;
}

void WebServer::setMessageCallback(WebReadCallBack callback) {
    httpReadCallback = callback;
}

WebServer::~WebServer() {

}

void WebServer::start() {
    server.start();
}

void WebServer::onConnection(const TcpConnectionPtr &conn) {
    httpConnCallback(conn);
}

void WebServer::onMessage(const TcpConnectionPtr &conn, Buffer *buffer) {
    auto context = std::any_cast<WebContext>(conn->getMutableContext());
    while (buffer->readableBytes() > 0) {
        size_t size = 0;
        bool fin = false;

        if (!context->wsFrameExtractBuffer(conn, buffer->peek(), buffer->readableBytes(), size, fin)) {
            break;
        }

        if (context->getRequest().getOpCode() == WebRequest::PING_FRAME ||
            context->getRequest().getOpCode() == WebRequest::PONG_FRAME ||
            context->getRequest().getOpCode() == WebRequest::CLOSE_FRAME) {
            conn->forceClose();
            break;
        } else if (context->getRequest().getOpCode() == WebRequest::CONTINUATION_FRAME) {

        } else if (fin) {
            if (!httpReadCallback(&(context->getRequest()), conn)) {
                conn->forceClose();
                break;
            }
            context->reset();
        } else {
            conn->forceClose();
            break;
        }

        buffer->retrieve(size);
    }
}

void WebServer::onHandeShake(const TcpConnectionPtr &conn, Buffer *buffer) {
    auto context = std::any_cast<WebContext>(conn->getMutableContext());
    if (!context->parseRequest(buffer) ||
        context->getRequest().getMethod() != WebRequest::kGet) {
        conn->send("HTTP/1.1 400 Bad Request\r\n\r\n");
        conn->forceClose();
    }

    if (context->gotAll()) {
        onRequest(conn, context->getRequest());
        context->reset();
    } else {
        conn->forceClose();
    }
}

void WebServer::onRequest(const TcpConnectionPtr &conn, const WebRequest &req) {
    auto &headers = req.getHeaders();
    auto iter = headers.find("Sec-WebSocket-Key");
    if (iter == headers.end()) {
        conn->forceClose();
        return;
    }

    Buffer sendBuf;
    sendBuf.append("HTTP/1.1 101 Switching Protocols\r\n"
                   "Upgrade: websocket\r\n"
                   "Connection: Upgrade\r\n"
                   "Sec-WebSocket-Accept: ");

    std::string secKey = iter->second + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    SHA1_CTX ctx;
    unsigned char hash[20];
    SHA1Init(&ctx);
    SHA1Update(&ctx, (const unsigned char *) secKey.c_str(), secKey.size());
    SHA1Final(hash, &ctx);
    std::string base64Str = base64Encode((const unsigned char *) hash, sizeof(hash));
    sendBuf.append(base64Str.data(), base64Str.size());
    sendBuf.append("\r\n\r\n");
    conn->send(&sendBuf);
    conn->setMessageCallback(std::bind(&WebServer::onMessage, this, std::placeholders::_1, std::placeholders::_2));
}












