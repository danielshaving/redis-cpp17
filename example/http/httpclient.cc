#include "httpclient.h"
#include "socket.h"

HttpClient::HttpClient(EventLoop *loop)
        : loop(loop),
          index(0) {

}


void HttpClient::getUrl(const char *ip, int16_t port, const std::string &url,
                        const std::string &host, const TcpConnectionPtr &conn, HttpCallBack &&callback) {
    std::shared_ptr <HttpRequest> request(new HttpRequest());
    request->setIndex(++index);
    request->setQuery(url);
    request->setMethod(HttpRequest::kGet);
    request->addHeader("Host", host);

    TcpClientPtr client(new TcpClient(loop, ip, port, request));
    client->closeRetry();
    client->setConnectionCallback(std::bind(&HttpClient::onConnection,
                                            this, std::placeholders::_1));
    client->setMessageCallback(std::bind(&HttpClient::onMessage,
                                         this, std::placeholders::_1, std::placeholders::_2));
    client->connect();
    tcpclients[index] = client;
    tcpConns[index] = conn;
    httpCallbacks[index] = callback;
}

void HttpClient::postUrl(const char *ip, int16_t port, const std::string &url,
                         const std::string &body, const std::string &host,
                         const TcpConnectionPtr &conn, HttpCallBack &&callback) {
    std::shared_ptr <HttpRequest> request(new HttpRequest());
    request->setIndex(++index);
    request->setQuery(url);
    request->setBody(body);
    request->setMethod(HttpRequest::kPost);
    request->addHeader("Host", host);
    request->addHeader("Content-Length", std::to_string(body.size()));

    TcpClientPtr client(new TcpClient(loop, ip, port, request));
    client->closeRetry();
    client->setConnectionCallback(std::bind(&HttpClient::onConnection,
                                            this, std::placeholders::_1));
    client->setMessageCallback(std::bind(&HttpClient::onMessage,
                                         this, std::placeholders::_1, std::placeholders::_2));
    client->connect();
    tcpclients[index] = client;
    tcpConns[index] = conn;
    httpCallbacks[index] = callback;
}

void HttpClient::onConnection(const TcpConnectionPtr &conn) {
    std::shared_ptr <HttpRequest> request = std::any_cast <
                                            std::shared_ptr < HttpRequest >> (conn->getContext());
    if (conn->connected()) {
        Socket::setkeepAlive(conn->getSockfd(), kHeart);
        request->appendToBuffer(conn->outputBuffer());
        conn->sendPipe();

        std::shared_ptr <HttpContext> c(new HttpContext());
        conn->setContext1(c);
    } else {
        size_t n = tcpclients.erase(request->getIndex());
        assert(n == 1);
        n = tcpConns.erase(request->getIndex());
        assert(n == 1);
        n = httpCallbacks.erase(request->getIndex());
        assert(n == 1);
    }
}

void HttpClient::onMessage(const TcpConnectionPtr &conn, Buffer *buffer) {
    std::shared_ptr <HttpContext> context = std::any_cast <
                                            std::shared_ptr < HttpContext >> (conn->getContext1());
    std::shared_ptr <HttpRequest> request = std::any_cast <
                                            std::shared_ptr < HttpRequest >> (conn->getContext());
    assert(context->parseResponse(buffer));

    if (context->gotAll()) {
        auto it = tcpConns.find(request->getIndex());
        assert(it != tcpConns.end());

        auto iter = httpCallbacks.find(request->getIndex());
        assert(iter != httpCallbacks.end());
        iter->second(conn, context->getResponse(), it->second);
    }
}
