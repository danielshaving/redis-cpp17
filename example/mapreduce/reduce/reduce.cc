#include "all.h"
#include "log.h"
#include "tcpserver.h"
#include "tcpconnection.h"

std::multimap <std::string, std::string> reduces;

void read(const char *file) {
    std::ifstream in(file);
    std::string line;

    while (getline(in, line)) {
        size_t tab = line.find('\t');
        assert(tab != string::npos);
        std::string key(line.c_str(), line.c_str() + tab);
        std::string value(line.c_str() + tab + 1, line.c_str() + line.size());
        reduces.insert(std::make_pair(key, value));
    }
    LOG_INFO << "Reduces size: " << reduces.size();
}

std::multimap<std::string, std::string>::iterator fillBuffer(
        std::multimap<std::string, std::string>::iterator first, Buffer *buf) {
    while (first != reduces.end()) {
        buf->append(first->first);
        buf->append("\t");
        buf->append(first->second);
        buf->append("\n");
        ++first;
        if (buf->readableBytes() > 65536) {
            break;
        }
    }
    return first;
}

void send(const TcpConnectionPtr &conn, std::multimap<std::string, std::string>::iterator first) {
    Buffer buf;
    auto last = fillBuffer(first, &buf);
    conn->setContext(last);
    conn->send(&buf);
}

void onConnection(const TcpConnectionPtr &conn) {
    if (conn->connected()) {
        send(conn, reduces.begin());
    }
}

void onWriteComplete(const TcpConnectionPtr &conn) {
    auto first = std::any_cast<std::multimap<std::string, std::string>::iterator>(conn->getContext());
    if (first != reduces.end()) {
        send(conn, first);
    } else {
        conn->shutdown();
        LOG_INFO << "Sender - done";
    }
}

void serve(uint16_t port) {
    LOG_INFO << "Listen on port " << port;
    EventLoop loop;
    TcpServer server(&loop, "127.0.0.1", port, nullptr);
    server.setConnectionCallback(onConnection);
    server.setWriteCompleteCallback(onWriteComplete);
    server.start();
    loop.run();
}

int main(int argc, char *argv[]) {
    if (argc > 1) {
        read(argv[1]);
        serve(9999);
    } else {
        fprintf(stderr, "Usage: %s shard_file \n", argv[0]);
    }
}
