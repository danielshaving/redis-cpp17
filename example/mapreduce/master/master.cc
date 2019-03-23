#include "all.h"
#include "log.h"
#include "tcpserver.h"
#include "tcpconnection.h"

class Master {
public:
    Master(EventLoop *loop, const char *ip, int16_t port)
            : loop(loop),
              server(loop, ip, port, this),
              senders(0) {
        server.setConnectionCallback(
                std::bind(&Master::connCallBack, this, std::placeholders::_1));
        server.setMessageCallback(
                std::bind(&Master::readCallBack, this, std::placeholders::_1, std::placeholders::_2));
    }

    void start(int senders) {
        LOG_INFO << "start " << senders << " senders";
        this->senders = senders;
        masters.clear();
        server.start();
    }

private:
    void connCallBack(const TcpConnectionPtr &conn) {
        if (!conn->connected()) {
            if (--senders == 0) {
                output();
                loop->quit();
            }
        }
    }

    void readCallBack(const TcpConnectionPtr &conn, Buffer *buf) {
        const char *crlf = nullptr;
        while ((crlf = buf->findCRLF()) != nullptr) {
            // string request(buf->peek(), crlf);
            // printf("%s\n", request.c_str());
            const char *tab = std::find(buf->peek(), crlf, '\t');
            if (tab != crlf) {
                std::string key(buf->peek(), tab);
                std::string value(tab + 1, crlf);
                masters.insert(std::make_pair(key, value));
            } else {
                LOG_WARN << "Wrong format, no tab found";
                conn->shutdown();
            }
            buf->retrieveUntil(crlf + 2);
        }
    }

    void output() {
        LOG_INFO << "Writing shard " << masters.size();
        std::ofstream out("shard");
        for (auto &it : masters) {
            out << it.first << '\t' << it.second << '\n';
        }
    }

    EventLoop *loop;
    TcpServer server;
    int senders;
    std::multimap <std::string, std::string> masters;
};

int main(int argc, char *argv[]) {
    EventLoop loop;
    Master receiver(&loop, "127.0.0.1", 8888);
    receiver.start(1);
    loop.run();
}
