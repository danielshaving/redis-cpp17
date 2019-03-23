#include "log.h"
#include "tcpclient.h"
#include "tcpconnection.h"

const size_t kMaxHashSize = 10 * 1000 * 1000;

class MapWoker {
public:
    MapWoker(EventLoop *loop, const char *ip1,
             int port1)
            : loop(loop),
              count(1) {
        TcpClientPtr client1(new TcpClient(loop, ip1, port1, this));
        client1->setConnectionCallback(std::bind(&MapWoker::connCallBack, this,
                                                 std::placeholders::_1));
        client1->setMessageCallback(std::bind(&MapWoker::readCallBack, this,
                                              std::placeholders::_1, std::placeholders::_2));
        client1->connect(true);
        tcpClients.push_back(client1);
        LOG_INFO << "all connect success";
    }

    ~MapWoker() {

    }

    void readCallBack(const TcpConnectionPtr &conn, Buffer *recvBuf) {

    }

    void connCallBack(const TcpConnectionPtr &conn) {
        if (conn->connected()) {
            --count;
        } else {
            if (++count == 1) {
                LOG_INFO << "all disconnect success";
                loop->quit();
            }
        }
    }

    bool isconnectAll() {

    }

    void readFile(const char *fileName) {
        std::ifstream in(fileName);
        std::string str;
        while (in) {
            works.clear();
            while (in >> str) {
                works.insert(std::make_pair(str, str));
            }
        }

        int32_t topN = 100;
        for (auto &it : works) {
            if (--topN <= 0) {
                break;
            }
            std::cout << it.first << " " << it.second << std::endl;
        }
    }

    void processFile(const char *fileName) {
        std::ifstream in(fileName);
        std::string str;

        while (in) {
            works.clear();
            while (in >> str) {
                works.insert(std::make_pair(str, str));
                if (works.size() > kMaxHashSize) {
                    break;
                }
            }

            LOG_INFO << "map work " << works.size() << " recude work";
            for (auto &it : works) {
                size_t index = std::hash<std::string>()(it.first) % tcpClients.size();
                buffer.append(it.first);
                buffer.append("\t");
                buffer.append(it.second);
                buffer.append("\r\n");
                tcpClients[index]->getConnection()->send(&buffer);
                buffer.retrieveAll();
            }
        }
    }

    void disconnectAll() {
        for (auto &it : tcpClients) {
            it->getConnection()->shutdown();//close eagin  close write  ask  TCP fin
        }
    }

private:
    std::multimap <std::string, std::string> works;
    EventLoop *loop;
    std::vector <TcpClientPtr> tcpClients;
    std::atomic <int32_t> count;
    Buffer buffer;
};

int main(int argc, char *argv[]) {
    if (argc < 2) {
        exit(1);
    }

    EventLoop loop;
    MapWoker work(&loop, "127.0.0.1", 8888);
    //work.readFile(argv[1]);

    work.processFile(argv[1]);
    work.disconnectAll();
    loop.run();
    return 0;
}
