#include "tunnel.h"
#include "socket.h"

EventLoop *g_eventLoop;
std::map <int32_t, TunnelPtr> g_tunnels;

void onServerConnection(const TcpConnectionPtr &conn) {
    if (conn->connected()) {
        conn->setTcpNoDelay(true);
    } else {
        auto it = g_tunnels.find(conn->getSockfd());
        if (it != g_tunnels.end()) {
            it->second->disconnect();
            g_tunnels.erase(it);
        }
    }
}

void onServerMessage(const TcpConnectionPtr &conn, Buffer *buf, TimeStamp) {
    LOG_DEBUG << conn->getSockfd() << " " << buf->readableBytes();
    if (g_tunnels.find(conn->getSockfd()) == g_tunnels.end()) {
        if (buf->readableBytes() > 128) {
            conn->shutdown();
        } else if (buf->readableBytes() > 8) {
            const char *begin = buf->peek() + 8;
            const char *end = buf->peek() + buf->readableBytes();
            const char *where = std::find(begin, end, '\0');
            if (where != end) {
                char ver = buf->peek()[0];
                char cmd = buf->peek()[1];
                const void *port = buf->peek() + 2;
                const void *ip = buf->peek() + 4;

                sockaddr_in addr;
                bzero(&addr, sizeof addr);
                addr.sin_family = AF_INET;
                addr.sin_port = *static_cast<const in_port_t *>(port);
                addr.sin_addr.s_addr = *static_cast<const uint32_t *>(ip);

                bool socks4a = Socket::networkToHost32(addr.sin_addr.s_addr) < 256;
                bool okay = false;
                if (socks4a) {
                    const char *endOfHostName = std::find(where + 1, end, '\0');
                    if (endOfHostName != end) {
                        std::string hostname = where + 1;
                        where = endOfHostName;
                        LOG_INFO << "Socks4a host name " << hostname;

                        struct sockaddr_in tmp;
                        if (Socket::resolve(hostname, &tmp)) {
                            addr.sin_addr.s_addr = tmp.sin_addr.s_addr;
                            okay = true;
                        }
                    } else {
                        return;
                    }
                } else {
                    okay = true;
                }

                if (ver == 4 && cmd == 1 && okay) {
                    TunnelPtr tunnel(new Tunnel(g_eventLoop, addr.sin_addr.s_addr, addr.sin_port, conn));
                    tunnel->setup();
                    tunnel->connect();
                    g_tunnels[conn->getSockfd()] = tunnel;
                    buf->retrieveUntil(where + 1);
                    char response[] = "\000\x5aUVWXYZ";
                    memcpy(response + 2, &addr.sin_port, 2);
                    memcpy(response + 4, &addr.sin_addr.s_addr, 4);
                    conn->send(response, 8);
                } else {
                    char response[] = "\000\x5bUVWXYZ";
                    conn->send(response, 8);
                    conn->shutdown();
                }
            }
        }
    } else if (conn->getContext().has_value()) {
        const TcpConnectionPtr &clientConn
                = std::any_cast<const TcpConnectionPtr &>(conn->getContext());
        clientConn->send(buf);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <listen_port>\n", argv[0]);
    } else {
        LOG_INFO << "pid = " << getpid();

        uint16_t port = static_cast<uint16_t>(atoi(argv[1]));

        EventLoop loop;
        g_eventLoop = &loop;

        TcpServer server(&loop, "127.0.0.1", port, "Socks4");

        server.setConnectionCallback(onServerConnection);
        server.setMessageCallback(onServerMessage);

        server.start();

        loop.loop();
    }
}

