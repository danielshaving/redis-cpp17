#pragma once

#include "all.h"
#include "util.h"
#include "socket.h"
#include "callback.h"
#include "log.h"
#include "eventloop.h"
#include "channel.h"

extern "C"
{
struct hostent;
struct ares_channeldata;
typedef struct ares_channeldata *ares_channel;
}

class Resolver {
public:
    typedef std::function<void(const std::string &, uint16_t &)> Callback;
    enum Option {
        kDNSandHostsFile,
        kDNSonly,
    };

    explicit Resolver(EventLoop *loop, Option opt = kDNSandHostsFile);

    ~Resolver();

    bool resolve(StringArg hostname, const Callback &cb);

private:
    Resolver(const Resolver &);

    void operator=(const Resolver &);

    struct QueryData {
        Resolver *owner;
        Callback callback;

        QueryData(Resolver *o, const Callback &cb)
                : owner(o), callback(cb) {

        }
    };

    EventLoop *loop;
    Socket socket;
    ares_channel ctx;
    bool timerActive;
    typedef std::map <int32_t, ChannelPtr> ChannelList;
    ChannelList channels;

    void onRead(int32_t sockfd);

    void onTimer();

    void onQueryResult(int32_t status, struct hostent *result, const Callback &cb);

    void onSockCreate(int32_t sockfd, int32_t type);

    void onSockStateChange(int32_t sockfd, bool read, bool write);

    static void ares_host_callback(void *data, int32_t status, int32_t timeouts, struct hostent *hostent);

    static int32_t ares_sock_create_callback(int32_t sockfd, int32_t type, void *data);

    static void ares_sock_state_callback(void *data, int32_t sockfd, int32_t read, int32_t write);

};
