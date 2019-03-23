//
// Created by admin on 2018/6/25.
//

#include "all.h"
#include "common.h"
#include "timer.h"

class TTcp {
public:
    void transmit(const Options &opt);

    void receive(const Options &opt);

    void blockTransmit(const Options &opt);

    void blockReceive(const Options &opt);

    int read(int sockfd, const void *buf, int length);

    int write(int sockfd, const void *buf, int length);

    int acceptOrDie(uint16_t port);

private:

};
