//
// Created by admin on 2018/6/25.
//

#include "common.h"
#include "ttcp.h"

int main(int argc, char *argv[]) {
    TTcp tcp;
    Options options;
    if (parseCommandLine(argc, argv, &options)) {
        if (options.transmit) {
            tcp.transmit(options);
        } else if (options.receive) {
            tcp.receive(options);
        } else {
            assert(0);
        }
    }
    return 0;
}