//
// Created by zhanghao on 2018/6/17.
//

#include "rediscli.h"

int main(int argc,char **argv)
{
    RedisCli cli;

    int firstarg = cli.parseOptions(argc,argv);
    argc -= firstarg;
    argv += firstarg;

    assert(cli.cliConnect(0) == REDIS_OK);
/* Otherwise, we have some arguments to execute */
    return cli.noninteractive(argc, cli.convertToSds(argc, argv));
}