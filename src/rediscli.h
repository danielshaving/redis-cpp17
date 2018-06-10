#pragma once
#include "all.h"
#include "sds.h"
#include "hiredis.h"
#include "socket.h"

#define UNUSED(V) ((void) V)

#define OUTPUT_STANDARD 0
#define OUTPUT_RAW 1
#define OUTPUT_CSV 2
#define REDIS_CLI_KEEPALIVE_INTERVAL 15 /* seconds */
#define REDIS_CLI_DEFAULT_PIPE_TIMEOUT 30 /* seconds */
#define REDIS_CLI_HISTFILE_ENV "REDISCLI_HISTFILE"
#define REDIS_CLI_HISTFILE_DEFAULT ".rediscli_history"
#define REDIS_CLI_RCFILE_ENV "REDISCLI_RCFILE"
#define REDIS_CLI_RCFILE_DEFAULT ".redisclirc"

#define CLUSTER_MANAGER_SLOTS               16384
#define CLUSTER_MANAGER_MIGRATE_TIMEOUT     60000
#define CLUSTER_MANAGER_MIGRATE_PIPELINE    10
#define CLUSTER_MANAGER_REBALANCE_THRESHOLD 2

#define CLUSTER_MANAGER_INVALID_HOST_ARG \
    "[ERR] Invalid arguments: you need to pass either a valid " \
    "address (ie. 120.0.0.1:7000) or space separated IP " \
    "and port (ie. 120.0.0.1 7000)\n"


#define AE_READABLE 1   /* Fire when descriptor is readable. */
#define AE_WRITABLE 2   /* Fire when descriptor is writable. */

/* Cluster Manager Command Info */
typedef struct ClusterManagerCommand 
{
    char *name;
    int argc;
    char **argv;
    int flags;
    int replicas;
    char *from;
    char *to;
    char **weight;
    char *masterId;
    int weightArgc;
    int slots;
    int timeout;
    int pipeline;
    float threshold;
};

struct Config
{
	char *hostip;
    int hostport;
    char *hostsocket;
    long repeat;
    long interval;
    int dbnum;
    int interactive;
    int shutdown;
    int monitorMde;
    int pubsubMode;
    int latencyMode;
    int latencyDistMode;
    int latencyHistory;
    int lruTestMode;
    long long lruTestSampleSize;
    int clusterMode;
    int clusterReissueCommand;
    int slaveMode;
    int pipeMode;
    int pipeTimeout;
    int getrdbMode;
    int statMode;
    int scanMode;
    int intrinsicLatencyMode;
    int intrinsicLatencyDuration;
    char *pattern;
    char *rdbFileName;
    int bigkeys;
    int hotkeys;
    int stdinarg; /* get last arg from stdin. (-x option) */
    char *auth;
    int output; /* output mode, see OUTPUT_* defines */
    sds mbDelim;
    char prompt[128];
    char *eval;
    int evalLdb;
    int evalLdbSync;  /* Ask for synchronous mode of the Lua debugger. */
    int evalLdbEnd;   /* Lua debugging session ended. */
    int enableLdbOnEval; /* Handle manual SCRIPT DEBUG + EVAL commands. */
    int lastCmdType;
    int verbose;
    ClusterManagerCommand clusterManagerCommand;
};

class RedisCli
{
public:
    RedisCli();
    ~RedisCli();

    int redsCli(int argc,char **argv);
    int parseOptions(int argc,char **argv);
    void usage();
    int cliConnect(int force);
    int cliAuth();
    int cliSelect();
    int noninteractive(int argc,char **argv);
    sds readArgFromStdin(void);
    int issueCommand(int argc,char **argv) { return issuseCommandRepeat(argc,argv,config.repeat); }
    int issuseCommandRepeat(int argc,char **argv,int repat);
    int pollWait(int fd,int mask,int64_t milliseconds);
    int cliSendCommand(int argc,char **argv,int repeat);

private:
    RedisContextPtr context;
    Config config;
    Socket socket;

};






