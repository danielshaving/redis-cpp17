#include "rediscli.h"

RedisCli::RedisCli()
{
	config.hostip = sdsnew("127.0.0.1");
	config.hostport = 6379;
	config.hostsocket = nullptr;
	config.dbnum = 0;
	config.auth = nullptr;

}

RedisCli::~RedisCli()
{

}

void RedisCli::usage()
{
    fprintf(stderr,
"Usage: redis-cli [OPTIONS] [cmd [arg [arg ...]]]\n"
"  -h <hostname>      Server hostname (default: 127.0.0.1).\n"
"  -p <port>          Server port (default: 6379).\n"
"  -s <socket>        Server socket (overrides hostname and port).\n"
"  -a <password>      Password to use when connecting to the server.\n"
"  -u <uri>           Server URI.\n"
"  -r <repeat>        Execute specified command N times.\n"
"  -i <interval>      When -r is used, waits <interval> seconds per command.\n"
"                     It is possible to specify sub-second times like -i 0.1.\n"
"  -n <db>            Database number.\n"
"  -x                 Read last argument from STDIN.\n"
"  -d <delimiter>     Multi-bulk delimiter in for raw formatting (default: \\n).\n"
"  -c                 Enable cluster mode (follow -ASK and -MOVED redirections).\n"
"  --raw              Use raw formatting for replies (default when STDOUT is\n"
"                     not a tty).\n"
"  --no-raw           Force formatted output even when STDOUT is not a tty.\n"
"  --csv              Output in CSV format.\n"
"  --stat             Print rolling stats about server: mem, clients, ...\n"
"  --latency          Enter a special mode continuously sampling latency.\n"
"                     If you use this mode in an interactive session it runs\n"
"                     forever displaying real-time stats. Otherwise if --raw or\n"
"                     --csv is specified, or if you redirect the output to a non\n"
"                     TTY, it samples the latency for 1 second (you can use\n"
"                     -i to change the interval), then produces a single output\n"
"                     and exits.\n"
"  --latency-history  Like --latency but tracking latency changes over time.\n"
"                     Default time interval is 15 sec. Change it using -i.\n"
"  --latency-dist     Shows latency as a spectrum, requires xterm 256 colors.\n"
"                     Default time interval is 1 sec. Change it using -i.\n"
"  --lru-test <keys>  Simulate a cache workload with an 80-20 distribution.\n"
"  --slave            Simulate a slave showing commands received from the master.\n"
"  --rdb <filename>   Transfer an RDB dump from remote server to local file.\n"
"  --pipe             Transfer raw Redis protocol from stdin to server.\n"
"  --pipe-timeout <n> In --pipe mode, abort with error if after sending all data.\n"
"                     no reply is received within <n> seconds.\n"
"                     Default timeout: %d. Use 0 to wait forever.\n"
"  --bigkeys          Sample Redis keys looking for big keys.\n"
"  --hotkeys          Sample Redis keys looking for hot keys.\n"
"                     only works when maxmemory-policy is *lfu.\n"
"  --scan             List all keys using the SCAN command.\n"
"  --pattern <pat>    Useful with --scan to specify a SCAN pattern.\n"
"  --intrinsic-latency <sec> Run a test to measure intrinsic system latency.\n"
"                     The test will run for the specified amount of seconds.\n"
"  --eval <file>      Send an EVAL command using the Lua script at <file>.\n"
"  --ldb              Used with --eval enable the Redis Lua debugger.\n"
"  --ldb-sync-mode    Like --ldb but uses the synchronous Lua debugger, in\n"
"                     this mode the server is blocked and script changes are\n"
"                     are not rolled back from the server memory.\n"
"  --cluster <command> [args...] [opts...]\n"
"                     Cluster Manager command and arguments (see below).\n"
"  --verbose          Verbose mode.\n"
"  --help             Output this help and exit.\n"
"  --version          Output version and exit.\n"
"\n"
"Cluster Manager Commands:\n"
"  Use --cluster help to list all available cluster manager commands.\n"
"\n"
"Examples:\n"
"  cat /etc/passwd | redis-cli -x set mypasswd\n"
"  redis-cli get mypasswd\n"
"  redis-cli -r 100 lpush mylist x\n"
"  redis-cli -r 100 -i 1 info | grep used_memory_human:\n"
"  redis-cli --eval myscript.lua key1 key2 , arg1 arg2 arg3\n"
"  redis-cli --scan --pattern '*:12345*'\n"
"\n"
"  (Note: when using --eval the comma separates KEYS[] from ARGV[] items)\n"
"\n"
"When no command is given, redis-cli starts in interactive mode.\n"
"Type \"help\" in interactive mode for information on available commands\n"
"and settings.\n"
"\n");

    exit(1);
}

int RedisCli::parseOptions(int argc,char **argv)
{
	int i;
	for (i = 1; i < argc; i++)
	{
		int lastarg = i==argc - 1;
		if (!strcmp(argv[i],"-h") && !lastarg)
		{
			sdsfree(config.hostip);
			config.hostip = sdsnew(argv[++i]);
		}
		else if (!strcmp(argv[i],"-h") && lastarg)
		{
			usage();
		}
		else if (!strcmp(argv[i],"--help"))
		{
			usage();
		}
		else if (!strcmp(argv[i],"-p") && lastarg)
		{
			config.hostport = atoi(argv[++i]);
		}
		else
		{
			if (argv[i][0] == '-')
			{
				fprintf(stderr,"Unrecognized option or bad number of args for: '%s'\n",argv[i]);
                exit(1);
			}
			else
			{
				break;
			}
		}
	}
	return i;
}

int RedisCli::cliConnect(int force)
{
	if (context == nullptr || force)
	{
		if(context != nullptr)
		{
			context.reset();
		}
	}

	if (config.hostsocket == nullptr)
	{
		context = redisConnect(config.hostip,config.hostport);
	}
	else
	{
		context = redisConnectUnix(config.hostsocket);
	}

	if (context->err)
	{
		fprintf(stderr,"Could not connect to Redis at ");
		if (config.hostsocket == nullptr) { fprintf(stderr,"%s:%d: %s\n",config.hostip,config.hostport,context->errstr); }
		else { fprintf(stderr,"%s: %s\n",config.hostsocket,context->errstr); }
		context.reset();
		context = nullptr;
		return REDIS_ERR;
	}
}

int RedisCli::cliAuth()
{
	RedisReply *reply;
	if(config.auth == nullptr) return REDIS_ERR;
	reply = context->redisCommand("AUTH %s",config.auth);
	if(reply != nullptr)
	{
		freeReply(reply);
		return REDIS_OK;
	}
	return REDIS_ERR;
}

int RedisCli::cliSelect()
{
	RedisReply *reply;
	if (config.dbnum == 0) { return REDIS_OK; }

	reply = context->redisCommand("SELECT %d",config.dbnum);
	if (reply != nullptr)
	{
		int result = REDIS_OK;
		if (reply->type == REDIS_REPLY_ERROR) { result = REDIS_ERR; }
		freeReply(reply);
		return result;
	}
	return REDIS_ERR;
}

int RedisCli::redsCli(int argc,char **argv)
{
	int firstarg = parseOptions(argc,argv);
	argc -= firstarg;
    argv += firstarg;

    if (cliConnect(0) != REDIS_OK)
    {
    	exit(1);
    }

	socket.setkeepAlive(context->fd,REDIS_CLI_KEEPALIVE_INTERVAL);
	/* Do AUTH and select the right DB. */
	if (cliAuth() != REDIS_OK) { return REDIS_ERR; }
	if (cliSelect() != REDIS_OK) { return REDIS_ERR; }

	return REDIS_OK;
}

sds RedisCli::readArgFromStdin(void)
{
	char buf[1024];
	sds arg = sdsempty();

	while (1)
	{
		int nread = ::read(fileno(stdin),buf,1024);
		if (nread == 0) { break; }
		else if (nread == -1) {  perror("Reading from standard input"); exit(1); }
		arg = sdscatlen(arg,buf,nread);

	}
	return arg;
}

int RedisCli::noninteractive(int argc,char **argv)
{
	int retval = 0;
	if (config.stdinarg)
	{
		argv = (char **)zrealloc(argv,(argc + 1) * sizeof(char *));
		argv[argc] = readArgFromStdin();
		retval = issueCommand(argc + 1,argv);
	}
	else { retval = issueCommand(argc,argv); }

	return retval;
}

int RedisCli::cliSendCommand(int argc,char **argv,int repeat)
{

}

int RedisCli::issuseCommandRepeat(int argc,char **argv,int repat)
{
	while(1)
	{
		config.clusterReissueCommand = 0;
		if (clieSendCommand(argc,argv,repeat) != REDIS_OK)
		{
			cliConnect(1);
			if (clieSendCommand(argc,argv,repeat) != REDIS_OK)
			{
				return REDIS_ERR;
			}
		}
	}
	return REDIS_OK;
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int RedisCli::pollWait(int fd,int mask,int64_t milliseconds)
{
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd,0,sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;
    if ((retval = poll(&pfd, 1, milliseconds)) == 1)
    {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    }
    else
    {
        return retval;
    }
}


























