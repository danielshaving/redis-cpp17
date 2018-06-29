#include "rediscli.h"

RedisCli::RedisCli()
{
	config.hostip = sdsnew("127.0.0.1");
	config.hostport = 6379;
	config.hostsocket = nullptr;
	config.dbnum = 0;
	config.auth = nullptr;
	config.stdinarg = 0;
    config.monitorMde = 0;
	config.repeat = 1;
    config.pubsubMode = 0;
    config.slaveMode = 0;
    config.output = OUTPUT_STANDARD;

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
		else if (!strcmp(argv[i],"--csv"))
        {
			config.output = OUTPUT_CSV;
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
		if (config.hostsocket == nullptr)
		{
			fprintf(stderr,"%s:%d: %s\n",config.hostip,config.hostport,context->errstr);
		}
		else
		{
			fprintf(stderr,"%s: %s\n",config.hostsocket,context->errstr);
		}

		context.reset();
		context = nullptr;
		return REDIS_ERR;
	}

    socket.setkeepAlive(context->fd,REDIS_CLI_KEEPALIVE_INTERVAL);
    /* Do AUTH and select the right DB. */
    //if (cliAuth() != REDIS_OK) { return REDIS_ERR; }
    //if (cliSelect() != REDIS_OK) { return REDIS_ERR; }

    return REDIS_OK;

}

int RedisCli::cliAuth()
{
	RedisReply *reply;
	if(config.auth == nullptr) { return REDIS_ERR; }
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

sds RedisCli::readArgFromStdin(void)
{
	char buf[1024];
	sds arg = sdsempty();

	while (1)
	{
		int nread = ::read(fileno(stdin),buf,1024);
		if (nread == 0)
		{
			break;
		}
		else if (nread == -1)
		{
			perror("Reading from standard input");
			exit(1);
		}
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
	else
    {
		retval = issueCommand(argc,argv);
	}
	return retval;
}

void RedisCli::cliPrintContextError()
{
	if (context == nullptr) { return; }
	fprintf(stderr,"Error: %s\n",context->errstr);
}

void RedisCli::cliRefreshPrompt()
{
	int len;
	if (config.evalLdb) { return; }

	if (config.hostsocket != nullptr)
		len = snprintf(config.prompt,sizeof(config.prompt),"redis %s",
					   config.hostsocket);
	else
		len = snprintf(config.prompt,sizeof(config.prompt), strchr(config.hostip,':') ?
									 "[%s]:%d" : "%s:%d", config.hostip, config.hostport);
	/* Add [dbnum] if needed */
	if (config.dbnum != 0)
		len += snprintf(config.prompt+len,sizeof(config.prompt)-len,"[%d]",
						config.dbnum);
	snprintf(config.prompt+len,sizeof(config.prompt)-len,"> ");
}

int RedisCli::isColorTerm()
{
	char *t = getenv("TERM");
	return t != nullptr && strstr(t,"xterm") != nullptr;
}

sds RedisCli::sdscatcolor(sds o,char *s,size_t len,char *color)
{
	if (!isColorTerm()) return sdscatlen(o,s,len);

	int bold = strstr(color,"bold") != nullptr;
	int ccode = 37; /* Defaults to white. */
	if (strstr(color,"red")) ccode = 31;
	else if (strstr(color,"green")) ccode = 32;
	else if (strstr(color,"yellow")) ccode = 33;
	else if (strstr(color,"blue")) ccode = 34;
	else if (strstr(color,"magenta")) ccode = 35;
	else if (strstr(color,"cyan")) ccode = 36;
	else if (strstr(color,"white")) ccode = 37;

	o = sdscatfmt(o,"\033[%i;%i;49m",bold,ccode);
	o = sdscatlen(o,s,len);
	o = sdscat(o,"\033[0m");
	return o;
}


sds RedisCli::sdsCatColorizedLdbReply(sds o,char *s,size_t len)
{
	char *color = "white";
	if (strstr(s,"<debug>")) color = "bold";
	if (strstr(s,"<redis>")) color = "green";
	if (strstr(s,"<reply>")) color = "cyan";
	if (strstr(s,"<error>")) color = "red";
	if (strstr(s,"<hint>")) color = "bold";
	if (strstr(s,"<value>") || strstr(s,"<retval>")) color = "magenta";
	if (len > 4 && isdigit(s[3]))
	{
		if (s[1] == '>') color = "yellow"; /* Current line. */
		else if (s[2] == '#') color = "bold"; /* Break point. */
	}
	return sdscatcolor(o,s,len,color);
}

sds RedisCli::cliFormatReplyRaw(RedisReply *r)
{
	sds out = sdsempty(), tmp;
	size_t i;

	switch (r->type)
	{
		case REDIS_REPLY_NIL:
		/* Nothing... */
			break;
		case REDIS_REPLY_ERROR:
			out = sdscatlen(out,r->str,r->len);
			out = sdscatlen(out,"\n",1);
			break;
		case REDIS_REPLY_STATUS:
		case REDIS_REPLY_STRING:
			if (r->type == REDIS_REPLY_STATUS && config.evalLdb)
			{
				/* The Lua debugger replies with arrays of simple (status)
				 * strings. We colorize the output for more fun if this
				 * is a debugging session. */

				/* Detect the end of a debugging session. */
				if (strstr(r->str,"<endsession>") == r->str)
				{
					config.enableLdbOnEval = 0;
					config.evalLdb = 0;
					config.evalLdbEnd = 1; /* Signal the caller session ended. */
					config.output = OUTPUT_STANDARD;
					cliRefreshPrompt();
				}
				else
				{
					out = sdsCatColorizedLdbReply(out,r->str,r->len);
				}
			}
			else
			{
				out = sdscatlen(out,r->str,r->len);
			}
			break;
		case REDIS_REPLY_INTEGER:
			out = sdscatprintf(out,"%lld",r->integer);
			break;
			case REDIS_REPLY_ARRAY:
			for (i = 0; i < r->elements; i++)
			{
				if (i > 0) out = sdscat(out,config.mbDelim);
				tmp = cliFormatReplyRaw(r->element[i]);
				out = sdscatlen(out,tmp,sdslen(tmp));
				sdsfree(tmp);
			}
			break;
		default:
		fprintf(stderr,"Unknown reply type: %d\n", r->type);
		exit(1);
	}
	return out;
}

sds RedisCli::cliFormatReplyCSV(RedisReply *r)
{
	unsigned int i;
	sds out = sdsempty();
	switch (r->type)
	{
		case REDIS_REPLY_ERROR:
			out = sdscat(out,"ERROR,");
			out = sdscatrepr(out,r->str,strlen(r->str));
			break;
		case REDIS_REPLY_STATUS:
			out = sdscatrepr(out,r->str,r->len);
			break;
		case REDIS_REPLY_INTEGER:
			out = sdscatprintf(out,"%lld",r->integer);
			break;
		case REDIS_REPLY_STRING:
			out = sdscatrepr(out,r->str,r->len);
			break;
		case REDIS_REPLY_NIL:
			out = sdscat(out,"NIL");
			break;
		case REDIS_REPLY_ARRAY:
			for (i = 0; i < r->elements; i++)
			{
				sds tmp = cliFormatReplyCSV(r->element[i]);
				out = sdscatlen(out,tmp,sdslen(tmp));
				if (i != r->elements - 1) { out = sdscat(out,","); }
				sdsfree(tmp);
			}
			break;
		default:
			fprintf(stderr,"Unknown reply type: %d\n",r->type);
			exit(1);
	}
	return out;
}

char **RedisCli::convertToSds(int count,char** args)
{
	int j;
	char **sds =(char **)zmalloc(sizeof(char*)*count);
	for(j = 0; j < count; j++) { sds[j] = sdsnew(args[j]); }
	return sds;
}

sds RedisCli::cliFormatReplyTTY(RedisReply *r,char *prefix)
{
	sds out = sdsempty();
	switch (r->type)
	{
		case REDIS_REPLY_ERROR:
			out = sdscatprintf(out,"(error) %s\n",r->str);
			break;
		case REDIS_REPLY_STATUS:
			out = sdscat(out,r->str);
			out = sdscat(out,"\n");
			break;
		case REDIS_REPLY_INTEGER:
			out = sdscatprintf(out,"(integer) %lld\n",r->integer);
			break;
		case REDIS_REPLY_STRING:
			/* If you are producing output for the standard output we want
            * a more interesting output with quoted characters and so forth */
			out = sdscatrepr(out,r->str,r->len);
			out = sdscat(out,"\n");
			break;
		case REDIS_REPLY_NIL:
			out = sdscat(out,"(nil)\n");
			break;
		case REDIS_REPLY_ARRAY:
			if (r->elements == 0)
			{
				out = sdscat(out,"(empty list or set)\n");
			}
			else
			{
				unsigned int i, idxlen = 0;
				char _prefixlen[16];
				char _prefixfmt[16];
				sds _prefix;
				sds tmp;

				/* Calculate chars needed to represent the largest index */
				i = r->elements;
				do
				{
					idxlen++;
					i /= 10;
				} while(i);

				/* Prefix for nested multi bulks should grow with idxlen+2 spaces */
				memset(_prefixlen,' ',idxlen+2);
				_prefixlen[idxlen+2] = '\0';
				_prefix = sdscat(sdsnew(prefix),_prefixlen);

				/* Setup prefix format for every entry */
				snprintf(_prefixfmt,sizeof(_prefixfmt),"%%s%%%ud) ",idxlen);

				for (i = 0; i < r->elements; i++)
				{
					/* Don't use the prefix for the first element, as the parent
					 * caller already prepended the index number. */
					out = sdscatprintf(out,_prefixfmt,i == 0 ? "" : prefix,i+1);

					/* Format the multi bulk entry */
					tmp = cliFormatReplyTTY(r->element[i],_prefix);
					out = sdscatlen(out,tmp,sdslen(tmp));
					sdsfree(tmp);
				}
				sdsfree(_prefix);
			}
			break;
		default:
			fprintf(stderr,"Unknown reply type: %d\n", r->type);
			exit(1);
	}
	return out;
}

int RedisCli::cliReadReply(int outputRawString)
{
    RedisReply *reply;
    sds out = nullptr;
    int output = 1;
    if (context->redisGetReply(&reply) != REDIS_OK)
    {
        if (config.shutdown)
        {
            context.reset();
            context = nullptr;
            return REDIS_OK;
        }

        if(config.interactive)
        {
            if (context->err == REDIS_ERR_IO &&
                    (errno == ECONNRESET || errno == EPIPE))
            {
                return REDIS_ERR;
            }

            if (context->err == REDIS_ERR_EOF)
            {
                return REDIS_ERR;
            }
        }

        cliPrintContextError();
        exit(1);
        return REDIS_ERR; /* avoid compiler warning */
    }

    assert(reply != nullptr);
    config.lastCmdType = reply->type;

	/* Check if we need to connect to a different node and reissue the
    * request. */
	if (config.clusterMode && reply->type == REDIS_REPLY_ERROR &&
		(!strncmp(reply->str,"MOVED",5) || !strcmp(reply->str,"ASK")))
	{
		char *p = reply->str, *s;
		int slot;

		output = 0;
		/* Comments show the position of the pointer as:
         *
         * [S] for pointer 's'
         * [P] for pointer 'p'
         */
		s = strchr(p,' ');      /* MOVED[S]3999 127.0.0.1:6381 */
		p = strchr(s+1,' ');    /* MOVED[S]3999[P]127.0.0.1:6381 */
		*p = '\0';
		slot = atoi(s+1);
		s = strrchr(p+1,':');    /* MOVED 3999[P]127.0.0.1[S]6381 */
		*s = '\0';
		sdsfree(config.hostip);
		config.hostip = sdsnew(p+1);
		config.hostport = atoi(s+1);
		if (config.interactive)
			printf("-> Redirected to slot [%d] located at %s:%d\n",
				   slot, config.hostip, config.hostport);
		config.clusterReissueCommand = 1;
		cliRefreshPrompt();
	}

	if (output)
	{
		if (outputRawString)
		{
			out = cliFormatReplyRaw(reply);
		}
		else
		{
			if (config.output == OUTPUT_RAW)
            {
				out = cliFormatReplyRaw(reply);
				out = sdscat(out,"\n");
			}
			else if (config.output == OUTPUT_STANDARD)
			{
				out = cliFormatReplyTTY(reply,"");
			}
			else if (config.output == OUTPUT_CSV)
			{
				out = cliFormatReplyCSV(reply);
				out = sdscat(out,"\n");
			}
		}

		fwrite(out,sdslen(out),1,stdout);
		sdsfree(out);
	}

	freeReply(reply);
	return REDIS_OK;
}

int RedisCli::cliSendCommand(int argc,char **argv,int repeat)
{
	char *command = argv[0];
	size_t *argvlen;
	int j,outputRaw;

	if (!config.evalLdb && /* In debugging mode, let's pass "help" to Redis. */
		(!strcasecmp(command,"help") || !strcasecmp(command,"?")))
	{
		cliOutputHelp(--argc,++argv);
		return REDIS_OK;
	}

    if (context == nullptr) { return REDIS_ERR; }

    outputRaw = 0;
	if (!strcasecmp(command,"info") ||
		(argc >= 2 && !strcasecmp(command,"debug") &&
		 !strcasecmp(argv[1],"htstats")) ||
		(argc >= 2 && !strcasecmp(command,"memory") &&
		 (!strcasecmp(argv[1],"malloc-stats") ||
		  !strcasecmp(argv[1],"doctor"))) ||
		(argc == 2 && !strcasecmp(command,"cluster") &&
		 (!strcasecmp(argv[1],"nodes") ||
		  !strcasecmp(argv[1],"info"))) ||
		(argc == 2 && !strcasecmp(command,"client") &&
		 !strcasecmp(argv[1],"list")) ||
		(argc == 3 && !strcasecmp(command,"latency") &&
		 !strcasecmp(argv[1],"graph")) ||
		(argc == 2 && !strcasecmp(command,"latency") &&
		 !strcasecmp(argv[1],"doctor")))
	{
		outputRaw = 1;
	}

	if (!strcasecmp(command,"shutdown"))  { config.shutdown = 1; }
	if (!strcasecmp(command,"monitor")) { config.monitorMde = 1; }
	if (!strcasecmp(command,"subscribe") ||
		!strcasecmp(command,"psubscribe"))  { config.pubsubMode = 1; }
	if (!strcasecmp(command,"sync") ||
		!strcasecmp(command,"psync")) { config.slaveMode = 1; }

	/* When the user manually calls SCRIPT DEBUG, setup the activation of
 	* debugging mode on the next eval if needed. */
	if (argc == 3 && !strcasecmp(argv[0],"script") &&
		!strcasecmp(argv[1],"debug"))
	{
		if (!strcasecmp(argv[2],"yes") || !strcasecmp(argv[2],"sync"))
		{
			config.enableLdbOnEval = 1;
		}
		else
		{
			config.enableLdbOnEval = 0;
		}
	}

	/* Actually activate LDB on EVAL if needed. */
	if (!strcasecmp(command,"eval") && config.enableLdbOnEval)
	{
		config.evalLdb = 1;
		config.output = OUTPUT_RAW;
	}

	/* Setup argument length */
	argvlen = (size_t*)zmalloc(argc * sizeof(size_t));
	for (j = 0; j < argc; j++)
	{
	    argvlen[j] = sdslen(argv[j]);
	}

	while (repeat-- > 0)
	{
		context->redisAppendCommandArgv(argc,(const char**)argv,argvlen);
		while (config.monitorMde)
		{
			if (cliReadReply(outputRaw) != REDIS_OK) { exit(1); }
			fflush(stdout);
		}

		if (config.pubsubMode)
		{
			if (config.output != OUTPUT_RAW)
			{
			    printf("Reading messages... (press Ctrl-C to quit)\n");
			}

			while (1)
			{
			    if (cliReadReply(outputRaw) != REDIS_OK)  { exit(1); }
			}
		}

		if (config.slaveMode)
		{
			printf("Entering slave output mode...  (press Ctrl-C to quit)\n");
			//slaveMode();
			config.slaveMode = 0;
			zfree(argvlen);
			return REDIS_ERR;  /* Error = slaveMode lost connection to master */
		}

		if (cliReadReply(outputRaw) != REDIS_OK)
        {
			zfree(argvlen);
			return REDIS_ERR;
		}
		else
		{
			/* Store database number when SELECT was successfully executed. */
			if (!strcasecmp(command,"select") && argc == 2 && config.lastCmdType != REDIS_REPLY_ERROR)
			{
				config.dbnum = atoi(argv[1]);
				cliRefreshPrompt();
			}
			else if (!strcasecmp(command,"auth") && argc == 2)
			{
				cliSelect();
			}
		}

		if (config.interval) { usleep(config.interval); }
		fflush(stdout); /* Make it grep friendly */
	}

	zfree(argvlen);
	return REDIS_OK;
}

void RedisCli::cliOutputGenericHelp()
{
	printf(
			"redis-cli %s\n"
			"To get help about Redis commands type:\n"
			"      \"help @<group>\" to get a list of commands in <group>\n"
			"      \"help <command>\" for help on <command>\n"
			"      \"help <tab>\" to get a list of possible help topics\n"
			"      \"quit\" to exit\n"
			"\n"
			"To set redis-cli preferences:\n"
			"      \":set hints\" enable online hints\n"
			"      \":set nohints\" disable online hints\n"
			"Set your preferences in ~/.redisclirc\n");
}

void RedisCli::cliInitHelp()
{
	int commandslen = sizeof(CommandHelp)/sizeof(struct CommandHelp);
	int groupslen = sizeof(CommandGroups)/sizeof(char*);
	int i,len,pos = 0;
	HelpEntry tmp;

	helpEntriesLen = len = commandslen + groupslen;
	helpEntries = (HelpEntry*)zmalloc(sizeof(HelpEntry)*len);

	for (i = 0; i < groupslen; i++)
	{
		tmp.argc = 1;
		tmp.argv = (sds*)zmalloc(sizeof(sds));
		tmp.argv[0] = sdscatprintf(sdsempty(),"@%s",CommandGroups[i]);
		tmp.full = tmp.argv[0];
		tmp.type = CLI_HELP_GROUP;
		tmp.org = nullptr;
		helpEntries[pos++] = tmp;
	}

	for (i = 0; i < commandslen; i++)
	{
		tmp.argv = sdssplitargs(commandHelp[i].name,&tmp.argc);
		tmp.full = sdsnew(commandHelp[i].name);
		tmp.type = CLI_HELP_COMMAND;
		tmp.org = &commandHelp[i];
		helpEntries[pos++] = tmp;
	}
}

void RedisCli::cliOutputCommandHelp(struct CommandHelp *help,int group)
{
	printf("\r\n  \x1b[1m%s\x1b[0m \x1b[90m%s\x1b[0m\r\n",help->name,help->params);
	printf("  \x1b[33msummary:\x1b[0m %s\r\n",help->summary);
	printf("  \x1b[33msince:\x1b[0m %s\r\n",help->since);
	if (group)
	{
		printf("  \x1b[33mgroup:\x1b[0m %s\r\n",CommandGroups[help->group]);
	}
}

void RedisCli::cliOutputHelp(int argc,char **argv)
{
	int i, j, len;
	int group = -1;
	HelpEntry *entry;
	struct CommandHelp *help;

	if (argc == 0)
	{
		cliOutputGenericHelp();
		return;
	}
	else if (argc > 0 && argv[0][0] == '@')
	{
		len = sizeof(CommandGroups)/sizeof(char*);
		for (i = 0; i < len; i++)
		{
			if (strcasecmp(argv[0]+1,CommandGroups[i]) == 0)
			{
				group = i;
				break;
			}
		}
	}

	assert(argc > 0);
	for (i = 0; i < helpEntriesLen; i++)
	{
		entry = &helpEntries[i];
		if (entry->type != CLI_HELP_COMMAND) continue;

		help = entry->org;
		if (group == -1)
		{
			/* Compare all arguments */
			if (argc == entry->argc)
			{
				for (j = 0; j < argc; j++)
				{
					if (strcasecmp(argv[j],entry->argv[j]) != 0) break;
				}

				if (j == argc)
				{
					cliOutputCommandHelp(help,1);
				}
			}
		}
		else
		{
			if (group == help->group)
			{
				cliOutputCommandHelp(help,0);
			}
		}
	}
	printf("\r\n");

}

int RedisCli::issuseCommandRepeat(int argc,char **argv,int repeat)
{
	while(1)
	{
		config.clusterReissueCommand = 0;
		if (cliSendCommand(argc,argv,repeat) != REDIS_OK)
		{
			cliConnect(1);
			/* If we still cannot send the command print error.
            * We'll try to reconnect the next time. */
			if (cliSendCommand(argc,argv,repeat) != REDIS_OK)
			{
				cliPrintContextError();
				return REDIS_ERR;
			}
		}

        /* Issue the command again if we got redirected in cluster mode */
        if (config.clusterMode && config.clusterReissueCommand)
        {
            cliConnect(1);
        }
        else
        {
            break;
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


























