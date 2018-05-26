#include "hiredis.h"

const char *ip;
int32_t port;

void testCommand(RedisContextPtr c)
{
	redisReply *reply;

	reply = c->redisCommand("PING");
	if(reply != nullptr)
	printf("PING: %s\n", reply->str);
	freeReply(reply);

	reply = c->redisCommand("SET %s %s", "foo", "hello world");
	printf("SET: %s\n", reply->str);
	freeReply(reply);

	reply = c->redisCommand("SET %b %b", "bar", (size_t) 3, "hello", (size_t) 5);
	printf("SET (binary API): %s\n", reply->str);
	freeReply(reply);

	reply = c->redisCommand("GET foo");
	printf("GET foo: %s\n", reply->str);
	freeReply(reply);

	reply = c->redisCommand("INCR counter");
	printf("INCR counter: %lld\n", reply->integer);
	freeReply(reply);

	reply = c->c->redisCommand("INCR counter");
	printf("INCR counter: %lld\n", reply->integer);
	freeReply(reply);

	reply = c->redisCommand("DEL mylist");
	freeReply(reply);
	for (int j = 0; j < 10; j++)
	{
		char buf[64];

		snprintf(buf,64,"%d",j);
		reply = c->redisCommand("LPUSH mylist element-%s", buf);
		freeReply(reply);
	}

	reply = c->redisCommand("LRANGE mylist 0 -1");
	if (reply->type == REDIS_REPLY_ARRAY)
	{
		for (int j = 0; j < reply->elements; j++)
		{
			printf("%u) %s\n", j, reply->element[j]->str);
		}
	}
	freeReply(reply);
}

void testFormatCommand()
{
	char *cmd;
	int len;

	test("Format command without interpolation: ");
	len = redisFormatCommand(&cmd,"SET foo bar");
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(3+2)+4+(3+2));
	zfree(cmd);

	test("Format command with %%s string interpolation: ");
	len = redisFormatCommand(&cmd,"SET %s %s","foo","bar");
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(3+2)+4+(3+2));
	zfree(cmd);

	test("Format command with %%s and an empty string: ");
	len = redisFormatCommand(&cmd,"SET %s %s","foo","");
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$0\r\n\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(3+2)+4+(0+2));
	zfree(cmd);

	test("Format command with an empty string in between proper interpolations: ");
	len = redisFormatCommand(&cmd,"SET %s %s","","foo");
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$3\r\nfoo\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(0+2)+4+(3+2));
	zfree(cmd);

	test("Format command with %%b string interpolation: ");
	len = redisFormatCommand(&cmd,"SET %b %b","foo",(size_t)3,"b\0r",(size_t)3);
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nb\0r\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(3+2)+4+(3+2));
	zfree(cmd);

	test("Format command with %%b and an empty string: ");
	len = redisFormatCommand(&cmd,"SET %b %b","foo",(size_t)3,"",(size_t)0);
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$0\r\n\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(3+2)+4+(0+2));
	zfree(cmd);

	test("Format command with literal %%: ");
	len = redisFormatCommand(&cmd,"SET %% %%");
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$1\r\n%\r\n$1\r\n%\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(1+2)+4+(1+2));
	zfree(cmd);

#define INTEGER_WIDTH_TEST(fmt, type) do {                                                \
	type value = 123;                                                                     \
	test("Format command with printf-delegation (" #type "): ");                          \
	len = redisFormatCommand(&cmd,"key:%08" fmt " str:%s", value, "hello");               \
	test_cond(strncmp(cmd,"*2\r\n$12\r\nkey:00000123\r\n$9\r\nstr:hello\r\n",len) == 0 && \
	    len == 4+5+(12+2)+4+(9+2));                                                       \
	zfree(cmd);                                                                            \
	} while(0)

#define FLOAT_WIDTH_TEST(type) do {                                                       \
	type value = 123.0;                                                                   \
	test("Format command with printf-delegation (" #type "): ");                          \
	len = redisFormatCommand(&cmd,"key:%08.3f str:%s", value, "hello");                   \
	test_cond(strncmp(cmd,"*2\r\n$12\r\nkey:0123.000\r\n$9\r\nstr:hello\r\n",len) == 0 && \
	    len == 4+5+(12+2)+4+(9+2));                                                       \
	zfree(cmd);                                                                            \
	} while(0)

	INTEGER_WIDTH_TEST("d", int);
	INTEGER_WIDTH_TEST("hhd", char);
	INTEGER_WIDTH_TEST("hd", short);
	INTEGER_WIDTH_TEST("ld", long);
	INTEGER_WIDTH_TEST("lld", long long);
	INTEGER_WIDTH_TEST("u", unsigned int);
	INTEGER_WIDTH_TEST("hhu", unsigned char);
	INTEGER_WIDTH_TEST("hu", unsigned short);
	INTEGER_WIDTH_TEST("lu", unsigned long);
	INTEGER_WIDTH_TEST("llu", unsigned long long);
	FLOAT_WIDTH_TEST(float);
	FLOAT_WIDTH_TEST(double);

	test("Format command with invalid printf format: ");
	len = redisFormatCommand(&cmd,"key:%08p %b",(void*)1234,"foo",(size_t)3);
	test_cond(len == -1);

	const char *argv[3];
	argv[0] = "SET";
	argv[1] = "foo\0xxx";
	argv[2] = "bar";
	size_t lens[3] = { 3, 7, 3 };
	int argc = 3;

	test("Format command by passing argc/argv without lengths: ");
	len = redisFormatCommandArgv(&cmd,argc,argv,nullptr);
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(3+2)+4+(3+2));
	zfree(cmd);

	test("Format command by passing argc/argv with lengths: ");
	len = redisFormatCommandArgv(&cmd,argc,argv,lens);
	test_cond(strncmp(cmd,"*3\r\n$3\r\nSET\r\n$7\r\nfoo\0xxx\r\n$3\r\nbar\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(7+2)+4+(3+2));
	zfree(cmd);

	sds sds_cmd;

	sds_cmd = sdsempty();
	test("Format command into sds by passing argc/argv without lengths: ");
	len = redisFormatSdsCommandArgv(&sds_cmd,argc,argv,nullptr);
	test_cond(strncmp(sds_cmd,"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(3+2)+4+(3+2));
	sdsfree(sds_cmd);

	sds_cmd = sdsempty();
	test("Format command into sds by passing argc/argv with lengths: ");
	len = redisFormatSdsCommandArgv(&sds_cmd,argc,argv,lens);
	test_cond(strncmp(sds_cmd,"*3\r\n$3\r\nSET\r\n$7\r\nfoo\0xxx\r\n$3\r\nbar\r\n",len) == 0 &&
	    len == 4+4+(3+2)+4+(7+2)+4+(3+2));
	sdsfree(sds_cmd);
}

void testReplyReader()
{
	std::shared_ptr<xRedisReader>  reader (new xRedisReader);
}

void testBlockingConnectionTimeOuts()
{
	RedisContextPtr c = redisConnect(ip,port);
	redisReply *reply;
	xSocket socket;
	ssize_t s;
	const char *cmd = "DEBUG SLEEP 3\r\n";
	struct timeval tv;
	test("Successfully completes a command when the timeout is not exceeded: ");
	reply = (redisReply*)c->redisCommand("SET foo fast");
	freeReply(reply);

	tv.tv_sec = 0; 
	tv.tv_usec = 10000;
	socket.setTimeOut(c->fd,tv);
	reply = (redisReply*)c->redisCommand("GET foo");
	test_cond(reply != nullptr && reply->type == REDIS_REPLY_STRING && memcmp(reply->str,"fast",4) == 0);
	freeReply(reply);

	test("Does not return a reply when the command times out: ");
	s = ::write(c->fd,cmd,strlen(cmd));
	reply =  (redisReply*)c->redisCommand( "GET foo");
	test_cond(s > 0 && reply == nullptr && c->err == REDIS_ERR_IO && strcmp(c->errstr, "Resource temporarily unavailable") == 0);
	freeReply(reply);

	test("Reconnect properly reconnects after a timeout: ");
	c.reset();
	c = redisConnect(ip,port);
	reply = (redisReply*)c->redisCommand( "PING");
	test_cond(reply != nullptr && reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "PONG") == 0);
	freeReply(reply);
}

void testBlockingConnection()
{
	RedisContextPtr c;
	redisReply *reply;
	Socket socket;
	c = redisConnect(ip,port);
	test("Is able to deliver commands: ");
	reply = (redisReply*)c->redisCommand("PING");
	test_cond(reply->type == REDIS_REPLY_STATUS &&
	strcasecmp(reply->str,"pong") == 0)
	freeReply(reply);

	test("Is a able to send commands verbatim: ");
	reply =(redisReply*) c->redisCommand("SET foo bar");
	test_cond (reply->type == REDIS_REPLY_STATUS &&
	strcasecmp(reply->str,"ok") == 0)
	freeReply(reply);

	test("%%s String interpolation works: ");
	reply = (redisReply*)c->redisCommand("SET %s %s","foo","hello world");
	freeReply(reply);
	reply = (redisReply*)c->redisCommand("GET foo");
	test_cond(reply->type == REDIS_REPLY_STRING &&
	strcmp(reply->str,"hello world") == 0);
	freeReply(reply);

	test("%%b String interpolation works: ");
	reply =(redisReply*) c->redisCommand("SET %b %b","foo",(size_t)3,"hello\x00world",(size_t)11);
	freeReply(reply);
	reply = (redisReply*)c->redisCommand("GET foo");
	test_cond(reply->type == REDIS_REPLY_STRING &&
	memcmp(reply->str,"hello\x00world",11) == 0)

	test("Binary reply length is correct: ");
	test_cond(reply->len == 11)
	freeReply(reply);

	test("Can parse nil replies: ");
	reply = (redisReply*)c->redisCommand("GET nokey");
	test_cond(reply->type == REDIS_REPLY_NIL)
	freeReply(reply);

	/* test 7 */
	test("Can parse integer replies: ");
	reply = (redisReply*)c->redisCommand("INCR mycounter");
	test_cond(reply->type == REDIS_REPLY_INTEGER && reply->integer == 1)
	freeReply(reply);

	test("Can parse multi bulk replies: ");
	freeReply(c->redisCommand("LPUSH mylist foo"));
	freeReply(c->redisCommand("LPUSH mylist bar"));
	reply = (redisReply*)c->redisCommand("LRANGE mylist 0 -1");
	test_cond(reply->type == REDIS_REPLY_ARRAY &&
	reply->elements == 2 &&
	!memcmp(reply->element[0]->str,"bar",3) &&
	!memcmp(reply->element[1]->str,"foo",3))
	freeReply(reply);

	/* m/e with multi bulk reply *before* other reply.
	* specifically test ordering of reply items to parse. */
	test("Can handle nested multi bulk replies: ");
	freeReply(c->redisCommand("MULTI"));
	freeReply(c->redisCommand("LRANGE mylist 0 -1"));
	freeReply(c->redisCommand("PING"));
	reply = (redisReply*)(c->redisCommand("EXEC"));
	test_cond(reply->type == REDIS_REPLY_ARRAY &&
	reply->elements == 2 &&
	reply->element[0]->type == REDIS_REPLY_ARRAY &&
	reply->element[0]->elements == 2 &&
	!memcmp(reply->element[0]->element[0]->str,"bar",3) &&
	!memcmp(reply->element[0]->element[1]->str,"foo",3) &&
	reply->element[1]->type == REDIS_REPLY_STATUS &&
	strcasecmp(reply->element[1]->str,"pong") == 0);
	freeReply(reply);
}

void testBlockingConecntionerros()
{
	RedisContextPtr c;
	test("Returns error when host cannot be resolved: ");
	c = redisConnect((char*)"text",port);
	test_cond(c->err == REDIS_ERR_OTHER &&
		(strcmp(c->errstr,"Name or service not known") == 0 ||
		 strcmp(c->errstr,"nodename nor servname provided, or not known") == 0 ||
		 strcmp(c->errstr,"No address associated with hostname") == 0 ||
		 strcmp(c->errstr,"Temporary failure in name resolution") == 0 ||
		 strcmp(c->errstr,"hostname nor servname provided, or not known") == 0 ||
		 strcmp(c->errstr,"no address associated with name") == 0))

	c.reset();
	test("Returns error when the port is not open: ");
	c = redisConnect("127.0.0.1", 1);
	test_cond(c->err == REDIS_ERR_IO &&strcmp(c->errstr,"Connection refused") == 0);
}

void testBlockIoerrors()
{
	RedisContextPtr c;
	Socket socket;
	redisReply * reply;
	void * _reply;
	int major, minor;
	c = redisConnect(ip,port);
	{
		const char *field = "redis_version:";
		char *p, *eptr;

		reply = (redisReply*)c->redisCommand("INFO");
		p = strstr(reply->str,field);
		major = strtol(p+strlen(field),&eptr,10);
		p = eptr+1; /* char next to the first "." */
		minor = strtol(p,&eptr,10);
		freeReply(reply);
	}

	test("Returns I/O error when the connection is lost: ");
	reply = (redisReply*)c->redisCommand("QUIT");
	if (major > 2 || (major == 2 && minor > 0))
	{
		/* > 2.0 returns OK on QUIT and read() should be issued once more
		 * to know the descriptor is at EOF. */
		test_cond(strcasecmp(reply->str,"OK") == 0 && redisGetReply(c,&_reply) == REDIS_ERR);
		freeReply(reply);
	} 
	else
	{
		test_cond(reply == nullptr);
	}
	
	assert(c->err == REDIS_ERR_EOF && strcmp(c->errstr,"Server closed the connection") == 0);
	c.reset();
	c = redisConnect(ip,port);
	test("Returns I/O error on socket timeout: ");
	struct timeval tv = { 0, 1000 };
	assert(socket.setTimeOut(c->fd,tv));
	test_cond(redisGetReply(c,&_reply) == REDIS_ERR &&
        c->err == REDIS_ERR_IO && errno == EAGAIN);
		
}

void testInvalidTimeOuterros()
{
	test("Set error when an invalid timeout usec value is given to redisConnectWithTimeout: ");
	{
		struct timeval timeout = { 0, 10000001 }; 
		RedisContextPtr c = redisConnectWithTimeout(ip,port,timeout);
		test_cond(c->err == REDIS_ERR_IO && strcmp(c->errstr, "Invalid timeout specified") == 0);
	}
	
	test("Set error when an invalid timeout sec value is given to redisConnectWithTimeout: ");
	{
		struct timeval timeout = { (((LONG_MAX) - 999) / 1000) + 1, 0 }; 
		RedisContextPtr c = redisConnectWithTimeout(ip,port,timeout);
		test_cond(c->err == REDIS_ERR_IO && strcmp(c->errstr, "Invalid timeout specified") == 0);
	}
}

void testAppendFormateedCommands()
{
	RedisContextPtr c;
	redisReply *reply;
	char *cmd;
	int len;
	c = redisConnect(ip,port);
	test("Append format command: ");
	len = redisFormatCommand(&cmd, "SET foo bar");
	test_cond(redisAppendFormattedCommand(c, cmd, len) == REDIS_OK);
	assert(redisGetReply(c, (void**)&reply) == REDIS_OK);

	zfree(cmd);
	freeReply(reply);
}

void testThroughPut()
{
	RedisContextPtr c = redisConnect(ip,port);
	redisReply **replies;
	int i,num;
	int64_t t1,t2;

	test("Throughput:\n");
	for(i = 0; i < 500; i++)
	{
		freeReply(c->redisCommand("LPUSH mylist foo"));
	}

	num = 1000;
	replies = (redisReply**)zmalloc(sizeof(redisReply*) * num );
	t1 = ustime();
	for(i = 0; i < num; i++)
	{
		replies[i] = (redisReply*)c->redisCommand("PING");
		assert(replies[i] != nullptr && replies[i]->type == REDIS_REPLY_STATUS);
	}

	t2 = ustime();
	for(i = 0; i < num; i++)
	{
		freeReply(replies[i]);
	}
	
	zfree(replies);
	printf("\t(%dx PING: %.3fs)\n", num, (t2-t1)/1000000.0);

	replies = (redisReply**)zmalloc(sizeof(redisReply*) * num );
	t1 = ustime();
	for (i = 0; i < num; i++) 
	{
	   replies[i] = (redisReply*)c->redisCommand("LRANGE mylist 0 499");
	   assert(replies[i] != nullptr && replies[i]->type == REDIS_REPLY_ARRAY);
	   assert(replies[i] != nullptr && replies[i]->elements == 500);
	}
	
	t2 = ustime();
	for (i = 0; i < num; i++) 
	{
		freeReply(replies[i]);
	}
	zfree(replies);
	printf("\t(%dx LRANGE with 500 elements: %.3fs)\n", num, (t2-t1)/1000000.0);


	num = 10000;
	replies = (redisReply**)zmalloc(sizeof(redisReply*)*num);
	for (i = 0; i < num; i++)
	{
		redisAppendCommand(c,"PING");
	}
	    
	t1 = ustime();
	for (i = 0; i < num; i++)
	{
	    assert(redisGetReply(c, (void**)&replies[i]) == REDIS_OK);
	    assert(replies[i] != NULL && replies[i]->type == REDIS_REPLY_STATUS);
	}
	
	t2 = ustime();
	for (i = 0; i < num; i++)
	{
		freeReply(replies[i]);	
	}

	zfree(replies);
	printf("\t(%dx PING (pipelined): %.3fs)\n", num, (t2-t1)/1000000.0);

	replies =  (redisReply**)zmalloc(sizeof(redisReply*)*num);
	for (i = 0; i < num; i++)
	{
	    redisAppendCommand(c,"LRANGE mylist 0 499");
	}
	
	t1 = ustime();
	for (i = 0; i < num; i++)
	{
	    assert(redisGetReply(c, (void**)&replies[i]) == REDIS_OK);
	    assert(replies[i] != NULL && replies[i]->type == REDIS_REPLY_ARRAY);
	    assert(replies[i] != NULL && replies[i]->elements == 500);
	}
	
	t2 = ustime();
	for (i = 0; i < num; i++)
	{
		freeReply(replies[i]);
	}
	
	zfree(replies);
	printf("\t(%dx LRANGE with 500 elements (pipelined): %.3fs)\n", num, (t2-t1)/1000000.0);
}

int main(int argc,char* argv[])
{
	if (argc != 3)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> \n ");
	}
	else
	{
		ip = argv[1];
		port = static_cast<uint16_t>(atoi(argv[2]));
		struct timeval timeout = { 1, 500000 }; // 1.5 seconds

		RedisContextPtr c;
		redisReply *reply;
	
		c = redisConnectWithTimeout(ip,port,timeout);
		if (c == nullptr || c->err)
		{
			if (c) 
			{
				printf("Connection error: %s\n", c->errstr);
			} 
			else
			{
				printf("Connection error: can't allocate redis context\n");
			}
			
			exit(1);
		}

		testThroughPut();
		testCommand(c);
		testBlockIoerrors();
		testBlockingConecntionerros();
		testAppendFormateedCommands();
		testInvalidTimeOuterros();
		testBlockingConnection();
		testFormatCommand();
		testReplyReader();
		testBlockingConnectionTimeOuts();
	}
	return 0;
}






