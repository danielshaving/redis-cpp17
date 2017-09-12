#include "xHiredis.h"
static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

const char * ip;
int32_t port;


xRedisContextPtr connect()
{
	return redisConnect(ip,port);
}

void  disconnect(xRedisContextPtr c)
{
	if(c->fd)
	{
		::close(c->fd);
	}
	c->init();

}


void testCommand(xRedisContextPtr c)
{
	redisReply *reply;

	reply = ( redisReply *)redisCommand(c,"PING");
	if(reply != nullptr)
	printf("PING: %s\n", reply->str);
	freeReply(reply);


	reply =  ( redisReply *)redisCommand(c,"SET %s %s", "foo", "hello world");
	printf("SET: %s\n", reply->str);
	freeReply(reply);


	reply =  ( redisReply *)redisCommand(c,"SET %b %b", "bar", (size_t) 3, "hello", (size_t) 5);
	printf("SET (binary API): %s\n", reply->str);
	freeReply(reply);

	reply =  ( redisReply *)redisCommand(c,"GET foo");
	printf("GET foo: %s\n", reply->str);
	freeReply(reply);

	reply =  ( redisReply *)redisCommand(c,"INCR counter");
	printf("INCR counter: %lld\n", reply->integer);
	freeReply(reply);

	reply = ( redisReply *) redisCommand(c,"INCR counter");
	printf("INCR counter: %lld\n", reply->integer);
	freeReply(reply);

	reply = ( redisReply *) redisCommand(c,"DEL mylist");
	freeReply(reply);
	for (int j = 0; j < 10; j++) {
		char buf[64];

		snprintf(buf,64,"%d",j);
		reply =  ( redisReply *)redisCommand(c,"LPUSH mylist element-%s", buf);
		freeReply(reply);
	}

	reply =  ( redisReply *)redisCommand(c,"LRANGE mylist 0 -1");
	if (reply->type == REDIS_REPLY_ARRAY) {
		for (int j = 0; j < reply->elements; j++) {
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


void testBlockingConnectionTimeOuts(xRedisContextPtr c)
{
	redisReply *reply;
	xSocket socket;
	ssize_t s;
	const char *cmd = "DEBUG SLEEP 3\r\n";
	struct timeval tv;
	test("Successfully completes a command when the timeout is not exceeded: ");
	reply = (redisReply*)redisCommand(c,"SET foo fast");
	freeReply(reply);

	tv.tv_sec = 0; 
	tv.tv_usec = 10000;
	socket.setTimeOut(c->fd,tv);
	reply = (redisReply*)redisCommand(c,"GET foo");
	test_cond(reply != nullptr && reply->type == REDIS_REPLY_STRING && memcmp(reply->str,"fast",4) == 0);
	freeReply(reply);
	disconnect(c);
	c.reset();
	c = connect();

	tv.tv_sec = 0;
	tv.tv_usec = 10000;
	socket.setTimeOut(c->fd,tv);

	test("Does not return a reply when the command times out: ");
	s = ::write(c->fd,cmd,strlen(cmd));
	reply =  (redisReply*)redisCommand(c, "GET foo");
	test_cond(s > 0 && reply == nullptr && c->err == REDIS_ERR_IO && strcmp(c->errstr, "Resource temporarily unavailable") == 0);
	freeReply(reply);
	c.reset();
	c = connect();

}


int main(int argc, char* argv[])
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

		xRedisContextPtr c;
		redisReply *reply;
	
		c = redisConnectWithTimeout(ip, port, timeout);
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
	
		testBlockingConnectionTimeOuts(c);
		testCommand(c);
		testFormatCommand();
		testReplyReader();
	}
	return 0;
}






