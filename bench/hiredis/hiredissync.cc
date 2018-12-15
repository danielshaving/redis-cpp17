#include "hiredis.h"
#include "socket.h"

const char *ip;
int32_t port;

/* The following lines make up our testing "framework" :) */
static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define testCond(_c) if(_c) printf("PASSED\n"); else {printf("FAILED\n"); fails++;}

void testCommand(RedisContextPtr c)
{
	RedisReplyPtr reply;
	reply = c->redisCommand("PING");
	if (reply != nullptr)
		printf("PING: %s\n", reply->str);

	reply = c->redisCommand("SET %s %s", "foo", "hello world");
	printf("SET: %s\n", reply->str);

	reply = c->redisCommand("SET %b %b", "bar", (size_t)3, "hello", (size_t)5);
	printf("SET (binary API): %s\n", reply->str);

	reply = c->redisCommand("GET foo");
	printf("GET foo: %s\n", reply->str);

	reply = c->redisCommand("INCR counter");
	printf("INCR counter: %lld\n", reply->integer);

	reply = c->redisCommand("INCR counter");
	printf("INCR counter: %lld\n", reply->integer);

	reply = c->redisCommand("DEL mylist");

	for (int j = 0; j < 10; j++)
	{
		char buf[64];

		snprintf(buf, 64, "%d", j);
		reply = c->redisCommand("LPUSH mylist element-%s", buf);
	}

	reply = c->redisCommand("LRANGE mylist 0 -1");
	if (reply->type == REDIS_REPLY_ARRAY)
	{
		for (int j = 0; j < reply->element.size(); j++)
		{
			printf("%u) %s\n", j, reply->element[j]->str);
		}
	}
}

void testFormatCommand()
{
	char *cmd;
	int len;

	test("Format command without interpolation: ");
	len = redisFormatCommand(&cmd, "SET foo bar");
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (3 + 2) + 4 + (3 + 2));
	zfree(cmd);

	test("Format command with %%s string interpolation: ");
	len = redisFormatCommand(&cmd, "SET %s %s", "foo", "bar");
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (3 + 2) + 4 + (3 + 2));
	zfree(cmd);

	test("Format command with %%s and an empty string: ");
	len = redisFormatCommand(&cmd, "SET %s %s", "foo", "");
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$0\r\n\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (3 + 2) + 4 + (0 + 2));
	zfree(cmd);

	test("Format command with an empty string in between proper interpolations: ");
	len = redisFormatCommand(&cmd, "SET %s %s", "", "foo");
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$0\r\n\r\n$3\r\nfoo\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (0 + 2) + 4 + (3 + 2));
	zfree(cmd);

	test("Format command with %%b string interpolation: ");
	len = redisFormatCommand(&cmd, "SET %b %b", "foo", (size_t)3, "b\0r", (size_t)3);
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nb\0r\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (3 + 2) + 4 + (3 + 2));
	zfree(cmd);

	test("Format command with %%b and an empty string: ");
	len = redisFormatCommand(&cmd, "SET %b %b", "foo", (size_t)3, "", (size_t)0);
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$0\r\n\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (3 + 2) + 4 + (0 + 2));
	zfree(cmd);

	test("Format command with literal %%: ");
	len = redisFormatCommand(&cmd, "SET %% %%");
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$1\r\n%\r\n$1\r\n%\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (1 + 2) + 4 + (1 + 2));
	zfree(cmd);

#define INTEGER_WIDTH_TEST(fmt, type) do {                                                \
	type value = 123;                                                                     \
	test("Format command with printf-delegation (" #type "): ");                          \
	len = redisFormatCommand(&cmd,"key:%08" fmt " str:%s", value, "hello");               \
	testCond(strncmp(cmd,"*2\r\n$12\r\nkey:00000123\r\n$9\r\nstr:hello\r\n",len) == 0 && \
	    len == 4+5+(12+2)+4+(9+2));                                                       \
	zfree(cmd);                                                                            \
	} while(0)

#define FLOAT_WIDTH_TEST(type) do {                                                       \
	type value = 123.0;                                                                   \
	test("Format command with printf-delegation (" #type "): ");                          \
	len = redisFormatCommand(&cmd,"key:%08.3f str:%s", value, "hello");                   \
	testCond(strncmp(cmd,"*2\r\n$12\r\nkey:0123.000\r\n$9\r\nstr:hello\r\n",len) == 0 && \
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
	len = redisFormatCommand(&cmd, "key:%08p %b", (void*)1234, "foo", (size_t)3);
	testCond(len == -1);

	const char *argv[3];
	argv[0] = "SET";
	argv[1] = "foo\0xxx";
	argv[2] = "bar";
	size_t lens[3] = { 3, 7, 3 };
	int argc = 3;

	test("Format command by passing argc/argv without lengths: ");
	len = redisFormatCommandArgv(&cmd, argc, argv, nullptr);
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (3 + 2) + 4 + (3 + 2));
	zfree(cmd);

	test("Format command by passing argc/argv with lengths: ");
	len = redisFormatCommandArgv(&cmd, argc, argv, lens);
	testCond(strncmp(cmd, "*3\r\n$3\r\nSET\r\n$7\r\nfoo\0xxx\r\n$3\r\nbar\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (7 + 2) + 4 + (3 + 2));
	zfree(cmd);

	sds sds_cmd;

	sds_cmd = sdsempty();
	test("Format command into sds by passing argc/argv without lengths: ");
	len = redisFormatSdsCommandArgv(&sds_cmd, argc, argv, nullptr);
	testCond(strncmp(sds_cmd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (3 + 2) + 4 + (3 + 2));
	sdsfree(sds_cmd);

	sds_cmd = sdsempty();
	test("Format command into sds by passing argc/argv with lengths: ");
	len = redisFormatSdsCommandArgv(&sds_cmd, argc, argv, lens);
	testCond(strncmp(sds_cmd, "*3\r\n$3\r\nSET\r\n$7\r\nfoo\0xxx\r\n$3\r\nbar\r\n", len) == 0 &&
		len == 4 + 4 + (3 + 2) + 4 + (7 + 2) + 4 + (3 + 2));
	sdsfree(sds_cmd);
}

void testReplyReader()
{

}

void testBlockingConnectionTimeOuts()
{
	RedisContextPtr c = redisConnect(ip, port);
	RedisReplyPtr reply;
	ssize_t s;
	const char *cmd = "DEBUG SLEEP 3\r\n";
	struct timeval tv;
	test("Successfully completes a command when the timeout is not exceeded: ");
	reply = c->redisCommand("SET foo fast");

	tv.tv_sec = 0;
	tv.tv_usec = 10000;
	Socket::setTimeOut(c->fd, tv);
	reply = c->redisCommand("GET foo");
	testCond(reply != nullptr && reply->type == REDIS_REPLY_STRING && memcmp(reply->str, "fast", 4) == 0);

	test("Does not return a reply when the command times out: ");
#ifdef _WIN64
	s = ::send(c->fd, cmd, strlen(cmd), 0);
#else
	s = ::write(c->fd, cmd, strlen(cmd));
#endif
	reply = c->redisCommand("GET foo");
	testCond(s > 0 && reply == nullptr && c->err == REDIS_ERR_IO && strcmp(c->errstr, "Resource temporarily unavailable") == 0);

	test("Reconnect properly reconnects after a timeout: ");
	c.reset();
	c = redisConnect(ip, port);
	reply = c->redisCommand("PING");
	testCond(reply != nullptr && reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "PONG") == 0);
}

void testBlockingConnection()
{
	RedisContextPtr c;
	RedisReplyPtr reply;
	c = redisConnect(ip, port);
	test("Is able to deliver commands: ");
	reply = c->redisCommand("PING");
	testCond(reply->type == REDIS_REPLY_STATUS &&
		strcmp(reply->str, "PONG") == 0)

	test("Is a able to send commands verbatim: ");
	reply = c->redisCommand("SET foo bar");
	testCond(reply->type == REDIS_REPLY_STATUS &&
		strcmp(reply->str, "OK") == 0)

	test("%%s String interpolation works: ");
	reply = c->redisCommand("SET %s %s", "foo", "hello world");
	reply = c->redisCommand("GET foo");
	testCond(reply->type == REDIS_REPLY_STRING &&
		strcmp(reply->str, "hello world") == 0);

	test("%%b String interpolation works: ");
	reply = c->redisCommand("SET %b %b", "foo", (size_t)3, "hello\x00world", (size_t)11);
	reply = c->redisCommand("GET foo");
	testCond(reply->type == REDIS_REPLY_STRING &&
		memcmp(reply->str, "hello\x00world", 11) == 0)

	test("Binary reply length is correct: ");
	testCond(sdslen(reply->str) == 11)

	test("Can parse nil replies: ");
	reply = c->redisCommand("GET nokey");
	testCond(reply->type == REDIS_REPLY_NIL)

	/* test 7 */
	test("Can parse integer replies: ");
	reply = c->redisCommand("INCR mycounter");
	testCond(reply->type == REDIS_REPLY_INTEGER && reply->integer == 1)
	test("Can parse multi bulk replies: ");

	c->redisCommand("LPUSH mylist1 foo");
	c->redisCommand("LPUSH mylist1 bar");

	reply = c->redisCommand("LRANGE mylist1 0 -1");
	testCond(reply->type == REDIS_REPLY_ARRAY &&
		reply->element.size() == 2 &&
		!memcmp(reply->element[0]->str, "bar", 3) &&
		!memcmp(reply->element[1]->str, "foo", 3))

		/* m/e with multi bulk reply *before* other reply.
		* specifically test ordering of reply items to parse. */
		test("Can handle nested multi bulk replies: ");
	c->redisCommand("MULTI");
	c->redisCommand("LRANGE mylist1 0 -1");
	c->redisCommand("PING");

	reply = (c->redisCommand("EXEC"));
	testCond(reply->type == REDIS_REPLY_ARRAY &&
		reply->element.size() == 2 &&
		reply->element[0]->type == REDIS_REPLY_ARRAY &&
		reply->element[0]->element.size() == 2 &&
		!memcmp(reply->element[0]->element[0]->str, "bar", 3) &&
		!memcmp(reply->element[0]->element[1]->str, "foo", 3) &&
		reply->element[1]->type == REDIS_REPLY_STATUS &&
		strcmp(reply->element[1]->str, "PONG") == 0);
}

void testBlockingConecntionerros()
{
	RedisContextPtr c;
	test("Returns error when host cannot be resolved: ");
	c = redisConnect((char*)"text", port);
	testCond(c->err == REDIS_ERR_OTHER &&
		(strcmp(c->errstr, "Name or service not known") == 0 ||
			strcmp(c->errstr, "nodename nor servname provided, or not known") == 0 ||
			strcmp(c->errstr, "No address associated with hostname") == 0 ||
			strcmp(c->errstr, "Temporary failure in name resolution") == 0 ||
			strcmp(c->errstr, "hostname nor servname provided, or not known") == 0 ||
			strcmp(c->errstr, "no address associated with name") == 0) ||
		strcmp(c->errstr, "Permission denied") == 0);

	c.reset();
	test("Returns error when the port is not open: ");
	c = redisConnect("127.0.0.1", 1);
	testCond(c->err == REDIS_ERR_IO && strcmp(c->errstr, "Connection refused") == 0);
}

void testBlockIoerrors()
{
	RedisContextPtr c = redisConnect(ip, port);
	int major, minor;

	{
		const char *field = "redis_version:";
		char *p, *eptr;

		RedisReplyPtr reply = c->redisCommand("INFO");
		assert(reply != nullptr);

		p = strstr(reply->str, field);
		major = strtol(p + strlen(field), &eptr, 10);
		p = eptr + 1; /* char next to the first "." */
		minor = strtol(p, &eptr, 10);
	}

	test("Returns I/O error when the connection is lost: ");
	RedisReplyPtr reply = c->redisCommand("QUIT");
	assert(reply != nullptr);

	if (major > 2 || (major == 2 && minor > 0))
	{
		/* > 2.0 returns OK on QUIT and read() should be issued once more
		* to know the descriptor is at EOF. */
		testCond(strcmp(reply->str, "OK") == 0
			&& c->redisGetReply(reply) == REDIS_ERR);
	}
	else
	{
		testCond(reply == nullptr);
	}

	assert(c->err == REDIS_ERR_IO || 
		c->err == REDIS_ERR_EOF || strcmp(c->errstr, "Server closed the connection") == 0);

	c.reset();
	c = redisConnect(ip, port);

	test("Returns I/O error on socket timeout: ");
	struct timeval tv = { 1, 1000 };
	assert(Socket::setTimeOut(c->fd, tv));
	testCond(c->redisGetReply(reply) == REDIS_ERR &&
		c->err == REDIS_ERR_IO || errno == EAGAIN);
}

void testAppendFormateedCommands()
{
	RedisContextPtr c;
	RedisReplyPtr reply;
	char *cmd;
	int len;
	c = redisConnect(ip, port);
	test("Append format command: ");
	len = redisFormatCommand(&cmd, "SET foo bar");
	c->redisAppendFormattedCommand(cmd, len);
	testCond(c->redisGetReply(reply) == REDIS_OK);
	zfree(cmd);
}

void testThroughPut()
{
	RedisContextPtr c = redisConnect(ip, port);
	int i, num;
	int64_t t1, t2;

	test("Throughput:\n");
	num = 1000;

	t1 = ustime();
	for (i = 0; i < num; i++)
	{
		RedisReplyPtr reply = c->redisCommand("PING");
		assert(reply != nullptr && reply->type == REDIS_REPLY_STATUS);
	}

	t2 = ustime();
	printf("\t(%dx PING: %.3fs)\n", num, (t2 - t1) / 1000000.0);

	t1 = ustime();
	for (i = 0; i < 500; i++)
	{
		c->redisCommand("LPUSH mylist foo");
	}

	for (i = 0; i < 500; i++)
	{
		RedisReplyPtr reply = c->redisCommand("LRANGE mylist 0 499");
		assert(reply != nullptr && reply->type == REDIS_REPLY_ARRAY);
		assert(reply != nullptr && reply->element.size() == 500);
	}

	t2 = ustime();
	printf("\t(%dx LRANGE with 500 element.size(): %.3fs)\n", num, (t2 - t1) / 1000000.0);

	t1 = ustime();
	for (i = 0; i < num; i++)
	{
		c->redisAppendCommand("PING");
	}

	for (i = 0; i < num; i++)
	{
		RedisReplyPtr reply;
		assert(c->redisGetReply(reply) == REDIS_OK);
		assert(reply != nullptr && reply->type == REDIS_REPLY_STATUS);
	}

	t2 = ustime();
	printf("\t(%dx PING (pipelined): %.3fs)\n", num, (t2 - t1) / 1000000.0);

	t1 = ustime();
	for (i = 0; i < num; i++)
	{
		c->redisAppendCommand("LRANGE mylist 0 499");
	}

	for (i = 0; i < num; i++)
	{
		RedisReplyPtr reply;
		assert(c->redisGetReply(reply) == REDIS_OK);
		assert(reply != nullptr && reply->type == REDIS_REPLY_ARRAY);
		assert(reply != nullptr && reply->element.size() == 500);
	}

	t2 = ustime();
	printf("\t(%dx LRANGE with 500 element.size() (pipelined): %.3fs)\n", num, (t2 - t1) / 1000000.0);
}

int main(int argc, char* argv[])
{
	#ifdef _WIN64
		WSADATA wsaData;
		int32_t iRet = WSAStartup(MAKEWORD(2, 2), &wsaData);
		assert(iRet == 0);
	#else
		signal(SIGPIPE, SIG_IGN);
		signal(SIGHUP, SIG_IGN);
	#endif

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds

	RedisContextPtr c;
	RedisReplyPtr reply;

	ip = "127.0.0.1";
	port = 6379;

	c = redisConnectWithTimeout(ip, port, timeout);
	if (c == nullptr || c->err)
	{
		if (c)
		{
			printf("Connection error: %s\n", c->errstr.c_str());
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
	testBlockingConnection();
	testFormatCommand();
	testReplyReader();
	testBlockingConnectionTimeOuts();
	system("pause");
	return 0;
}
