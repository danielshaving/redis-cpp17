#include "xHiredisSync.h"

/* The following lines make up our testing "framework" :) */
static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}


xHiredis::xHiredis(xEventLoop * loop,xClient * owner)
:client(loop,this),
owner(owner)
{
	client.setConnectionCallback(std::bind(&xHiredis::connSyncCallBack, this, std::placeholders::_1,std::placeholders::_2));
	client.setConnectionErrorCallBack(std::bind(&xHiredis::connErrorCallBack, this));
}


xHiredis::~xHiredis()
{
	redisSyncs.clear();
}
void xHiredis::start()
{
	client.connect(owner->ip,owner->port);
}


void xHiredis::connErrorCallBack()
{
	LOG_WARN<<"connect server failure";
}





void xHiredis::testCommand(xRedisContextPtr c)
{
	redisReply *reply;

	reply = ( redisReply *)redisCommand(c,"PING");
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

void xHiredis::testFormatCommand()
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


void xHiredis::testReplyReader()
{
	std::shared_ptr<xRedisReader>  reader (new xRedisReader);
	
}


void xHiredis::connSyncCallBack(const xTcpconnectionPtr& conn,void *data)
{
	if(conn->connected())
	{
		xBuffer buffer;
		xRedisContextPtr c (new (xRedisContext));

		c->fd = conn->getSockfd();
		c->flags	|= REDIS_BLOCK;
		c->reader->buf = & buffer;
		redisSyncs.insert(std::make_pair(conn->getSockfd(),c));
		testFormatCommand();
		testReplyReader();
		testCommand(c);
	}
	else
	{
		redisSyncs.erase(conn->getSockfd());
	}
}


int main(int argc, char* argv[])
{
	if (argc != 3)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> \n ");
	}
	else
	{
		xEventLoop loop;
		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		xClient client(&loop,ip,port);
		loop.run();
	}
	return 0;
}



