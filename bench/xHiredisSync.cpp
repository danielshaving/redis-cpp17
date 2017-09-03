#include "xHiredisSync.h"
/*
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

*/
