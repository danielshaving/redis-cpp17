#pragma once
#include "all.h"
#include "hiredis.h"
#include "tcpserver.h"
#include "blockqueue.h"

EventLoop *base;
EventLoop *base1;

RedisContextPtr c;
BlockingQueue<std::string> blockQueues;

void redisLoop()
{
	c = redisConnect("127.0.0.1", 6379);
	if (c == nullptr || c->err)
	{
		exit(1);
	}
	
	EventLoop loop;
	base1 = &loop;
	loop.run();
}

void redisLoop1()
{
	c = redisConnect("127.0.0.1", 6379);
	if (c == nullptr || c->err)
	{
		exit(1);
	}

	while (1)
	{
		std::string str = blockQueues.take();
	}
}

void redisPong(const RedisReplyPtr &reply)
{
	assert(reply != nullptr);
	assert(strcmp(reply->str, "PONG") == 0);
	assert(reply->type == REDIS_REPLY_STATUS);
}

void redisPing()
{
	RedisReplyPtr reply = c->redisCommand("PING");
	assert(reply != nullptr);
	assert(strcmp(reply->str, "PONG") == 0);
	assert(reply->type == REDIS_REPLY_STATUS);
	base1->runInLoop(std::bind(redisPong, reply));
}

void redisPing1()
{
	std::string str = blockQueues.take();
	RedisReplyPtr reply = c->redisCommand("PING");
	assert(reply != nullptr);
	assert(strcmp(reply->str, "PONG") == 0);
	assert(reply->type == REDIS_REPLY_STATUS);
	base1->runInLoop(std::bind(redisPong, reply));
}

void serverCron()
{
	base1->runInLoop(std::bind(redisPing));
}

void serverCron1()
{
	blockQueues.put("");
}

int main(int argc, char *argv[])
{
	std::thread thread(std::bind(redisLoop));
	thread.detach();

	EventLoop loop;
	base = &loop;

	loop.runAfter(1.0, true, std::bind(serverCron));
	loop.run();
	return 0;
}




