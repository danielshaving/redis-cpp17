#include "hiredisasync.h"

int64_t startTime = 0;
int64_t endTime = 0;

std::atomic<int32_t> sessionCount = 0;
std::atomic<int32_t> sconnetCount = 0;
std::atomic<int32_t> gconnetCount = 0;
std::atomic<int32_t> messageCount = 1000000;
int32_t message = 1000000;

HiredisAsync::HiredisAsync(EventLoop *loop,
	int8_t threadCount, const char *ip, int16_t port)
	:hiredis(loop, sessionCount),
	connectCount(0),
	loop(loop),
	cron(true)
{
	if (threadCount <= 0)
	{
		threadCount = 1;
	}

	hiredis.setConnectionCallback(std::bind(&HiredisAsync::connectionCallback,
		this, std::placeholders::_1));
	hiredis.setDisconnectionCallback(std::bind(&HiredisAsync::disConnectionCallback,
		this, std::placeholders::_1));

	hiredis.setThreadNum(threadCount);
	hiredis.start();

	std::unique_lock<std::mutex> lk(mutex);
	while (connectCount < sessionCount)
	{
		condition.wait(lk);
	}
}

HiredisAsync::~HiredisAsync()
{

}

void HiredisAsync::connectionCallback(const TcpConnectionPtr &conn)
{
	connectCount++;
	condition.notify_one();
}

void HiredisAsync::disConnectionCallback(const TcpConnectionPtr &conn)
{
	if (--connectCount == 0)
	{
		hiredis.clearTcpClient();
		endTime = ustime();
		double dff = endTime - startTime;
		double elapsed = dff / (1000 * 1000);

		printf("all client diconnect success\n");
		printf("all client command benchmark seconds %.5f\n", elapsed);
		loop->quit();
	}
}

void HiredisAsync::setCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(reply->len == 2);
	assert(strcmp(reply->str, "OK") == 0);

	std::thread::id threadId = std::any_cast<std::thread::id>(privdata);
	assert(threadId == std::this_thread::get_id());
	assert(sconnetCount <= messageCount);
	if (++sconnetCount == messageCount)
	{
		printf("all client setcallback success\n");
	}
}

void HiredisAsync::getCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply->type == REDIS_REPLY_STRING);
	std::thread::id threadId = std::any_cast<std::thread::id>(privdata);
	assert(threadId == std::this_thread::get_id());
	assert(gconnetCount <= messageCount);
	if (++gconnetCount == messageCount)
	{
		printf("all client getcallback success\n");
	}
}

void HiredisAsync::serverCron()
{
	if (cron && gconnetCount == messageCount && sconnetCount == messageCount)
	{
		hiredis.diconnectTcpClient();
		cron = false;
	}
}

int main(int argc, char *argv[])
{
	if (argc != 5)
	{
		fprintf(stderr, "Usage: client <host_ip> <port> <sessionCount> <threadCount>\n ");
	}
	else
	{
		sigset_t set;
		sigemptyset(&set);
		sigaddset(&set, SIGPIPE);
		sigprocmask(SIG_BLOCK, &set, nullptr);

		const char *ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		sessionCount = atoi(argv[3]);
		int8_t threadCount = atoi(argv[4]);

		startTime = ustime();
		EventLoop loop;
		HiredisAsync async(&loop, threadCount, ip, port);
		printf("all client connect success\n");

		for (int32_t count = 1; count <= message; count++)
		{
			auto redis = async.getHiredis()->getRedisAsyncContext();
			if (redis == nullptr)
			{
				printf("redis-server disconnct, client reconnect......\n");
				continue;
			}

			std::thread::id threadId = redis->redisConn->getLoop()->getThreadId();
			redis->redisAsyncCommand(std::bind(&HiredisAsync::setCallback,
				&async, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
				threadId, "set key%d %d", count, count);

			redis->redisAsyncCommand(std::bind(&HiredisAsync::getCallback,
				&async, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
				threadId, "get key%d", count);
		}

		loop.runAfter(1.0, true, std::bind(&HiredisAsync::serverCron, &async));
		loop.run();
	}
	return 0;
}



