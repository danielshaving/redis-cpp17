#include "hiredislogic.h"
#include "eventbase.h"
#include "login.pb.h"

HiredisLogic::HiredisLogic(EventBase *base,
			EventLoop *loop,int16_t threadCount, int16_t sessionCount,
			const char *ip,int16_t port)
:base(base),
 hiredis(loop, sessionCount, ip, port),
 connectCount(0)
{
	unlockScript = sdsnew("if redis.call('get', KEYS[1]) == ARGV[1] \
				 then return redis.call('del', KEYS[1]) else return 0 end");

	hiredis.setConnectionCallback(std::bind(&HiredisLogic::connectionCallback,
		this, std::placeholders::_1));
	hiredis.setDisconnectionCallback(std::bind(&HiredisLogic::disConnectionCallback,
		this, std::placeholders::_1));
}

void HiredisLogic::start()
{
	hiredis.setPool(base->getPool());
	hiredis.start();
}

HiredisLogic::~HiredisLogic()
{
	sdsfree(unlockScript);
}

void HiredisLogic::connectionCallback(const TcpConnectionPtr &conn)
{
	connectCount++;
	//condition.notify_one();
}

void HiredisLogic::disConnectionCallback(const TcpConnectionPtr &conn)
{
	connectCount--;
}

void HiredisLogic::setCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(reply->type == REDIS_REPLY_STATUS);
	assert(strcmp(reply->str, "OK") == 0);
}

void HiredisLogic::loginCallback(const RedisAsyncContextPtr &c,
	const RedisReplyPtr &reply, const std::any &privdata)
{
	assert(reply != nullptr);
	assert(privdata.has_value());
	const TcpConnectionPtr &conn = std::any_cast<TcpConnectionPtr>(privdata);
	assert(conn != nullptr);
	conn->getLoop()->assertInLoopThread();
	assert(conn->getContext().has_value());
	int16_t cmd = std::any_cast<int16_t>(conn->getContext());

	message::login_toC rsp;
	if (reply->type == REDIS_REPLY_STRING)
	{
		rsp.set_game(reply->str);
		rsp.set_token(reply->str);
		rsp.set_code(message::EnumType::OK);
	}
	else if (reply->type == REDIS_REPLY_NIL)
	{
		rsp.set_code(message::EnumType::ERROR);
	}
	else
	{
		printf("%s\n", reply->str);
		assert(false);
	}
	//thread safe
	base->replyClient(cmd, rsp, conn);
}

void HiredisLogic::getCommand(const std::string &userID,
		int16_t cmd, const RedisCallbackFn &fn, const TcpConnectionPtr &conn)
{
	conn->setContext(cmd);
	auto redis = hiredis.getRedisAsyncContext(conn->getLoop()->getThreadId(), conn->getSockfd());
	assert(redis != nullptr);
	size_t size = userID.size();
	int32_t status = redis->redisAsyncCommand(fn,
			conn, "get %b", userID.c_str(), size);
	assert(status == REDIS_OK);
}

	
