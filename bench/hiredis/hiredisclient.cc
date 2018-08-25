#include "hiredisclient.h"

HiredisClient::HiredisClient(EventLoop *loop, const std::string &ip, uint16_t port)
	:loop(loop),
	ip(ip),
	port(port),
	context(nullptr)
{

}

HiredisClient::~HiredisClient()
{
	LOG_INFO << this;
	assert(!channel || channel->isNoneEvent());
	::redisAsyncFree(context);
}

bool HiredisClient::connected() const
{
	return channel && context && (context->c.flags & REDIS_CONNECTED);
}

const char *HiredisClient::errstr() const
{
	assert(context != nullptr);
	return context->errstr;
}

void HiredisClient::connect()
{
	assert(!context);
	context = ::redisAsyncConnect(ip.c_str(), port);
	context->ev.addRead = addRead;
	context->ev.delRead = delRead;
	context->ev.addWrite = addWrite;
	context->ev.delWrite = delWrite;
	context->ev.cleanup = cleanup;
	context->ev.data = this;
	setChannel();

	assert(context->onConnect == nullptr);
	assert(context->onDisconnect == nullptr);
	::redisAsyncSetConnectCallback(context, connectCallback);
	::redisAsyncSetDisconnectCallback(context, disconnectCallback);
}

void HiredisClient::disconnect()
{
	if (connected())
	{
		LOG_INFO << this;
		::redisAsyncDisconnect(context);
	}
}

int HiredisClient::getFd() const
{
	assert(context);
	return context->c.fd;
}

void HiredisClient::setChannel()
{
	assert(!channel);
	channel.reset(new xChannel(loop, getFd()));
	channel->setReadCallback(std::bind(&HiredisClient::handleRead, this));
	channel->setWriteCallback(std::bind(&HiredisClient::handleWrite, this));
}

void HiredisClient::removeChannel()
{
	channel->disableAll();
	channel->remove();
	channel.reset();
}

void HiredisClient::handleRead()
{
	::redisAsyncHandleRead(context);
}

void HiredisClient::handleWrite()
{
	::redisAsyncHandleWrite(context);
}

void HiredisClient::connectCallback(const redisAsyncContext *ac, int status)
{
	getHiredis(ac)->connectCallback(status);
}

void HiredisClient::disconnectCallback(const redisAsyncContext *ac, int status)
{
	getHiredis(ac)->disconnectCallback(status);
}

void HiredisClient::commandCallback(redisAsyncContext *ac, void *r, void *privdata)
{
	redisReply *reply = static_cast<redisReply*>(r);
	CommandCallback *cb = static_cast<CommandCallback*>(privdata);
	getHiredis(ac)->commandCallback(reply, cb);
}

void HiredisClient::connectCallback(int status)
{
	if (status != REDIS_OK)
	{
		LOG_ERROR << context->errstr << "failed to connect to " << ip << ":" << port;
	}
	else
	{

	}

	if (connectCb)
	{
		connectCb(this, status);
	}
}

void HiredisClient::disconnectCallback(int status)
{
	removeChannel();
	if (disconnectCb)
	{
		disconnectCb(this, status);
	}
}

void HiredisClient::commandCallback(redisReply *reply, CommandCallback *cb)
{
	(*cb)(this, reply);
	delete cb;
}

HiredisClient *HiredisClient::getHiredis(const redisAsyncContext *ac)
{
	HiredisClient *hiredis = static_cast<HiredisClient*>(ac->ev.data);
	assert(hiredis->context == ac);
	return hiredis;
}

void HiredisClient::addRead(void *privdata)
{
	HiredisClient *hiredis = static_cast<HiredisClient*>(privdata);
	hiredis->channel->enableReading();
}

void HiredisClient::delRead(void *privdata)
{
	HiredisClient *hiredis = static_cast<HiredisClient*>(privdata);
	hiredis->channel->disableReading();
}

void HiredisClient::addWrite(void *privdata)
{
	HiredisClient *hiredis = static_cast<HiredisClient*>(privdata);
	hiredis->channel->enableWriting();
}

void HiredisClient::delWrite(void *privdata)
{
	HiredisClient *hiredis = static_cast<HiredisClient*>(privdata);
	hiredis->channel->disableWriting();
}

void HiredisClient::cleanup(void *privdata)
{
	HiredisClient *hiredis = static_cast<HiredisClient*>(privdata);
	LOG_INFO << hiredis;
}

int HiredisClient::command(const CommandCallback& cb, StringArg cmd, ...)
{
	CommandCallback *p = new CommandCallback(cb);
	va_list args;
	va_start(args, cmd);
	int ret = ::redisvAsyncCommand(context, commandCallback, p, cmd.c_str(), args);
	va_end(args);
	return ret;
}

void HiredisClient::pingCallback(HiredisClient *msg, redisReply *reply)
{
	assert(this == msg);
	LOG_INFO << reply->str;
}

int HiredisClient::ping()
{
	return command(std::bind(&HiredisClient::pingCallback, this, std::placeholders::_1, std::placeholders::_2), "ping");
}

void connectCallback(HiredisClient *c, int status)
{
	if (status != REDIS_OK)
	{
		LOG_ERROR << "connectCallback Error:" << c->errstr();
	}
	else
	{
		LOG_INFO << "Connected...";
	}
}

void disconnectCallback(HiredisClient *c, int status)
{
	if (status != REDIS_OK)
	{
		LOG_ERROR << "disconnectCallback Error:" << c->errstr();
	}
	else
	{
		LOG_INFO << "Disconnected...";
	}
}

int main(int argc, char** argv)
{
	EventLoop loop;
	HiredisClient hiredis(&loop, "127.0.0.1", 6379);
	hiredis.setConnectCallback(connectCallback);
	hiredis.setDisconnectCallback(disconnectCallback);
	hiredis.connect();
	loop.runAfter(1.0, nullptr, true, std::bind(&HiredisClient::ping, &hiredis));
	loop.run();
	return 0;
}




