#include "xHiredisClient.h"

xHiredisClient::xHiredisClient(xEventLoop * loop,const std::string &ip, uint16_t port)
:loop(loop),
ip(ip),
port(port),
context(nullptr)
{
	
}

xHiredisClient::~xHiredisClient()
{
	LOG_INFO<< this;
	assert(!channel || channel->isNoneEvent());
	::redisAsyncFree(context);
}

bool xHiredisClient::connected() const
{
	return channel && context && (context->c.flags & REDIS_CONNECTED);
}

const char * xHiredisClient::errstr() const
{
	assert(context != nullptr);
	return context->errstr;
}

void xHiredisClient::connect()
{
	assert(!context);
	context = ::redisAsyncConnect(ip.c_str(),port);
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


void xHiredisClient::disconnect()
{
	if(connected())
	{
		LOG_INFO<<this;
		::redisAsyncDisconnect(context);
	}
}


int xHiredisClient::getFd() const
{
	assert(context);
	return context->c.fd;
}


void xHiredisClient::setChannel()
{
	assert(!channel);
	channel.reset(new xChannel(loop,getFd()));
	channel->setReadCallback(std::bind(&xHiredisClient::handleRead,this));
	channel->setWriteCallback(std::bind(&xHiredisClient::handleWrite,this));
}


void xHiredisClient::removeChannel()
{
	channel->disableAll();
	channel->remove();
	channel.reset();
}


void xHiredisClient::handleRead()
{
	::redisAsyncHandleRead(context);

}

void xHiredisClient::handleWrite()
{
	::redisAsyncHandleWrite(context);
}


void xHiredisClient::connectCallback(const redisAsyncContext * ac, int status)
{
	getHiredis(ac)->connectCallback(status);
}

void xHiredisClient::disconnectCallback(const redisAsyncContext * ac, int status)
{
	 getHiredis(ac)->disconnectCallback(status);
}

void xHiredisClient::commandCallback(redisAsyncContext * ac, void * r , void * privdata)
{
	redisReply* reply = static_cast<redisReply*>(r);
	CommandCallback* cb = static_cast<CommandCallback*>(privdata);
	getHiredis(ac)->commandCallback(reply, cb);
}



void xHiredisClient::connectCallback(int status)
{
	if(status != REDIS_OK)
	{
		LOG_ERROR<<context->errstr<<"failed to connect to " << ip <<":" << port;
	}
	else
	{
		
	}

	if(connectCb)
	{
		connectCb(this,status);
	}
}


void xHiredisClient::disconnectCallback(int status)
{
	removeChannel();
	if(disconnectCb)
	{
		disconnectCb(this,status);
	}
}

void xHiredisClient::commandCallback(redisReply * reply,CommandCallback * cb)
{
	(*cb)(this, reply);
	delete cb;
}



xHiredisClient * xHiredisClient::getHiredis(const redisAsyncContext * ac)
{
	xHiredisClient * hiredis = static_cast<xHiredisClient*>(ac->ev.data);
	assert(hiredis->context == ac);
	return hiredis;
}


void xHiredisClient::addRead(void* privdata)
{
	xHiredisClient* hiredis = static_cast<xHiredisClient*>(privdata);
	hiredis->channel->enableReading();
}

void xHiredisClient::delRead(void* privdata)
{
	xHiredisClient* hiredis = static_cast<xHiredisClient*>(privdata);
	hiredis->channel->disableReading();
}

void xHiredisClient::addWrite(void* privdata)
{
	xHiredisClient* hiredis = static_cast<xHiredisClient*>(privdata);
	hiredis->channel->enableWriting();
}

void xHiredisClient::delWrite(void* privdata)
{
	xHiredisClient* hiredis = static_cast<xHiredisClient*>(privdata);
	hiredis->channel->disableWriting();
}

void xHiredisClient::cleanup(void* privdata)
{
	xHiredisClient* hiredis = static_cast<xHiredisClient*>(privdata);
	LOG_INFO << hiredis;
}

int xHiredisClient::command(const CommandCallback& cb, stringArg  cmd, ...)
{
	CommandCallback * p = new CommandCallback(cb);
	va_list args;
	va_start(args,cmd);
	int ret = ::redisvAsyncCommand(context, commandCallback, p, cmd.c_str(), args);
	va_end(args);
	
	return ret;
}


void xHiredisClient::pingCallback(xHiredisClient * msg, redisReply * reply)
{
	assert(this == msg);
	LOG_INFO<<reply->str;
}

int xHiredisClient::ping()
{
	return command(std::bind(&xHiredisClient::pingCallback,this,std::placeholders::_1,std::placeholders::_2),"ping");
}
	


void connectCallback(xHiredisClient * c, int status)
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

void disconnectCallback(xHiredisClient * c, int status)
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
	xEventLoop loop;
	xHiredisClient hiredis(&loop,"127.0.0.1",6379);
	hiredis.setConnectCallback(connectCallback);
	hiredis.setDisconnectCallback(disconnectCallback);
	hiredis.connect();
	loop.runAfter(1.0,nullptr,true,std::bind(&xHiredisClient::ping,&hiredis));
	loop.run();
	return 0;
}




