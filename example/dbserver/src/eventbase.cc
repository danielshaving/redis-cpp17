#include "eventbase.h"
#include "socket.h"

EventBase::EventBase(const char *ip, int16_t port,
		int16_t threadCount, int16_t sessionCount)
:server(&loop, ip, port, nullptr),
clientLogic(this),
hiredisLogic(this, &loop, threadCount, sessionCount, ip, 6379)
{
	server.setThreadNum(threadCount);
	server.setMessageCallback(std::bind(&EventBase::clientReadCallback,
			this, std::placeholders::_1, std::placeholders::_2));
	server.setConnectionCallback(std::bind(&EventBase::clientConnCallback,
			this, std::placeholders::_1));

	server.start();
	hiredisLogic.start();
	registerProtocol();
}

EventBase::~EventBase()
{
	
}

void EventBase::registerProtocol()
{
#define CLIENT_REGISTER(msgId,func) \
	clientCommands[msgId] = std::bind(&ClientLogic::func, &clientLogic, \
		std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
	CLIENT_REGISTER(3, onLogin)
	CLIENT_REGISTER(1, onHeartbeat)
}

void EventBase::run()
{
	loop.run();
}

void EventBase::clientConnCallback(const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
	if (conn->connected())
	{
		char buf[64] = "";
		uint16_t port = 0;
		auto addr = Socket::getPeerAddr(conn->getSockfd());
		Socket::toIp(buf, sizeof(buf), (const struct sockaddr *)&addr);
		Socket::toPort(&port, (const struct sockaddr *)&addr);
		LOG_INFO << "Client connect success " << buf << " " << port << " "<< conn->getSockfd();
	}
	else
	{
		LOG_INFO << "Client diconnect "<< conn->getSockfd();
	}
}

void EventBase::clientReadCallback(const TcpConnectionPtr &conn, Buffer *buffer)
{
	conn->getLoop()->assertInLoopThread();
	while (buffer->readableBytes() >= sizeof(int16_t) * 3)
	{
		int16_t len = buffer->peekInt16();
		if (len > buffer->readableBytes() - sizeof(int16_t))
		{
			break;
		}

		buffer->readInt16();
		int16_t flag = buffer->readInt16();
		int16_t cmd = buffer->readInt16();
		len -= sizeof(int16_t) * 2;

		auto it = clientCommands.find(cmd);
		if(it != clientCommands.end())
		{
			if(!it->second(cmd, buffer->peek(), len, conn))
			{
				LOG_WARN << "handle protocol error:" << cmd;
			}
		}
		else
		{
			LOG_WARN << "protocol not found error:" << cmd;
		}
		buffer->retrieve(len);
	}
}

void EventBase::replyClient(int32_t cmd, const google::protobuf::Message &msg, const TcpConnectionPtr &conn)
{
	conn->getLoop()->assertInLoopThread();
	if(conn == nullptr)
	{
		return;
	}

	Buffer *buffer = conn->outputBuffer();
	buffer->appendInt16(msg.ByteSize() + sizeof(int16_t) * 2);
	buffer->appendInt16(0);
	buffer->appendInt16(cmd);
	buffer->ensureWritableBytes(msg.ByteSize());

	char *data = buffer->beginWrite();
	if(!msg.SerializeToArray(data, msg.ByteSize()))
	{
		assert(false);
	}

	buffer->hasWritten(msg.ByteSize());
	conn->sendPipe();
}
