#pragma once
#include <google/protobuf/message.h>
#include "all.h"
#include "tcpserver.h"
#include "clientlogic.h"
#include "hiredislogic.h"

class EventBase
{
public:
	EventBase(const char *ip, int16_t port, int16_t threadCount, int16_t sessionCount);
	~EventBase();
	
	void registerProtocol();
	void run();
	
	void clientConnCallback(const TcpConnectionPtr &conn);
	void clientReadCallback(const TcpConnectionPtr &conn, Buffer *buffer);
	void replyClient(int32_t id, const google::protobuf::Message &msg, const TcpConnectionPtr &conn);
	HiredisLogic *getHiredisLogic() { return &hiredisLogic; }
	ThreadPoolPtr getPool() { return server.getThreadPool(); }
	EventLoop *getLoop() { return &loop; }

private:
	EventLoop loop;
	TcpServer server;
	ClientLogic clientLogic;
	HiredisLogic hiredisLogic;
	typedef std::function<bool(int16_t cmd, const char *, size_t, const TcpConnectionPtr &)> ClientFunc;
	std::unordered_map<int32_t, ClientFunc> clientCommands;


};
