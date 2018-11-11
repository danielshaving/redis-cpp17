#pragma once
#include "all.h"
#include "tcpconnection.h"

class EventBase;
class ClientLogic
{
public:
	ClientLogic(EventBase *base);
	~ClientLogic();

	bool onLogin(int16_t cmd, const char *data, size_t len, const TcpConnectionPtr &conn);
	bool onHeartbeat(int16_t cmd, const char *data, size_t len, const TcpConnectionPtr &conn);
private:
	EventBase *base;
};
