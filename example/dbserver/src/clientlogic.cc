#include "clientlogic.h"
#include "eventbase.h"
#include "hiredislogic.h"
#include "login.pb.h"
#include "heartbeat.pb.h"

ClientLogic::ClientLogic(EventBase *base)
:base(base)
{

}

ClientLogic::~ClientLogic()
{

}

bool ClientLogic::onLogin(int16_t cmd, const char *data, size_t len, const TcpConnectionPtr &conn)
{
	message::login_toS req;
	if(!req.ParseFromArray(data, len))
	{
		assert(false);
	}

	auto hiredis = base->getHiredisLogic();
	hiredis->getCommand(req.userid(), cmd,
			std::bind(&HiredisLogic::loginCallback,
					hiredis, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3), conn);
	return true;
}

bool ClientLogic::onHeartbeat(int16_t cmd, const char *data, size_t len, const TcpConnectionPtr &conn)
{
	message::heartbeat_toS req;
	message::heartbeat_toC rsp;

	if(!req.ParseFromArray(data, len))
	{
		assert(false);
	}

	base->replyClient(cmd, rsp, conn);
	return true;
}
