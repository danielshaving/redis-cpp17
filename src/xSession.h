#pragma once
#include "xAll.h"
#include "xTcpConnection.h"
#include "xObject.h"
#include "xSds.h"
#include "xUtil.h"
#include "xItem.h"

class xSentinel;
class xRedis;
class xMemcached;

class xSession : noncopyable, public std::enable_shared_from_this<xSession>
{
public:
	xSession(xRedis *redis,const TcpConnectionPtr &conn);
	xSession(xMemcached *memcahed,const TcpConnectionPtr &conn);
	~xSession();

	void resetVlaue();
	void clearObj();
	void reset();
	void readCallBack(const TcpConnectionPtr &conn,xBuffer *buffer);
	int32_t processMultibulkBuffer(xBuffer *buffer);
	int32_t processInlineBuffer(xBuffer *buffer);
	int32_t processCommand();
	bool checkCommand(rObj *robjs);

	void onMessage(const TcpConnectionPtr &conn,xBuffer *buffer);
	void receiveValue(xBuffer *buffer);
	void discardValue(xBuffer *buffer);
	bool processRequest(xStringPiece request);
	void resetRequest();
	void reply(xStringPiece msg);
	struct Reader;
	bool doUpdate(const std::string &command,auto &beg,auto end);
	void doDelete(const std::string &command,auto &beg,auto end);

public:
	enum State
	{
		kNewCommand,
		kReceiveValue,
		kDiscardValue,
	};

	enum Protocol
	{
		kAscii,
		kBinary,
		kAuto,
	};

	xRedis *redis;
	xMemcached *memcached;

	rObj *command;
	std::deque<rObj*> commands;

	int32_t reqtype;
	int32_t multibulklen;
	int64_t bulklen;
	int32_t argc;
	xBuffer clientBuffer;
	xBuffer slaveBuffer;
	xBuffer pubsubBuffer;

	TcpConnectionPtr clientConn;

	State state;
	Protocol protocol;
	xItem::UpdatePolicy policy;
	ItemPtr needle;
	ItemPtr currItem;
	size_t bytesToDiscard;
	size_t bytesRead;
	size_t requestsProcessed;
	static std::string kLongestKey;

	bool authEnabled;
	bool replyBuffer;
	bool fromMaster;
	bool fromSlave;
	bool noreply;
};

