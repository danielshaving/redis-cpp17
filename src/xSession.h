#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xSds.h"
#include "xUtil.h"
#include "xItem.h"

class xSentinel;
class xRedis;
class xMemcachedServer;

class xSession : noncopyable, public std::enable_shared_from_this<xSession>
{
public:
	xSession(xRedis *redis,const xTcpconnectionPtr &conn);
	xSession(xMemcachedServer *memcahed,const xTcpconnectionPtr &conn);
	~xSession();

	void resetVlaue();
	void clearObj();
	void reset();
	void readCallBack(const xTcpconnectionPtr &conn, xBuffer *buf);
	int32_t processMultibulkBuffer(xBuffer *recvBuf);
	int32_t processInlineBuffer(xBuffer *recvBuf);
	int32_t processCommand();
	bool checkCommand(rObj *robjs);

	void onMessage(const xTcpconnectionPtr  &conn,xBuffer *buf);
	void receiveValue(xBuffer *buf);
	void discardValue(xBuffer *buf);
	bool processRequest(xStringPiece request);
	void resetRequest();
	void reply(xStringPiece msg);
	struct Reader;
	bool doUpdate(const std::string &command,auto &beg,auto end);
	void doDelete(const std::string &command,auto &beg, auto end);

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
	xMemcachedServer *memcached;

	int32_t reqtype;
	int32_t multibulklen;
	int64_t bulklen;
	int32_t argc;
	xBuffer sendBuf;
	xBuffer sendSlaveBuf;
	xBuffer sendPubSub;
	xTcpconnectionPtr 	conn;
	std::deque<rObj*>  robjs;
	std::vector<xTcpconnectionPtr> pubSubTcpconn;
	rObj * command;
	State state;
	Protocol protocol;
	xItem::UpdatePolicy policy;
	xItemPtr needle;
	xItemPtr currItem;
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

