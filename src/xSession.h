#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xObject.h"
#include "xSds.h"

class xRedis;
class xSession: boost::noncopyable, public boost::enable_shared_from_this<xSession>
{
public:
	xSession(xRedis *redis,const xTcpconnectionPtr & conn);
	~xSession();
public:
	 void resetVlaue();
	 void clearObj();
	 void reset();
	 void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	 int processMultibulkBuffer(xBuffer *recvBuf);
	 int processInlineBuffer(xBuffer *recvBuf);
	 int processCommand();
	 bool checkCommond(rObj*  robjs,int size);

public:
	int32_t 			reqtype;
	int32_t 			multibulklen;
	int64_t 			bulklen;
	int32_t 		    argc;
	xTcpconnectionPtr 	conn;
	xBuffer 			sendBuf;
	xBuffer 			sendSlaveBuf;
	std::deque<rObj*>  robjs;
	std::string commond;
	xRedis *redis;
};

