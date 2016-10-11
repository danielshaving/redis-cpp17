#ifndef _SESSION_H_
#define _SESSION_H_
#include "all.h"
#include "xStringPiece.h"
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
	 void reset();
	 void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	 int processMultibulkBuffer(xBuffer *recvBuf);
	 int processCommand();

public:
	int32_t 			reqtype;
	int32_t 			multibulklen;
	int64_t 			bulklen;
	int32_t 		    argc;
	xTcpconnectionPtr 	conn;
	xBuffer 			sendBuf;
	std::vector<rObj*>  robjs;
	std::string commond;
	xRedis *redis;
};
#endif
