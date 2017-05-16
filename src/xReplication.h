#pragma once

#include "all.h"
#include "xObject.h"
#include "xTcpClient.h"

class xRedis;
class xReplication
{
public:
	xReplication():start(false){}
	~xReplication();
	void connectMaster(const char * str,long port);
	void replicationSetMaster(xRedis * redis,rObj* obj, long port);
	void connErrorCallBack();
	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	void syncWithMaster(const xTcpconnectionPtr& conn);
	void replicationCron();
	void syncWrite(const xTcpconnectionPtr& conn);
private:
	bool start;
	pid_t pid;
	std::shared_ptr<std::thread> threads;
	xEventLoop *loop;
	xTcpClient *client;
	mutable std::mutex mutex;
	std::condition_variable condition;
	xRedis *redis;
	int replLen;
	int replState;          /* Replication status if the instance is a slave */
	xBuffer sendBuf;
};


void replicationFeedSlaves(xBuffer &  sendBuf,std::string &commond,xRedis * redis ,std::vector<rObj*> &robjs,xTcpconnectionPtr & conn);

