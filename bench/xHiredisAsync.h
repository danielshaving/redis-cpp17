//hiredis client test async
#include "all.h"
#include "xLog.h"
#include "xHiredis.h"

class xClient;

class xHiredis:noncopyable
{
public:
	xHiredis(xEventLoop *loop,xClient * owner);
	xHiredis(xClient * owner);
	~xHiredis(){}
	void start();

	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data);
	void connCallBack(const xTcpconnectionPtr& conn,void *data);
	void connErrorCallBack();

private:
	xEventLoop *loop;
	std::map<int32_t,xRedisAsyncContextPtr> redisAsyncs;
	xClient * owner;
	xTcpClient client;
	mutable std::mutex mutex;
};


class xClient:noncopyable
{
public:
	xClient(xEventLoop *loop,const char *ip,uint16_t port,int blockSize,int sessionCount,int threadCount)
	:loop(loop),
	ip(ip),
	port(port),
	sessionCount(sessionCount),
	threadPool(loop)
{

	if (threadCount > 1)
	{
	  	threadPool.setThreadNum(threadCount);
	}

	threadPool.start();
	for (int i = 0; i < blockSize; ++i)
	{
		message.push_back(static_cast<char>(i % 128));
	}

	for(int i = 0; i < sessionCount ; i ++)
	{
		std::shared_ptr<xHiredis>   redis = std::shared_ptr<xHiredis>(new xHiredis(threadPool.getNextLoop(),this));
		redis->start();
		redisVectors.push_back(redis);
	}
}


~xClient()
{
	redisVectors.clear();
}

public:
	xEventLoop *loop;
	const char *ip;
	uint16_t port;
	int sessionCount;
	xThreadPool threadPool;
	std::string message;
	std::vector<std::shared_ptr<xHiredis>>   redisVectors;
};


