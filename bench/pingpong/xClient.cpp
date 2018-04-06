#pragma once
#include "xAll.h"

#include "xTcpClient.h"
#include "xTcpconnection.h"
#include "xThreadPool.h"
#include "xLog.h"


class xClient;

class xConnect : noncopyable
{
public:
	xConnect(xEventLoop *loop,
		const char *ip,uint16_t port,xClient *owner)
		:cli(loop,ip,port,nullptr),
		ip(ip),
		port(port),
		owner(owner),
		bytesRead(0),
		bytesWritten(0),
		messagesRead(0)
	{
		cli.setConnectionCallback(std::bind(&xConnect::connCallBack,this,std::placeholders::_1));
		cli.setMessageCallback(std::bind(&xConnect::readCallBack,this,std::placeholders::_1,std::placeholders::_2));
	}

	void start()
	{
		cli.asyncConnect();
	}

	void stop()
	{
		cli.disConnect();
	}


	int64_t getBytesRead() {  return bytesRead; }
	int64_t getMessagesRead() { return messagesRead; }

private:
	void connCallBack(const TcpConnectionPtr &conn);
	void readCallBack(const TcpConnectionPtr &conn, xBuffer* buf)
	{
		++messagesRead;
		bytesRead +=  buf->readableBytes();
		bytesWritten +=  buf->readableBytes();
		conn->send(buf);
		buf->retrieveAll();
	}

	xTcpClient cli;
	const char *ip;
	uint16_t port;
	xClient * owner;
	int64_t bytesRead;
	int64_t bytesWritten;
	int64_t messagesRead;

};

class xClient : noncopyable
{
public:
	xClient(xEventLoop *loop,const char *ip,uint16_t port,int blockSize,int sessionCount,
		int timeOut,int threadCount)
		:loop(loop),
		threadPool(loop),
		sessionCount(sessionCount),
		timeOut(timeOut)
	{
		loop->runAfter(timeOut,nullptr,false, std::bind(&xClient::handlerTimeout,this,std::placeholders::_1));
		if(threadCount > 1)
		{
			threadPool.setThreadNum(threadCount);
		}
		threadPool.start();

		for(int i = 0; i < blockSize; i ++)
		{
			message.push_back(static_cast<char>(i % 128));
		}

		for(int i = 0 ; i < sessionCount; i ++)
		{
			std::shared_ptr<xConnect> vsession (new xConnect(threadPool.getNextLoop(),ip,port,this));
			vsession->start();
    			sessions.push_back(vsession);
			numConencted++;
		}
	}

	void onConnect()
	{
		if (numConencted  == sessionCount)
		{
			LOG_WARN << "all connected";
		}
	}


	void onDisconnect(const TcpConnectionPtr& conn)
	{
		numConencted--;
		if (numConencted  == 0)
		{
			LOG_WARN << "all disconnected";

			int64_t totalBytesRead = 0;
			int64_t totalMessagesRead = 0;
			for(auto it = sessions.begin(); it != sessions.end(); ++it)
			{
				totalBytesRead += (*it)->getBytesRead();
				totalMessagesRead += (*it)->getMessagesRead();
			}
			LOG_WARN << totalBytesRead << " total bytes read";
			LOG_WARN << totalMessagesRead << " total messages read";
			LOG_WARN << static_cast<double>(totalBytesRead) / static_cast<double>(totalMessagesRead)<< " average message size";
			LOG_WARN << static_cast<double>(totalBytesRead) / (timeOut * 1024 * 1024) << " MiB/s throughput";
			conn->getLoop()->queueInLoop(std::bind(&xClient::quit, this));
		}
	}

	const std::string &getMessage() const { return message;	}
	void quit() { loop->queueInLoop(std::bind(&xEventLoop::quit,loop)); }
	void handlerTimeout(const std::any &context)
	{
		 LOG_WARN << "stop";
		 std::for_each(sessions.begin(),sessions.end(),std::mem_fn(&xConnect::stop));
	}

private:
	xEventLoop *loop;
	xThreadPool threadPool;
	int sessionCount;
	int timeOut;
	std::vector<std::shared_ptr<xConnect>> sessions;
	std::string message;
	std::atomic<int> numConencted;
};

void xConnect::connCallBack(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		conn->send(owner->getMessage());
		owner->onConnect();
	}
	else
	{
		owner->onDisconnect(conn);
	}
}

int main(int argc,char * argv[])
{
	if (argc != 7)
	{
		fprintf(stderr, "Usage: xClient <host_ip> <port> <threads> <blocksize> ");
		fprintf(stderr, "<sessions> <time>\n");
	}
	else
	{
		LOG_INFO << "ping pong xClient pid = " << getpid() << ", tid = " <<getpid();
		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int threadCount = atoi(argv[3]);
		int blockSize = atoi(argv[4]);
		int sessionCount = atoi(argv[5]);
		int timeout = atoi(argv[6]);

		xEventLoop loop;

		xClient cli(&loop, ip,port, blockSize, sessionCount, timeout, threadCount);
		loop.run();

	}
	return 0;
}

