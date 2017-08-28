#pragma once
#include "all.h"

#include "xTcpClient.h"
#include "xTcpconnection.h"
#include "xThreadPool.h"
#include "xCurrentThread.h"
#include "xLog.h"


class client;

class session:noncopyable
{
public:
	session(xEventLoop *loop,
		const char *ip,uint16_t port,client *owner)
		:cli(loop,nullptr),
		ip(ip),
		port(port),
		owner(owner),
		bytesRead(0),
		bytesWritten(0),
		messagesRead(0)
	{
		cli.setConnectionCallback(std::bind(&session::connCallBack, this, std::placeholders::_1, std::placeholders::_2));
		cli.setMessageCallback(std::bind(&session::readCallBack, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
		cli.setConnectionErrorCallBack(std::bind(&session::connErrorCallBack, this));
	}

	void start()
	{
		cli.connect(ip,port);
	}
	void stop()
	{
		cli.disconnect();
	}


	int64_t getBytesRead() {  return bytesRead; }
	int64_t getMessagesRead() { return messagesRead; }
	
private:

	void connCallBack(const xTcpconnectionPtr& conn, void *data);
	void connErrorCallBack()
	{	
		//LOG_WARN<<"tcp connect failure";
	}

	void readCallBack(const xTcpconnectionPtr& conn, xBuffer* buf, void *data)
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
	client * owner;
	int64_t bytesRead;
	int64_t bytesWritten;
	int64_t messagesRead;
  
};

class client:noncopyable
{
public:
	client(xEventLoop *loop,const char *ip,uint16_t port,int blockSize,int sessionCount,
		int timeOut,int threadCount)
		:loop(loop),
		threadPool(loop),
		sessionCount(sessionCount),
		timeOut(timeOut)
	{
		loop->runAfter(timeOut,nullptr,false, std::bind(&client::handlerTimeout, this,std::placeholders::_1));
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
			std::shared_ptr<session> vsession (new session(threadPool.getNextLoop(),ip,port,this));
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

	 
	void onDisconnect(const xTcpconnectionPtr& conn)
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
			conn->getLoop()->queueInLoop(std::bind(&client::quit, this));
		}
	}


	const std::string &getMessage()const
	{
		return message;
	}
private:
	void quit()
	{
		loop->queueInLoop(std::bind(&xEventLoop::quit,loop));
		
	}

	void handlerTimeout(void * data)
	{
		 LOG_WARN << "stop";
		 std::for_each(sessions.begin(),sessions.end(),std::mem_fn(&session::stop));
	}
	
	xEventLoop *loop;
	xThreadPool threadPool;
	int sessionCount;
	int timeOut;
	std::vector<std::shared_ptr<session>> sessions;
	std::string message;
	std::atomic<int> numConencted;
};


void session::connCallBack(const xTcpconnectionPtr & conn,void * data)
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
		fprintf(stderr, "Usage: client <host_ip> <port> <threads> <blocksize> ");
		fprintf(stderr, "<sessions> <time>\n");
	}
	else
	{			
		LOG_INFO << "ping pong client pid = " << getpid() << ", tid = " << xCurrentThread::tid();
		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int threadCount = atoi(argv[3]);
		int blockSize = atoi(argv[4]);
		int sessionCount = atoi(argv[5]);
		int timeout = atoi(argv[6]);

		xEventLoop loop;

		client cli(&loop, ip,port, blockSize, sessionCount, timeout, threadCount);
		loop.run();
	
	}
	return 0;
}

