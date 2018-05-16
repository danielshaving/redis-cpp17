#pragma once
#include "hiredis.h"
int clients = 100;
int requests = 10000;
int threadCount = 4;
int valueLen = 3;
std::mutex mtx;
std::condition_variable condition;
std::atomic<int> connShutDown;
std::atomic<int> connectCount;
class Client : noncopyable
{
public:
	enum Operation
	{
		kGet,
		kSet,
	};

	Client(EventLoop *loop,const char *ip,uint16_t port,Operation op)
	:client(loop,nullptr),
	 operation(op),
	 ack(0),
	 sent(0)
	{
		client.setConnectionCallback(std::bind(&Client::connCallBack,this,std::placeholders::_1));
		client.setMessageCallback(std::bind(&Client::readCallBack,this,std::placeholders::_1,std::placeholders::_2));
		client.connect(ip,port);
	}


	void countDown()
	{
		connShutDown++;
		std::unique_lock <std::mutex> lck(mtx);
		condition.notify_one();
	}

	void connCallBack(const TcpConnectionPtr &conn)
	{
		if(conn->connected())
		{
			connectCount++;
			this->conn = conn;
			std::unique_lock <std::mutex> lck(mtx);
			condition.notify_one();
		}
		else
		{
			this->conn.reset();
			client.getLoop()->queueInLoop(std::bind(&Client::countDown,this));
		}
	}

	void readCallBack(const TcpConnectionPtr &conn,Buffer *buffer)
	{
		if(operation == kSet)
		{
			while(buffer->readableBytes() > 0)
			{
				const char *end = static_cast<const char*>(memmem(buffer->peek(),
                        buffer->readableBytes(),
                        "+OK\r\n", 5));

				if(end)
				{
					buffer->retrieveUntil(end+5);
					++ack;
					if (sent < requests)
					{
						send();
					}

				}
				else
				{
					break;
				}
			}
		}
		else
		{
			while(buffer->readableBytes() > 0)
			{
				const char *end = static_cast<const char*>(memmem(buffer->peek(),
						buffer->readableBytes(),
						"$3\r\nbar\r\n", 9));

				if(end)
				{
					buffer->retrieveUntil(end+9);
					++ack;
					if (sent < requests)
					{
						send();
					}

				}
				else
				{
					break;
				}
			}
		}

		if(ack == requests)
		{
			conn->shutdown();
		}
	}


	void send()
	{
		char *cmd;
		int argc;
		const char *argv[3];
		if(operation == kSet)
		{
			argv[0] = "SET";
			argv[1] = "foo\0xxx";
			argv[2] = "bar";
			argc = 3;
		}
		else
		{
			argv[0] = "GET";
			argv[1] = "foo\0xxx";
			argc = 2;
		}

		sent++;
		int len = redisFormatCommandArgv(&cmd,argc,argv,nullptr);
		conn->send(cmd,len);
		zfree(cmd);

	}

	TcpClient client;
	TcpConnectionPtr conn;
	Operation operation;

	int ack;
	int sent;
};

std::vector<std::shared_ptr<Client>> clientPtr;

int main(int argc, char* argv[])
{
	if (argc < 3)
	{
		fprintf(stderr, "Usage: server <address> <port> <set,get>\n");
	}
	else
	{
		LOG_INFO<<"Connecting";
		connectCount = 0;
		connShutDown = 0;
		const char* ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		std::string op  = argv[3];
		EventLoop loop;
		ThreadPool pool(&loop);
		pool.setThreadNum(threadCount);
		pool.start();

		Client::Operation opertion;
		if(op == "set")
		{
			opertion = Client::kSet;
		}
		else
		{
			opertion = Client::kGet;
		}


		for(int i = 0; i< clients; i++)
		{
			std::shared_ptr<Client> client(new Client(pool.getNextLoop(),ip,port,opertion));
			clientPtr.push_back(client);
		}

		{
			std::unique_lock <std::mutex> lck(mtx);
			while(connectCount < clients)
			{
				condition.wait(lck);
			}
		}

		LOG_INFO<<"Client all connected";

		TimeStamp start = TimeStamp::now();
		for(auto it = clientPtr.begin(); it != clientPtr.end(); ++it)
		{
			(*it)->send();
		}

		{
			std::unique_lock <std::mutex> lck(mtx);
			while(connShutDown  < clients)
			{
				condition.wait(lck);
			}
		}

		TimeStamp end = TimeStamp::now();
		LOG_WARN<<"All finished";
		double seconds = timeDifference(end,start);
		LOG_WARN << seconds << " sec";
		LOG_WARN << 1.0 * clients * requests / seconds << " QPS";
	}

	return 0;
}



