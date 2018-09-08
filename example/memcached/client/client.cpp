#include "all.h"
#include "log.h"
#include "tcpconnection.h"
#include "tcpclient.h"
#include "threadpool.h"

int clients = 100;
int requests = 10000;
int threadCount = 4;
int valueLen = 3;
int keys = 10000;
std::mutex mtx;
std::condition_variable condition;
std::atomic<int> connShutDown;
std::atomic<int> connectCount;

class Client : boost::noncopyable
{
public:
	enum Operation
	{
		kGet,
		kSet,
	};

	Client(EventLoop *loop,const char *ip,uint16_t port,Operation  op)
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

	void connCallBack(const TcpConnectionPtr& conn)
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
			client.getLoop()->queueInLoop(std::bind(&Client::countDown, this));
		}
	}

	void readCallBack(const TcpConnectionPtr& conn,Buffer *buffer)
	{
		if(operation == kSet)
		{
			while(buffer->readableBytes() > 0)
			{
				const char* crlf = buffer->findCRLF();
				if (crlf)
				{
					buffer->retrieveUntil(crlf+2);
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
												  "END\r\n", 5));
				if (end)
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

		if(ack == requests)
		{
			conn->shutdown();
		}
	}

	void send()
	{
		Buffer buf;
		char req[256];
		if (operation == kSet)
		{
			snprintf(req, sizeof req,"set foo%d 0 0 %d\r\n",sent % keys,valueLen);
			++sent;
			buf.append(req);
			buf.append("foo\r\n");
		}
		else
		{
			snprintf(req, sizeof req,"get foo%d\r\n",sent % keys);
			++sent;
			buf.append(req);
		}
		conn->send(&buf);

	}

	TcpClient client;
	TcpConnectionPtr conn;
	Operation operation;
	int ack;
	int sent;
};

std::vector<std::shared_ptr<Client>> ClientPtr;
int main(int argc,char* argv[])
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
		const char *ip = argv[1];
		uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
		std::string op = argv[3];
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
			ClientPtr.push_back(client);
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
		for(auto &it : clientPtr)
		{
			it.send();
		}

		{
			std::unique_lock <std::mutex> lck(mtx);
			while(connShutDown < clients)
			{
				condition.wait(lck);
			}
		}

		TimeStamp end = TimeStamp::now();
		LOG_WARN<<"All finished";
		double seconds = timeDifference(end, start);
		LOG_WARN << seconds << " sec";
		LOG_WARN << 1.0 * clients * requests / seconds << " QPS";
	}

	return 0;
}



