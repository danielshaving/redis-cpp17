#include "all.h"
#include "tcpconnection.h"
#include "tcpserver.h"
#include "eventloop.h"
#include "log.h"
#include "md5.h"
namespace fs = std::experimental::filesystem;

class Server
{
public:
	Server(const char *ip, uint16_t port)
	:ip(ip),
	port(port),
	path("filedata/")
	{
		EventLoop loop;
		this->loop = &loop;
		TcpServer server(&loop, ip, port, nullptr);
		server.setConnectionCallback(std::bind(&Server::connectionCallback, this, std::placeholders::_1));
		server.setMessageCallback(std::bind(&Server::messageCallback, this, std::placeholders::_1, std::placeholders::_2));
		server.start();
	}
	
	void readAllFile(const TcpConnectionPtr &conn)
	{
		std::string fileName;
		for (auto &iter : fs::directory_iterator(path))
		{
			auto fe = iter.path();

			fileName.clear();
			fileName += path;
			fileName += fe.filename().string();

			if (!fs::exists(fileName))
			{
				exit(1);
			}

			std::ifstream fin(fileName.c_str());
			if (fin.eof())
			{
				continue;
			}

			int i = 0;
			std::string line;
			while(std::getline(fin,line, '\n'))
			{
				Buffer *buffer = conn->outputBuffer();
				buffer->append(MD5(line).toStr());
				buffer->append("\r\n", 2);
				conn->sendPipe();
			}
			fin.close();
		}
	}

	void writeCompleteCallback(const TcpConnectionPtr &conn)
	{
		conn->startRead();
		conn->setWriteCompleteCallback(WriteCompleteCallback());
	}

	void highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent)
	{
		LOG_INFO << " bytes " << bytesToSent;
		if (conn->outputBuffer()->readableBytes() > 0)
		{
			conn->stopRead();
			conn->setWriteCompleteCallback(
				std::bind(&Server::writeCompleteCallback, this, std::placeholders::_1));
		}
	}

	void connectionCallback(const TcpConnectionPtr &conn)
	{
		conn->setHighWaterMarkCallback(
			std::bind(&Server::highWaterCallback, this, std::placeholders::_1, std::placeholders::_2),
			1024 * 1024);
		readAllFile(conn);
	}
	
	void messageCallback(const TcpConnectionPtr &conn, Buffer *buffer)
	{
		buffer->retrieveAll();
	}
	
	void readFileData(const TcpConnectionPtr &conn)
	{
		conn->shutdown();
	}
	
	void run()
	{
		loop->run();
	}
private:
	Server(const Server&);
	void operator=(const Server&);
	
	EventLoop *loop;
	const char *ip;
	uint16_t port;
	const char *path;
};

int main(int argc, char* argv[])
{
	Server server("127.0.0.1", 8888);
	server.run();
	return 0;
}
