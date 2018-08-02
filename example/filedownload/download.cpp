#include "all.h"
#include "eventloop.h"
#include "tcpserver.h"
#include "tcpconnection.h"

const char *g_file = nullptr;

// FIXME: use FileUtil::readFile()
std::string readFile(const char *filename)
{
	std::string content;
	FILE* fp = ::fopen(filename,"rb");
	if (fp)
	{
		// inefficient!!!
		const int kBufSize = 1024*1024;
		char iobuf[kBufSize];
		::setbuffer(fp,iobuf,sizeof iobuf);

		char buf[kBufSize];
		size_t nread = 0;
		while ( (nread = ::fread(buf,1,sizeof buf,fp)) > 0)
		{
			content.append(buf,nread);
		}
		::fclose(fp);
	}
	return content;
}

void onHighWaterMark(const TcpConnectionPtr& conn,size_t len)
{
	LOG_INFO << "HighWaterMark " << len;
}

void onConnection(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		LOG_INFO << "FileServer - Sending file " << g_file;
		conn->setHighWaterMarkCallback(onHighWaterMark,64*1024);
		std::string fileContent = readFile(g_file);
		conn->send(fileContent);
		conn->shutdown();
		LOG_INFO << "FileServer - done";
	}
}

int main(int argc,char* argv[])
{
	LOG_INFO << "pid = " << getpid();
	if (argc > 1)
	{
		g_file = argv[1];
		EventLoop loop;
		TcpServer server(&loop,"0.0.0.0",6379,nullptr);
		server.setConnectionCallback(onConnection);
		server.start();
		loop.run();
	}
	else
	{
		fprintf(stderr, "Usage: %s file_for_downloading\n", argv[0]);
	}
}

