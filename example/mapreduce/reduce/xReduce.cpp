#pragma once
#include "all.h"
#include "xTcpconnection.h"
#include "xTcpServer.h"
#include "xEventLoop.h"
#include "xLog.h"

int senders = 0;
int  port = 0;
std::unordered_map<std::string, int64_t> reduceWorks;
xEventLoop loop;

void output()
{
	LOG_INFO << "Writing reduce";
	std::string fileName = "reduce";
	fileName += std::to_string(port);
	std::ofstream out(fileName);
	for (auto it = reduceWorks.begin(); it != reduceWorks.end(); ++it)
	{
	  out << it->first << '\t' << it->second << '\n';
	}
}


void onConnection(const xTcpconnectionPtr& conn,void * data)
{
	if (!conn->connected())
	{
	  if (--senders == 0)
	  {
		output();
		loop.quit();
	  }
	}
}

void onMessage(const xTcpconnectionPtr& conn, xBuffer* buf, void * data)
{
	const char* crlf = nullptr;
	while ( (crlf = buf->findCRLF()) != nullptr)
	{
	  const char* tab = std::find(buf->peek(), crlf, '\t');
	  if (tab != crlf)
	  {
		std::string word(buf->peek(), tab);
		int64_t cnt = atoll(tab);
		reduceWorks[word] += cnt;
	  }
	  else
	  {
		LOG_ERROR << "Wrong format, no tab found";
		conn->shutdown();
	  }
	  buf->retrieveUntil(crlf + 2);
	}
}


int main(int argc, char* argv[])
{
	if (argc < 4)
	{
		fprintf(stderr, "Usage: server <address> <port> <sendernum>\n");
	}
	else
	{
		const char* ip = argv[1];
		port = static_cast<uint16_t>(atoi(argv[2]));
		senders = atoi(argv[3]);

		xTcpServer server;
		server.init(&loop, ip,port,nullptr);
		server.setConnectionCallback(onConnection);
		server.setMessageCallback(onMessage);
		server.start();
		loop.run();
	}
}



