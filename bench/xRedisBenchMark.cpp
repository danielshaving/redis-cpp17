//#pragma once
//#include "xHiredis.h"
//
//class xClient : noncopyable
//{
//public:
//	xClient(xEventLoop *loop,int requests,int valueLen)
//	{
//
//	}
//
//};
//
//int main(int argc, char* argv[])
//{
//	if (argc < 2)
//	{
//		fprintf(stderr, "Usage: server <address> <port>\n");
//	}
//	else
//	{
//
//		LOG_INFO<<"Connecting";
//		int clients = 100;
//		int requests = 100000;
//		int threadCount = 4;
//		int valueLen =3;
//
//		xEventLoop loop;
//		xThreadPool pool(&loop);
//		pool.start();
//
//		for(int i = 0; i< clients; i++)
//		{
//			std::shared_ptr<xClient> client(new xClient());
//
//		}
//		LOG_INFO<<"Client all connected";
//
//	}
//
//	return 0;
//}
//
//
//
