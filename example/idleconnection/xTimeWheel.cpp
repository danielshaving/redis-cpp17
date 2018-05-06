#include "xTimeWheel.h"

xTimeWheel::xTimeWheel(xEventLoop *loop,const char *ip,int16_t port,int8_t idleSeconds)
:loop(loop),
server(loop,ip,port,nullptr),
idleSeconds(idleSeconds)
{
	server.setConnectionCallback(std::bind(&xTimeWheel::onConnection,this,std::placeholders::_1));
	server.setMessageCallback(std::bind(&xTimeWheel::onMessage,this,std::placeholders::_1,std::placeholders::_2));
	connectionBuckets.resize(idleSeconds);
	server.start();
	loop->runAfter(1.0,true,std::bind(&xTimeWheel::onTimer,this));
}

void xTimeWheel::onConnection(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		LOG_INFO<<"onConnection";
		EntryPtr entry(new Entry(conn));
		connectionBuckets.back().insert(entry);
		dumpConnectionBuckets();
		WeakEntryPtr weakEntry(entry);
		conn->setContext(weakEntry);
	}
	else
	{
		WeakEntryPtr weakEntry(std::any_cast<WeakEntryPtr>(conn->getContext()));
		LOG_DEBUG << "Entry use_count = " << weakEntry.use_count();
	}
}

void xTimeWheel::onMessage(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	std::string msg(buffer->retrieveAllAsString());
	conn->send(msg);
	WeakEntryPtr weakEntry(std::any_cast<WeakEntryPtr>(conn->getContext()));
	EntryPtr entry(weakEntry.lock());
	if (entry)
	{
		connectionBuckets.back().insert(entry);
		dumpConnectionBuckets();
	}
}

void xTimeWheel::onTimer()
{
	connectionBuckets.push_back(Bucket());
	dumpConnectionBuckets();
}

void xTimeWheel::dumpConnectionBuckets() const
{
	LOG_INFO << "size = " << connectionBuckets.size();
	int idx = 0;
	for (auto  bucketI = connectionBuckets.begin();
	  bucketI != connectionBuckets.end();
	  ++bucketI, ++idx)
	{
		const Bucket &bucket = *bucketI;
		printf("[%d] len = %zd : ", idx, bucket.size());
		for (auto  it = bucket.begin(); it != bucket.end(); ++it)
		{
			bool connectionDead = (*it)->weakConn.expired();
			printf("%p(%ld)%s, ",(*it).get(), it->use_count(),connectionDead ? " DEAD" : "");
		}
		puts("");
	}
}

int main(int argc,char *argv[])
{
	if(argc == 4)
	{
		const char *ip = argv[1];
		int16_t port = static_cast<uint16_t>(atoi(argv[2]));
		int8_t idleSeconds = static_cast<int8_t>(atoi(argv[3]));
		xEventLoop loop;
		xTimeWheel(&loop,ip,port,idleSeconds);
		loop.run();
	}
	else if (argc == 1)
	{
		xEventLoop loop;
		xTimeWheel wheel(&loop,"0.0.0.0",6379,5);
		loop.run();
	}
	else
	{
		fprintf(stderr,"Usage: server <host_ip> <port> \n");
	}

	return 0;
}
