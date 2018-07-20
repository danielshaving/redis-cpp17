#include "resolver.h"

double getSeconds(struct timeval *tv)
{
	if (tv)
		return double(tv->tv_sec) + double(tv->tv_usec)/1000000.0;
	else
		return -1.0;
}

const char *getSocketType(int32_t type)
{
	if (type == SOCK_DGRAM)
		return "UDP";
	else if (type == SOCK_STREAM)
		return "TCP";
	else
		return "Unknown";
}

const bool kDebug = false;

Resolver::Resolver(EventLoop *loop,Option opt)
:loop(loop),
 ctx(nullptr),
 timerActive(false)
{
	static char lookups[] = "b";
	struct ares_options options;
	int32_t optmask = ARES_OPT_FLAGS;
	options.flags = ARES_FLAG_NOCHECKRESP;
	options.flags |= ARES_FLAG_STAYOPEN;
	options.flags |= ARES_FLAG_IGNTC; // UDP only
	optmask |= ARES_OPT_SOCK_STATE_CB;
	options.sock_state_cb = &Resolver::ares_sock_state_callback;
	options.sock_state_cb_data = this;
	optmask |= ARES_OPT_TIMEOUT;
	options.timeout = 2;
	if (opt == kDNSonly)
	{
		optmask |= ARES_OPT_LOOKUPS;
		options.lookups = lookups;
	}

	int32_t status = ares_init_options(&ctx_,&options,optmask);
	if (status != ARES_SUCCESS)
	{
		assert(0);
	}
	ares_set_socket_callback(ctx,&Resolver::ares_sock_create_callback,this);
}

Resolver::~Resolver()
{
	ares_destroy(ctx);
}

bool Resolver::resolve(StringArg hostname,const Callback& cb)
{
	loop->assertInLoopThread();
	QueryData *queryData = new QueryData(this,cb);
	ares_gethostbyname(ctx,hostname.c_str(),AF_INET,
	  &Resolver::ares_host_callback,queryData);
	struct timeval tv;
	struct timeval *tvp = ares_timeout(ctx,nullptr,&tv);
	double timeout = getSeconds(tvp);
	LOG_DEBUG << "timeout " <<  timeout << " active " << timerActive;
	if (!timerActive)
	{
		loop->runAfter(timeout,false,std::bind(&Resolver::onTimer,this));
		timerActive = true;
	}
	return queryData != nullptr;
}

void Resolver::onRead(int32_t sockfd)
{
	LOG_DEBUG << "onRead " << sockfd << " at ";
	ares_process_fd(ctx,sockfd,ARES_SOCKET_BAD);
}


void Resolver::onTimer()
{
	assert(timerActive == true);
	ares_process_fd(ctx,ARES_SOCKET_BAD,ARES_SOCKET_BAD);
	struct timeval tv;
	struct timeval *tvp = ares_timeout(ctx,nullptr,&tv);
	double timeout = getSeconds(tvp);

	if (timeout < 0)
	{
		timerActive = false;
	}
	else
	{
		loop->runAfter(timeout,false,std::bind(&Resolver::onTimer,this));
	}
}

void Resolver::onQueryResult(int32_t status,struct hostent *result,const Callback &callback)
{
	LOG_DEBUG << "onQueryResult " << status;
	struct sockaddr_in addr;
	bzero(&addr,sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_port = 0;
	if (result)
	{
		addr.sin_addr = *reinterpret_cast<in_addr*>(result->h_addr);
		if (kDebug)
		{
			printf("h_name %s\n", result->h_name);
			for (char **alias = result->h_aliases; *alias != nullptr; ++alias)
			{
				printf("alias: %s\n",*alias);
			}
			// printf("ttl %d\n", ttl);
			// printf("h_length %d\n", result->h_length);
			for (char **haddr = result->h_addr_list; *haddr != nullptr; ++haddr)
			{
				char buf[32];
				inet_ntop(AF_INET,*haddr,buf,sizeof buf);
				printf("  %s\n",buf);
			}
		}
	}

	char buf[64];
	socket.toIp(buf,sizeof buf,(const struct sockaddr*)&addr);
	uint16_t port;
	socket.toPort(&port,(const struct sockaddr*)&addr);
	callback(buf,port);
}

void Resolver::onSockCreate(int32_t sockfd,int32_t type)
{
	loop->assertInLoopThread();
	assert(channels.find(sockfd) == channels.end());
	ChannelPtr channel(new Channel(loop,sockfd));
	channel->setReadCallback(std::bind(&Resolver::onRead,this,sockfd));
	channel->enableReading();
	channels[sockfd] = channel;
}

void Resolver::onSockStateChange(int32_t sockfd,bool read,bool write)
{
	loop->assertInLoopThread();
	auto it = channels.find(sockfd);
	assert(it != channels.end());
	if (read)
	{
		// update
		// if (write) { } else { }
	}
	else
	{
		// remove
		it->second->disableAll();
		it->second->remove();
		channels.erase(it);
	}
}

void Resolver::ares_host_callback(void *data,int32_t status,int32_t timeouts,struct hostent *hostent)
{
	QueryData *query = static_cast<QueryData*>(data);
	query->owner->onQueryResult(status,hostent,query->callback);
	delete query;
}

int32_t Resolver::ares_sock_create_callback(int32_t sockfd,int32_t type,void *data)
{
	LOG_TRACE << "sockfd=" << sockfd << " type=" << getSocketType(type);
	static_cast<Resolver*>(data)->onSockCreate(sockfd,type);
	return 0;
}

void Resolver::ares_sock_state_callback(void *data,int32_t sockfd,int32_t read,int32_t write)
{
	LOG_TRACE << "sockfd=" << sockfd << " read=" << read << " write=" << write;
	static_cast<Resolver*>(data)->onSockStateChange(sockfd,read,write);
}




