#include "eventloop.h"
#include "resolver.h"

EventLoop *g_loop;
int32_t count = 0;
int32_t total = 0;

void quit()
{
	g_loop->quit();
}

void resolveCallback(const std::string &host, const std::string &addr, uint16_t port)
{
	printf("resolveCallback %s -> %s  %d\n", host.c_str(), addr.c_str(), port);
	if (++count == total)
		quit();
}

void resolve(Resolver *res, const std::string &host)
{
	res->resolve(host, std::bind(&resolveCallback, host, std::placeholders::_1, std::placeholders::_2));
}

int32_t main(int32_t argc, char *argv[])
{
	EventLoop loop;
	loop.runAfter(10, false, quit);
	g_loop = &loop;
	Resolver resolver(&loop,
		argc == 1 ? Resolver::kDNSonly : Resolver::kDNSandHostsFile);
	if (argc == 1)
	{
		total = 3;
		resolve(&resolver, "www.baidu.com");
		resolve(&resolver, "www.example.com");
		resolve(&resolver, "www.google.com");
	}
	else
	{
		total = argc - 1;
		for (int32_t i = 1; i < argc; ++i)
			resolve(&resolver, argv[i]);
	}
	loop.run();
}
